/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package kafka.controllerV2

import java.util.Collections
import java.util.concurrent.LinkedBlockingQueue

import kafka.cluster.Broker
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, NetworkClientBlockingOps, ShutdownableThread}
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, LeaderAndIsrRequest, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.Time

/**
 * BrokerChannelManager manages controller-to-broker connections and requests.
 *
 * This class is not thread-safe!
 */
class BrokerChannelManager(kafkaConfig: KafkaConfig, time: Time, metrics: Metrics) {
  private val brokerChannels = scala.collection.mutable.Map.empty[Int, BrokerChannel]

  def reset(): Unit = {
    brokerChannels.keys.foreach(removeChannel)
  }

  def addChannel(broker: Broker): Unit = {
    if (!brokerChannels.contains(broker.id)) {
      brokerChannels.put(broker.id, new BrokerChannel(broker))
      brokerChannels(broker.id).start()
    }
  }

  def removeChannel(id: Int): Unit = {
    if (brokerChannels.contains(id)) {
      brokerChannels(id).stop()
      brokerChannels.remove(id)
    }
  }

  def enqueueLeaderAndIsrRequest(id: Int, requestBuilder: LeaderAndIsrRequest.Builder, callbackOpt: Option[(Int, AbstractResponse) => Unit]) = {
    val queuedRequest = QueuedRequest(ApiKeys.LEADER_AND_ISR, requestBuilder, callbackOpt)
    brokerChannels(id).queue.put(queuedRequest)
  }

  def enqueueUpdateMetadataRequest(id: Int, requestBuilder: UpdateMetadataRequest.Builder, callbackOpt: Option[(Int, AbstractResponse) => Unit]) = {
    val queuedRequest = QueuedRequest(ApiKeys.UPDATE_METADATA_KEY, requestBuilder, callbackOpt)
    brokerChannels(id).queue.put(queuedRequest)
  }

  def enqueueStopReplicaRequest(id: Int, requestBuilder: StopReplicaRequest.Builder, callbackOpt: Option[(Int, AbstractResponse) => Unit]) = {
    val queuedRequest = QueuedRequest(ApiKeys.STOP_REPLICA, requestBuilder, callbackOpt)
    brokerChannels(id).queue.put(queuedRequest)
  }

  class BrokerChannel(broker: Broker) {
    private val brokerChannelThread = new BrokerChannelThread(s"broker-channel-thread-to-broker-${broker.id}")
    private val brokerNode = broker.getNode(ListenerName.forSecurityProtocol(kafkaConfig.interBrokerSecurityProtocol))
    val queue = new LinkedBlockingQueue[QueuedRequest]
    private val networkClient: NetworkClient = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        kafkaConfig.interBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        kafkaConfig,
        kafkaConfig.interBrokerListenerName,
        kafkaConfig.saslMechanismInterBrokerProtocol,
        kafkaConfig.saslInterBrokerHandshakeRequestEnable)
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "broker-channel",
        Collections.singletonMap[String, String]("broker-id", broker.id.toString),
        false,
        channelBuilder
      )
      new NetworkClient(
        selector,
        new ManualMetadataUpdater(Collections.singletonList[Node](brokerNode)),
        kafkaConfig.brokerId.toString,
        1,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        kafkaConfig.requestTimeoutMs,
        time,
        false
      )
    }

    def start(): Unit = {
      brokerChannelThread.start()
    }

    def stop(): Unit = {
      networkClient.close()
      queue.clear()
      brokerChannelThread.shutdown()
    }

    class BrokerChannelThread(name: String) extends ShutdownableThread(name) {
      override def doWork(): Unit = {
        val queuedRequest = queue.take()
        try {
          val clientResponse = blockingSendAndReceive(queuedRequest)
          handleClientResponse(queuedRequest, clientResponse)
        } catch {
          case e: Throwable =>
            networkClient.close(broker.id.toString)
        }
      }

      private def blockingSendAndReceive(queuedRequest: QueuedRequest): ClientResponse = {
        import NetworkClientBlockingOps._
        while (isRunning.get()) {
          try {
            if (isBrokerReady) {
              val clientRequest = networkClient.newClientRequest(brokerNode.idString, queuedRequest.requestBuilder, time.milliseconds(), true)
              return networkClient.blockingSendAndReceive(clientRequest)(time)
            }
            else backoff()
          } catch {
            case e: Throwable =>
              networkClient.close(broker.id.toString)
              backoff()
          }
        }
        null
      }

      private def isBrokerReady: Boolean = {
        import NetworkClientBlockingOps._
        try {
          if (!networkClient.isReady(brokerNode)(time))
            networkClient.blockingReady(brokerNode, kafkaConfig.controllerSocketTimeoutMs)(time)
          else true
        } catch {
          case e: Throwable =>
            networkClient.close(broker.id.toString)
            false
        }
      }

      private def handleClientResponse(queuedRequest: QueuedRequest, clientResponse: ClientResponse): Unit = {
        if (clientResponse != null) {
          queuedRequest.callbackOpt.foreach(callback => callback(broker.id, clientResponse.responseBody()))
        }
      }

      private def backoff(): Unit = CoreUtils.swallowTrace(Thread.sleep(100))
    }
  }

  case class QueuedRequest(apiKey: ApiKeys, requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest], callbackOpt: Option[(Int, AbstractResponse) => Unit])
}