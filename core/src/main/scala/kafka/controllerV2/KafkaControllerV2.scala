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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import kafka.admin.{AdminUtils, PreferredReplicaLeaderElectionCommand}
import kafka.api.{KAFKA_0_10_0_IV1, KAFKA_0_10_2_IV0, KAFKA_0_9_0, LeaderAndIsr}
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.log.LogConfig
import kafka.server.{ConfigType, KafkaConfig}
import kafka.utils.{Json, KafkaScheduler, Logging, ReplicationUtils, ShutdownableThread, ZKCheckedEphemeral, ZkUtils}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{AbstractResponse, LeaderAndIsrRequest, PartitionState, StopReplicaRequest, StopReplicaResponse, UpdateMetadataRequest}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time

private[kafka] class KafkaControllerV2(kafkaConfig: KafkaConfig, zkUtils: ZkUtils, time: Time, metrics: Metrics)  extends Logging {
  private val _queue = new LinkedBlockingQueue[ControllerEvent]
  private val _controllerThread = new ControllerThread(s"controller-thread-${kafkaConfig.brokerId}")
  private val _brokerChannelManager = new BrokerChannelManager(kafkaConfig, time, metrics)
  private val _topicCreationListener = new TopicCreationListener
  private var _topicExpansionListeners = Map.empty[String, TopicExpansionListener]
  private val _topicDeletionListener = new TopicDeletionListener
  private val _partitionReassignmentListener = new PartitionReassignmentListener
  private var _partitionReassignmentIsrChangeListeners = Map.empty[TopicAndPartition, PartitionReassignmentIsrChangeListener]
  private val _isrChangeNotificationListener = new IsrChangeNotificationListener
  private val _preferredReplicaLeaderElectionListener = new PreferredReplicaLeaderElectionListener
  private val _brokerChangeListener = new BrokerChangeListener
  private val _controllerChangeListener = new ControllerChangeListener
  private val _scheduler = new KafkaScheduler(1)

  private var _epoch = -1
  private var _topics = Set.empty[String]
  private var _topicsToDelete = Set.empty[String]
  private var _replicasForTopicDeletion = Map.empty[TopicAndPartition, Set[Int]]
  private var _brokerIds = Set.empty[Int]
  private var _liveBrokers = Map.empty[Int, Broker]
  private var _shuttingDownBrokerIds = Set.empty[Int]
  private var _assignments = Map.empty[TopicAndPartition, Seq[Int]]
  private var _reassignments = Map.empty[TopicAndPartition, Seq[Int]]
  private var _leaders = Map.empty[TopicAndPartition, Option[Int]]
  private var _isr = Map.empty[TopicAndPartition, Seq[Int]]
  private var _leaderEpochs = Map.empty[TopicAndPartition, Int]
  private var _zkVersions = Map.empty[TopicAndPartition, Int]
  private var _isController = false

  def startup(): Unit = {
    _queue.add(Startup)
    _queue.add(Elect)
    _controllerThread.start()
  }

  def shutdown(): Unit = {
    _controllerThread.shutdown()
  }

  def controlledShutdown(id: Int): Set[TopicAndPartition] = {
    val resultQueue = new LinkedBlockingQueue[Set[TopicAndPartition]]
    val controlledShutdownEvent = ControlledShutdown(id, resultQueue)
    _queue.add(controlledShutdownEvent)
    resultQueue.take()
  }

  private def partitionsPerTopic: Map[String, Set[TopicAndPartition]] = {
    _assignments.keys.groupBy(_.topic).mapValues(_.toSet)
  }

  private def partitionsPerBroker: Map[Int, Set[TopicAndPartition]] = {
    val x = _assignments.toSeq.flatMap { case (partition, replicas) => replicas.map(replica => (partition, replica)) }
    _brokerIds.map(id => id -> Set.empty[TopicAndPartition]).toMap ++ x.groupBy(_._2).mapValues(_.map(_._1).toSet)
  }

  private def registerZkListeners(): Unit = {
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, _topicCreationListener)
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.DeleteTopicsPath, _topicDeletionListener)
    zkUtils.zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, _partitionReassignmentListener)
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.IsrChangeNotificationPath, _isrChangeNotificationListener)
    zkUtils.zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, _preferredReplicaLeaderElectionListener)
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, _brokerChangeListener)
  }

  private def unregisterZkListeners(): Unit = {
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.BrokerTopicsPath, _topicCreationListener)
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.DeleteTopicsPath, _topicDeletionListener)
    zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.ReassignPartitionsPath, _partitionReassignmentListener)
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.IsrChangeNotificationPath, _isrChangeNotificationListener)
    zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, _preferredReplicaLeaderElectionListener)
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, _brokerChangeListener)
    _topicExpansionListeners.foreach { case (topic, topicExpansionListener) =>
      zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.getTopicPath(topic), topicExpansionListener)
    }
    _partitionReassignmentIsrChangeListeners.foreach { case (partition, partitionReassignmentIsrChangeListener) =>
      zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(partition.topic, partition.partition), partitionReassignmentIsrChangeListener)
    }
  }

  private def buildPartitionStates(partitions: Set[TopicAndPartition]): Map[TopicPartition, PartitionState] = {
    import scala.collection.JavaConverters._
    partitions.map { partition =>
      val epoch = _epoch
      val leader = _leaders(partition).getOrElse(LeaderAndIsr.NoLeader)
      val leaderEpoch = _leaderEpochs(partition)
      val isr = _isr(partition).map(Int.box).toList.asJava
      val zkVersion = _zkVersions(partition)
      val replicas = _assignments(partition).map(Int.box).toSet.asJava
      val partitionState = new PartitionState(epoch, leader, leaderEpoch, isr, zkVersion, replicas)
      val tp = new TopicPartition(partition.topic, partition.partition)
      tp -> partitionState
    }.toMap
  }

  private def buildUpdateMetadataRequest(partitionStates: Map[TopicPartition, PartitionState]): (UpdateMetadataRequest.Builder, Short) = {
    import scala.collection.JavaConverters._
    val partitionStatesAdjustedForTopicDeletion = partitionStates.map { case (partition, partitionState) =>
      if (_topicsToDelete.contains(partition.topic()))
        partition -> new PartitionState(partitionState.controllerEpoch, LeaderAndIsr.LeaderDuringDelete, partitionState.leaderEpoch, partitionState.isr, partitionState.zkVersion, partitionState.replicas)
      else partition -> partitionState
    }
    val updateMetadataRequestVersion: Short =
      if (kafkaConfig.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
      else if (kafkaConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
      else if (kafkaConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
      else 0
    val liveBrokers = if (updateMetadataRequestVersion == 0) {
      _liveBrokers.map { case (id, broker) =>
        val securityProtocol = SecurityProtocol.PLAINTEXT
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val node = broker.getNode(listenerName)
        val endpoints = Seq(new UpdateMetadataRequest.EndPoint(node.host, node.port, securityProtocol, listenerName)).asJava
        new UpdateMetadataRequest.Broker(broker.id, endpoints, broker.rack.orNull)
      }.toSet.asJava
    } else {
      _liveBrokers.map { case (id, broker) =>
        val endpoints = broker.endPoints.map { case (endpoint) =>
          new UpdateMetadataRequest.EndPoint(endpoint.host, endpoint.port, endpoint.securityProtocol, endpoint.listenerName)
        }.asJava
        new UpdateMetadataRequest.Broker(id, endpoints, broker.rack.orNull)
      }.toSet.asJava
    }
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(kafkaConfig.brokerId, _epoch, partitionStatesAdjustedForTopicDeletion.asJava, liveBrokers).setVersion(updateMetadataRequestVersion)
    (updateMetadataRequest.asInstanceOf[UpdateMetadataRequest.Builder], updateMetadataRequestVersion)
  }

  private def buildLeaderAndIsrRequests(partitionStates: Map[TopicPartition, PartitionState]): Map[Int, LeaderAndIsrRequest.Builder] = {
    import scala.collection.JavaConverters._
    val liveLeaderNodes = partitionStates.values
      .flatMap(partitionState => _liveBrokers.get(partitionState.leader).map(_.getNode(ListenerName.forSecurityProtocol(kafkaConfig.interBrokerSecurityProtocol)))).toSet.asJava

    val livePartitionReplicaPairs = partitionStates.keys.flatMap { partition =>
      _assignments(TopicAndPartition(partition.topic(), partition.partition()))
        .filter(_liveBrokers.contains)
        .map(replica => partition -> replica)
    }
    val partitionsPerLiveBroker = livePartitionReplicaPairs.groupBy(_._2).mapValues(_.map(_._1))
    val partitionStatesPerLiveBroker = partitionsPerLiveBroker.map { case(id, partitions) =>
      val partitionStatesForBroker = partitions.map { partition =>
        val tp = new TopicPartition(partition.topic, partition.partition)
        tp -> partitionStates(tp)
      }.toMap.asJava
      id -> partitionStatesForBroker
    }
    partitionStatesPerLiveBroker.map { case (id, partitionStatesForBroker) =>
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(kafkaConfig.brokerId, _epoch, partitionStatesForBroker, liveLeaderNodes)
      id -> leaderAndIsrRequest
    }
  }

  private def buildStopReplicaRequest(partitions: Set[TopicAndPartition], delete: Boolean): StopReplicaRequest.Builder = {
    import scala.collection.JavaConverters._
    new StopReplicaRequest.Builder(kafkaConfig.brokerId, _epoch, delete, partitions.map(tp => new TopicPartition(tp.topic, tp.partition)).asJava)
  }

  private def broadcastMetadata(partitions: Set[TopicAndPartition] = _assignments.keySet): Unit = {
    val partitionStates = buildPartitionStates(partitions)
    val (updateMetadataRequest, updateMetadataRequestVersion) = buildUpdateMetadataRequest(partitionStates)
    _liveBrokers.keys.foreach(id => _brokerChannelManager.enqueueUpdateMetadataRequest(id, updateMetadataRequest, None))
  }

  private def broadcastLeadershipChanges(partitions: Set[TopicAndPartition]): Unit = {
    val partitionStates = buildPartitionStates(partitions)
    val leaderAndIsrRequestsPerBroker = buildLeaderAndIsrRequests(partitionStates)
    val (updateMetadataRequest, updateMetadataRequestVersion) = buildUpdateMetadataRequest(partitionStates)

    leaderAndIsrRequestsPerBroker.foreach { case (id, leaderAndIsrRequest) =>
      _brokerChannelManager.enqueueLeaderAndIsrRequest(id, leaderAndIsrRequest, None)
    }
    _liveBrokers.keys.foreach(id => _brokerChannelManager.enqueueUpdateMetadataRequest(id, updateMetadataRequest, None))
  }

  private def stopReplicas(id: Int, partitions: Set[TopicAndPartition], delete: Boolean): Unit = {
    val stopReplicaRequest = buildStopReplicaRequest(partitions, delete)
    _brokerChannelManager.enqueueStopReplicaRequest(id, stopReplicaRequest, None)
  }

  private def uncleanLeaderElectionEnabled(topic: String): Boolean = {
    LogConfig.fromProps(kafkaConfig.originals, AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)).uncleanLeaderElectionEnable
  }

  private def parseControllerId(controllerString: String) = {
    try {
      Json.parseFull(controllerString) match {
        case Some(m) =>
          val controllerInfo = m.asInstanceOf[Map[String, Any]]
          controllerInfo("brokerid").asInstanceOf[Int]
        case None => throw new RuntimeException("Failed to parse the controller info json [%s].".format(controllerString))
      }
    } catch {
      case _: Throwable => controllerString.toInt
    }
  }

  private def fetchControllerId = zkUtils.readDataMaybeNull(ZkUtils.ControllerPath)._1 match {
    case Some(controllerString) => parseControllerId(controllerString)
    case None => -1
  }

  private def initializePartitions(assignments: Map[TopicAndPartition, Seq[Int]]): Unit = {
    assignments.foreach { case (partition, replicas) =>
      _topics += partition.topic
      _assignments += partition -> replicas
      _isr += partition -> replicas.filter(_liveBrokers.contains)
      _leaders += partition -> PartitionLeaderSelector.defaultPartitionLeaderSelect(replicas, _isr(partition), _liveBrokers.keySet, uncleanLeaderElectionEnabled(partition.topic))
      _leaderEpochs += partition -> 0
      _zkVersions += partition -> 0
      initializePartitionZkState(partition)
    }
  }

  // TODO: maybe refactor to initializePartitionZkStates so we can do async zk writes that join on countdown latch
  private def initializePartitionZkState(partition: TopicAndPartition): Unit = {
    val leaderAndIsr = new LeaderAndIsr(_leaders(partition).getOrElse(LeaderAndIsr.NoLeader), _isr(partition).toList)
    zkUtils.createPersistentPath(
      ZkUtils.getTopicPartitionLeaderAndIsrPath(partition.topic, partition.partition),
      zkUtils.leaderAndIsrZkData(leaderAndIsr, _epoch))
  }

  private def updateLeaderEpoch(partition: TopicAndPartition): Unit = {
    var done = false
    while (!done) {
      val leaderIsrAndControllerEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, partition.topic, partition.partition).get
      // TODO: validate controller epoch
      _leaderEpochs += partition -> (leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch + 1)
      _zkVersions += partition -> leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion
      val leaderAndIsr = new LeaderAndIsr(_leaders(partition).getOrElse(LeaderAndIsr.NoLeader), _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
      val (updateSucceeded, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
      _zkVersions += partition -> zkVersion
      done = updateSucceeded
    }
  }

  private def removeFollowerFromIsr(id: Int, partition: TopicAndPartition): Unit = {
    var done = false
    while (!done) {
      val leaderIsrAndControllerEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, partition.topic, partition.partition).get
      // TODO: validate controller epoch
      if (leaderIsrAndControllerEpoch.leaderAndIsr.isr.contains(id)) {
        val isUncleanLeaderElectionEnabled = uncleanLeaderElectionEnabled(partition.topic)
        _leaderEpochs += partition -> (leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch + 1)
        val removeFromIsr = _isr(partition).toSet.intersect(_liveBrokers.keySet).size > 1 || isUncleanLeaderElectionEnabled
        if (removeFromIsr) {
          _isr += partition -> _isr(partition).filter(replica => replica != id)
        }
        _zkVersions += partition -> leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion
        val leaderAndIsr = new LeaderAndIsr(_leaders(partition).get, _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
        val (updateSucceeded, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
        _zkVersions += partition -> zkVersion
        done = updateSucceeded
      } else return
    }
  }

  class ControllerThread(name: String) extends ShutdownableThread(name = name) {
    override def doWork(): Unit = {
      val controllerEvent = _queue.take()
      info("processing " + controllerEvent)
      controllerEvent.process()
    }
  }

  sealed trait ControllerEvent {
    def process(): Unit
  }

  case class TopicCreation(topics: Set[String]) extends ControllerEvent {
    override def process(): Unit = {
      val newTopics = topics -- _topics
      newTopics.foreach(createTopic)
    }

    private def createTopic(topic: String): Unit = {
      val topicExpansionListener = new TopicExpansionListener(topic)
      _topicExpansionListeners += topic -> topicExpansionListener
      zkUtils.zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), topicExpansionListener)
      val assignments = zkUtils.getPartitionAssignmentForTopics(Seq(topic)).values.flatten.map { case (partition, replicas) =>
        TopicAndPartition(topic, partition) -> replicas
      }.toMap
      initializePartitions(assignments)
      broadcastLeadershipChanges(assignments.keySet)
    }
  }

  case class TopicExpansion(assignments: Map[TopicAndPartition, Seq[Int]]) extends ControllerEvent {
    override def process(): Unit = {
      val newPartitions = assignments.filterKeys(partition => !_assignments.contains(partition) && !_topicsToDelete.contains(partition.topic))
      addPartitions(newPartitions)
    }

    private def addPartitions(assignments: Map[TopicAndPartition, Seq[Int]]): Unit = {
      initializePartitions(assignments)
      broadcastLeadershipChanges(assignments.keySet)
    }
  }

  case class TopicDeletion(private var topics: Set[String]) extends ControllerEvent {
    override def process(): Unit = {
      if (!kafkaConfig.deleteTopicEnable) return
      if (topics.isEmpty) topics = _topicsToDelete // allow a retry thread to just pass in TopicDeletion with empty topics list to retry all _topicsToDelete
      val (existingTopicsToDelete, nonExistentTopicsToDelete) = topics.partition(_topics.contains)

      nonExistentTopicsToDelete.foreach(topic => zkUtils.deletePathRecursive(ZkUtils.getDeleteTopicPath(topic)))
      val newTopicsToDelete = existingTopicsToDelete -- _topicsToDelete
      _topicsToDelete ++= newTopicsToDelete
      _replicasForTopicDeletion ++= newTopicsToDelete
        .flatMap(topic => partitionsPerTopic(topic))
        .map(partition => partition -> _reassignments.getOrElse(partition, _assignments(partition)).toSet)

      val topicsBeingReassigned = _reassignments.keys.map(_.topic).toSet
      val topicsReadyForDeletion = _topicsToDelete -- topicsBeingReassigned
      topicsReadyForDeletion.foreach(deleteTopic)
    }

    private def deleteTopic(topic: String): Unit = {
      val partitionsToDelete = partitionsPerTopic(topic)
      broadcastMetadata(partitionsToDelete)
      val partitionsToDeletePerLiveBroker = partitionsToDelete
        .flatMap(partition => _replicasForTopicDeletion(partition).filter(_liveBrokers.contains).map(replica => partition -> replica))
        .groupBy(_._2).mapValues(_.map(_._1).toSet)
      partitionsToDeletePerLiveBroker.foreach { case (id, partitions) =>
        stopReplicas(id, partitions, delete = false)
      }
      partitionsToDeletePerLiveBroker.foreach { case (id, partitions) =>
        val stopReplicaRequest = buildStopReplicaRequest(partitions, delete = true)
        _brokerChannelManager.enqueueStopReplicaRequest(id, stopReplicaRequest, Option(deleteTopicDeleteReplicaCallback))
      }
    }

    private def deleteTopicDeleteReplicaCallback(id: Int, abstractResponse: AbstractResponse): Unit = {
      _queue.add(new TopicDeletionDeleteReplicaResult(id, abstractResponse))
    }
  }

  case class TopicDeletionDeleteReplicaResult(id: Int, abstractResponse: AbstractResponse) extends ControllerEvent {
    import scala.collection.JavaConverters._

    override def process(): Unit = {
      val stopReplicaResponse = abstractResponse.asInstanceOf[StopReplicaResponse]
      val replicaDeletionResults = stopReplicaResponse.responses.asScala.map { case (partition, error) => new TopicAndPartition(partition.topic(), partition.partition()) -> error }
      val successfulReplicaDeletionResults = replicaDeletionResults.filter { case (partition, error) => error == Errors.NONE.code }
      successfulReplicaDeletionResults.keys.foreach { partition =>
        val remainingReplicas = _replicasForTopicDeletion(partition).filter(replica => replica != id)
        if (remainingReplicas.isEmpty) {
          _replicasForTopicDeletion -= partition
          val topicDeletionComplete = !_replicasForTopicDeletion.keys.exists(_.topic == partition.topic)
          if (topicDeletionComplete) {
            finishTopicDeletion(partition.topic)
          }
        } else {
          _replicasForTopicDeletion += partition -> remainingReplicas
        }
      }
    }

    private def finishTopicDeletion(topic: String): Unit = {
      zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.getTopicPath(topic), _topicExpansionListeners(topic))
      zkUtils.zkClient.deleteRecursive(ZkUtils.getTopicPath(topic))
      zkUtils.zkClient.deleteRecursive(ZkUtils.getEntityConfigPath(ConfigType.Topic, topic))
      zkUtils.zkClient.delete(ZkUtils.getDeleteTopicPath(topic))
      val partitions = partitionsPerTopic(topic)
      _replicasForTopicDeletion --= partitions
      _assignments --= partitions
      _leaders --= partitions
      _isr --= partitions
      _leaderEpochs --= partitions
      _zkVersions --= partitions
      _topicExpansionListeners -= topic
      _topicsToDelete -= topic
      _topics -= topic
    }
  }

  case class PartitionReassignment(partition: TopicAndPartition, replicas: Seq[Int]) extends ControllerEvent {
    override def process(): Unit = {
      if (_topicsToDelete.contains(partition.topic)) {
        _reassignments -= partition
        PartitionReassignmentIsrChange.unregisterPartitionReassignmentIsrChangeListener(partition)
        zkUtils.updatePartitionReassignmentData(_reassignments)
      } else if (_assignments.contains(partition) && !_reassignments.contains(partition)) {
        beginPartitionReassignment(partition, replicas)
      }
    }

    private def beginPartitionReassignment(partition: TopicAndPartition, replicas: Seq[Int]): Unit = {
      if (_assignments(partition) == replicas) return
      _assignments += partition -> (_assignments(partition) ++ replicas)
      _reassignments += partition -> replicas
      registerPartitionReassignmentIsrChangeListener(partition)
      val assignment = partitionsPerTopic(partition.topic).map(tp => tp -> _assignments(tp)).toMap
      val jsonPartitionMap = zkUtils.replicaAssignmentZkData(assignment.map { case (tp, replicas) => tp.partition.toString -> replicas })
      zkUtils.updatePersistentPath(ZkUtils.getTopicPath(partition.topic), jsonPartitionMap)
      updateLeaderEpoch(partition)
      broadcastLeadershipChanges(Set(partition))
    }

    private def registerPartitionReassignmentIsrChangeListener(partition: TopicAndPartition): Unit = {
      val partitionReassignmentIsrChangeListener = new PartitionReassignmentIsrChangeListener(partition)
      _partitionReassignmentIsrChangeListeners += partition -> partitionReassignmentIsrChangeListener
      zkUtils.zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(partition.topic, partition.partition), partitionReassignmentIsrChangeListener)
    }
  }

  object PartitionReassignmentIsrChange {
    def maybeFinishPartitionReassignment(partition: TopicAndPartition, isr: Seq[Int]): Unit = {
      if (_reassignments(partition).diff(isr).isEmpty) {
        _isr += partition -> isr
        val needsLeaderElection = _leaders(partition).exists(id => !_reassignments(partition).contains(id) || !_liveBrokers.contains(id))
        if (!needsLeaderElection) {
          updateLeaderEpoch(partition)
        } else {
          _leaders += partition -> PartitionLeaderSelector.reassignedPartitionLeaderSelect(_reassignments(partition), _isr(partition), _liveBrokers.keySet)
          _leaderEpochs += partition -> (_leaderEpochs(partition) + 1)
          val leaderAndIsr = new LeaderAndIsr(_leaders(partition).get, _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
          val (_, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
          _zkVersions += partition -> zkVersion
        }
        broadcastLeadershipChanges(Set(partition))
        val oldReplicas = _assignments(partition).diff(_reassignments(partition))
        oldReplicas.foreach(id => stopReplicas(id, Set(partition), delete = true))
        _isr += partition -> _reassignments(partition)
        _assignments += partition -> _reassignments(partition)
        _reassignments -= partition
        _leaderEpochs += partition -> (_leaderEpochs(partition) + 1)
        val leaderAndIsr = new LeaderAndIsr(_leaders(partition).get, _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
        val (_, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
        _zkVersions += partition -> zkVersion
        val assignment = partitionsPerTopic(partition.topic).map(tp => tp -> _assignments(tp)).toMap
        val jsonPartitionMap = zkUtils.replicaAssignmentZkData(assignment.map { case (tp, replicas) => tp.partition.toString -> replicas })
        zkUtils.updatePersistentPath(ZkUtils.getTopicPath(partition.topic), jsonPartitionMap)
        zkUtils.updatePartitionReassignmentData(_reassignments)
        unregisterPartitionReassignmentIsrChangeListener(partition)
        broadcastLeadershipChanges(Set(partition))
      }
    }

    def unregisterPartitionReassignmentIsrChangeListener(partition: TopicAndPartition): Unit = {
      val partitionReassignmentIsrChangeListener = _partitionReassignmentIsrChangeListeners(partition)
      _partitionReassignmentIsrChangeListeners -= partition
      zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(partition.topic, partition.partition), partitionReassignmentIsrChangeListener)
    }
  }

  case class PartitionReassignmentIsrChange(partition: TopicAndPartition, isr: Seq[Int]) extends ControllerEvent {
    override def process(): Unit = {
      if (!_reassignments.contains(partition)) return
      PartitionReassignmentIsrChange.maybeFinishPartitionReassignment(partition, isr)
    }
  }

  case class IsrChangeNotification(sequenceNumbers: Seq[String]) extends ControllerEvent {
    override def process(): Unit = {
      try {
        val partitions = sequenceNumbers.flatMap(sequenceNumber => zkUtils.parseIsrChangeNotification(sequenceNumber))
        if (partitions.isEmpty) return
        val leaderIsrAndControllerEpochs = zkUtils.getPartitionLeaderAndIsrForTopics(zkUtils.zkClient, partitions.toSet)
        val updatedIsrs = leaderIsrAndControllerEpochs.mapValues(_.leaderAndIsr.isr.toSeq)
        _isr ++= updatedIsrs
        broadcastMetadata(partitions.toSet)
      } finally {
        sequenceNumbers.foreach(sequenceNumber => zkUtils.deletePath(ZkUtils.IsrChangeNotificationPath + "/" + sequenceNumber))
      }
    }
  }

  case class PreferredReplicaLeaderElection(partition: TopicAndPartition) extends ControllerEvent {
    override def process(): Unit = {
      if (!_assignments.contains(partition) || _topicsToDelete.contains(partition.topic)) return
      electPreferredReplicaLeader(partition)
    }

    private def electPreferredReplicaLeader(partition: TopicAndPartition): Unit = {
      val leaderOpt = PartitionLeaderSelector.preferredReplicaPartitionLeaderSelect(_assignments(partition), _isr(partition), _liveBrokers.keySet)
      if (leaderOpt.isEmpty || leaderOpt == _leaders(partition)) return
      val zkVersionBeforeUpdate = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, partition.topic, partition.partition).map(leaderIsrAndControllerEpoch => leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion).get
      _leaders += partition -> leaderOpt
      _leaderEpochs += partition -> (_leaderEpochs(partition) + 1)
      _zkVersions += partition -> zkVersionBeforeUpdate
      val leaderAndIsr = new LeaderAndIsr(_leaders(partition).get, _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
      val (_, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
      _zkVersions += partition -> zkVersion
      // TODO: decide if we should alternatively just make PreferredReplicaLeaderElection take all of the partitions
      // to avoid this hack and maybe apply a similar pattern to PartitionReassignment.
      if (partition == zkUtils.getPartitionsUndergoingPreferredReplicaElection().last)
        zkUtils.deletePath(ZkUtils.PreferredReplicaLeaderElectionPath)
      broadcastLeadershipChanges(Set(partition))
    }
  }

  case object AutoPreferredReplicaLeaderElection extends ControllerEvent {
    override def process(): Unit = {
      partitionsPerBroker.foreach { case (id, partitionsOnBroker) =>
        val eligiblePartitionsOnBroker = partitionsOnBroker.filter(partition => !_topicsToDelete.contains(partition.topic))
        val partitionsPreferredButNotLeadByBroker = eligiblePartitionsOnBroker.filter(partition => _assignments(partition).head == id && _leaders(partition) != Some(id))
        val imbalanceRatio = partitionsPreferredButNotLeadByBroker.size.toDouble / eligiblePartitionsOnBroker.size
        if (imbalanceRatio > kafkaConfig.leaderImbalancePerBrokerPercentage.toDouble / 100) {
          partitionsPreferredButNotLeadByBroker.foreach { partition =>
            val leaderOpt = PartitionLeaderSelector.preferredReplicaPartitionLeaderSelect(_assignments(partition), _isr(partition), _liveBrokers.keySet)
            if (!_reassignments.contains(partition) && !leaderOpt.isEmpty) {
              val zkVersionBeforeUpdate = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, partition.topic, partition.partition).map(leaderIsrAndControllerEpoch => leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion).get
              _leaders += partition -> leaderOpt
              _leaderEpochs += partition -> (_leaderEpochs(partition) + 1)
              _zkVersions += partition -> zkVersionBeforeUpdate
              val leaderAndIsr = new LeaderAndIsr(_leaders(partition).get, _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
              val (_, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
              _zkVersions += partition -> zkVersion
            }
          }
          broadcastLeadershipChanges(partitionsPreferredButNotLeadByBroker)
        }
      }
    }
  }

  case class BrokerChange(brokers: Set[Int]) extends ControllerEvent {
    override def process(): Unit = {
      val addedBrokers = brokers -- _liveBrokers.keySet
      val removedBrokers = _liveBrokers.keySet -- brokers
      addedBrokers.foreach(addBroker)
      removedBrokers.foreach(removeBroker)
    }

    private def addBroker(id: Int): Unit = {
      val brokerOpt = zkUtils.getBrokerInfo(id)
      if (!brokerOpt.isDefined) return
      _brokerIds += id
      _liveBrokers += id -> brokerOpt.get
      _shuttingDownBrokerIds -= id
      _brokerChannelManager.addChannel(brokerOpt.get)

      broadcastMetadata()
      val partitionsOnBroker = partitionsPerBroker(id)
      if (partitionsOnBroker.isEmpty) return
      broadcastLeadershipChanges(partitionsOnBroker)
      val (leaderlessPartitionsOnBrokerUndergoingReassignment, leaderlessPartitionsOnBrokerNotUndergoingReassignment) =
        partitionsOnBroker
          .filter(partition => _leaders(partition).isEmpty)
          .partition(_reassignments.contains)
      val newPartitionsOnBroker  = partitionsOnBroker.filter(partition => _isr(partition).isEmpty && _leaderEpochs(partition) == 0)
      newPartitionsOnBroker.foreach { partition =>
        _isr += partition -> Seq(id)
      }
      val newlyElectedLeaderPerPartitionOnBroker = leaderlessPartitionsOnBrokerNotUndergoingReassignment.map { partition =>
        partition -> PartitionLeaderSelector.defaultPartitionLeaderSelect(_assignments(partition), _isr(partition), _liveBrokers.keySet, uncleanLeaderElectionEnabled(partition.topic))
      }.toMap
      _leaders ++= newlyElectedLeaderPerPartitionOnBroker
      newlyElectedLeaderPerPartitionOnBroker.foreach { case (partition, leaderOpt) =>
        if (leaderOpt.isDefined) {
          _leaderEpochs += partition -> (_leaderEpochs(partition) + 1)
          val leaderAndIsr = new LeaderAndIsr(leaderOpt.get, _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
          val (_, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
          _zkVersions += partition -> zkVersion
        }
      }
      broadcastLeadershipChanges(newlyElectedLeaderPerPartitionOnBroker.keySet)
      leaderlessPartitionsOnBrokerUndergoingReassignment.foreach(partition => PartitionReassignmentIsrChange.maybeFinishPartitionReassignment(partition, _isr(partition)))
    }

    private def removeBroker(id: Int): Unit = {
      _brokerChannelManager.removeChannel(id)
      _liveBrokers -= id
      _shuttingDownBrokerIds -= id
      val partitionsOnBroker = partitionsPerBroker(id)
      val (leaderReplicasOnBroker, followerReplicasOnBroker) = partitionsOnBroker.partition(partition => _leaders(partition) == Some(id))
      leaderReplicasOnBroker.foreach { partition =>
        val isUncleanLeaderElectionEnabled = uncleanLeaderElectionEnabled(partition.topic)
        val leaderOpt = PartitionLeaderSelector.defaultPartitionLeaderSelect(_assignments(partition), _isr(partition), _liveBrokers.keySet, isUncleanLeaderElectionEnabled)
        if (leaderOpt.isDefined) {
          _leaders += partition -> leaderOpt
          _leaderEpochs += partition -> (_leaderEpochs(partition) + 1)
          val removeFromIsr = _isr(partition).toSet.intersect(_liveBrokers.keySet).size > 1 || isUncleanLeaderElectionEnabled
          if (removeFromIsr) {
            _isr += partition -> _isr(partition).filter(replica => replica != id)
          }
          val leaderAndIsr = new LeaderAndIsr(_leaders(partition).getOrElse(LeaderAndIsr.NoLeader), _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
          val (_, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
          _zkVersions += partition -> zkVersion
        }
      }
      followerReplicasOnBroker.foreach(partition => removeFollowerFromIsr(id, partition))
      if (partitionsOnBroker.nonEmpty) {
        broadcastLeadershipChanges(partitionsOnBroker)
      } else {
        broadcastMetadata()
      }
    }
  }

  case class ControlledShutdown(id: Int, resultQueue: LinkedBlockingQueue[Set[TopicAndPartition]]) extends ControllerEvent {
    override def process(): Unit = {
      if (!_liveBrokers.contains(id)) return
      val partitionsOnBroker = partitionsPerBroker(id)
      val partitionsToControlledShutdown = partitionsOnBroker.filter(partition => _assignments(partition).size > 1)
      val (leaderReplicasToControlledShutdown, followerReplicasToControlledShutdown) = partitionsToControlledShutdown.partition(partition => _leaders(partition) == Some(id))
      _shuttingDownBrokerIds += id
      leaderReplicasToControlledShutdown.foreach { partition =>
        val leaderOpt = PartitionLeaderSelector.controlledShutdownPartitionLeaderSelect(_isr(partition), _liveBrokers.keySet, _shuttingDownBrokerIds)
        if (leaderOpt.isDefined) {
          _leaders += partition -> leaderOpt
          _leaderEpochs += partition -> (_leaderEpochs(partition) + 1)
          _isr += partition -> _isr(partition).diff(_shuttingDownBrokerIds.toSeq)
          val leaderAndIsr = new LeaderAndIsr(_leaders(partition).getOrElse(LeaderAndIsr.NoLeader), _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
          val (_, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
          _zkVersions += partition -> zkVersion
        }
      }
      if (followerReplicasToControlledShutdown.nonEmpty) {
        stopReplicas(id, followerReplicasToControlledShutdown, delete = false)
      }
      followerReplicasToControlledShutdown.foreach(partition => removeFollowerFromIsr(id, partition))
      if (partitionsToControlledShutdown.nonEmpty) {
        broadcastLeadershipChanges(partitionsToControlledShutdown)
      }
      resultQueue.put(Set.empty[TopicAndPartition])
    }
  }

  case object Startup extends ControllerEvent {
    override def process(): Unit = {
      zkUtils.zkClient.subscribeDataChanges(ZkUtils.ControllerPath, _controllerChangeListener)
    }
  }

  case object Elect extends ControllerEvent {
    override def process(): Unit = {
      val controllerId = fetchControllerId
      if (controllerId != -1) return
      val controllerJson = Json.encode(Map("version" -> 1, "brokerid" -> kafkaConfig.brokerId, "timestamp" -> time.milliseconds().toString))
      try {
        val zkCheckedEphemeral = new ZKCheckedEphemeral(ZkUtils.ControllerPath,
          controllerJson,
          zkUtils.zkConnection.getZookeeper,
          JaasUtils.isZkSecurityEnabled())
        zkCheckedEphemeral.create()
        _isController = true
        init()
        _scheduler.startup()
        if (kafkaConfig.deleteTopicEnable) {
          _scheduler.schedule("topic-deletion-retry-task", () => _queue.add(TopicDeletion(Set.empty[String])), period = 10, unit = TimeUnit.SECONDS)
        }
        if (kafkaConfig.autoLeaderRebalanceEnable) {
          _scheduler.schedule("auto-leader-rebalance-task", () => _queue.add(AutoPreferredReplicaLeaderElection), delay = 5, period = kafkaConfig.leaderImbalanceCheckIntervalSeconds, unit = TimeUnit.SECONDS)
        }
      } catch {
        case e: ZkNodeExistsException =>
        case e: Throwable => {
          error("HIT EXCEPTION DURING ELECT: ", e)
          _queue.add(Resign(fetchControllerId))
        }
      }
    }

    private def init(): Unit = {
      try {
        zkUtils.zkClient.createPersistent(ZkUtils.ControllerEpochPath, "1")
        _epoch = 1
      } catch {
        case e: ZkNodeExistsException =>
          val (data, stat) = zkUtils.readData(ZkUtils.ControllerEpochPath)
          val incrementedEpoch = data.toInt + 1
          val epochZkVersion = stat.getVersion
          val (updateSucceeded, _) = zkUtils.conditionalUpdatePersistentPath(ZkUtils.ControllerEpochPath, incrementedEpoch.toString, epochZkVersion)
          if (!updateSucceeded) {
            _queue.add(Resign(fetchControllerId))
            return
          } else {
            _epoch = incrementedEpoch
          }
      }
      registerZkListeners()
      _topics = zkUtils.getAllTopics().toSet
      _assignments = zkUtils.getReplicaAssignmentForTopics(_topics.toSeq).toMap
      _brokerIds = _assignments.values.flatMap(_.toSet).toSet
      _liveBrokers = zkUtils.getAllBrokersInCluster().map(broker => broker.id -> broker).toMap
      val leaderIsrAndControllerEpochs = zkUtils.getPartitionLeaderAndIsrForTopics(zkUtils.zkClient, _assignments.keySet)
      leaderIsrAndControllerEpochs.foreach { case (partition, leaderIsrAndControllerEpoch) =>
        _leaders += partition -> Option(leaderIsrAndControllerEpoch.leaderAndIsr.leader).filter(_ >= 0)
        _isr += partition -> leaderIsrAndControllerEpoch.leaderAndIsr.isr.toSeq
        _leaderEpochs += partition -> leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch
        _zkVersions += partition -> leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion
      }
      _liveBrokers.values.foreach(_brokerChannelManager.addChannel)
      val preferredReplicaLeaderElections = zkUtils.getPartitionsUndergoingPreferredReplicaElection()
      val incompletePreferredReplicaLeaderElections = preferredReplicaLeaderElections.filter { partition =>
        _assignments.contains(partition) && _leaders(partition) != _assignments(partition).headOption
      }
      val reassignments = zkUtils.getPartitionsBeingReassigned()
      val incompleteReassignments = reassignments.filter { case (partition, context) =>
        _assignments.contains(partition) && _assignments(partition) != context.newReplicas
      }.mapValues(_.newReplicas).toMap
      zkUtils.updatePartitionReassignmentData(incompleteReassignments)
      _reassignments = incompleteReassignments
      if (kafkaConfig.deleteTopicEnable) {
        loadDeleteTopicStateFromZk()
      }
      broadcastLeadershipChanges(_leaders.filter { case (partition, leaderOpt) => leaderOpt.isDefined }.keySet)
      val (partitionsInitializedInZk, partitionsNotInitializedInZk) = _assignments.partition { case (partition, replicas) => leaderIsrAndControllerEpochs.contains(partition) }
      partitionsInitializedInZk.foreach { case(partition, replicas) =>
        val isUncleanLeaderElectionEnabled = uncleanLeaderElectionEnabled(partition.topic)
        _leaders += partition -> PartitionLeaderSelector.defaultPartitionLeaderSelect(_assignments(partition), _isr(partition), _liveBrokers.keySet, isUncleanLeaderElectionEnabled)
        _leaderEpochs += partition -> (_leaderEpochs(partition) + 1)
        val leaderAndIsr = new LeaderAndIsr(_leaders(partition).getOrElse(LeaderAndIsr.NoLeader), _leaderEpochs(partition), _isr(partition).toList, _zkVersions(partition))
        val (_, zkVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, partition.topic, partition.partition, leaderAndIsr, _epoch, _zkVersions(partition))
        _zkVersions += partition -> zkVersion
      }
      initializePartitions(partitionsNotInitializedInZk)
      broadcastLeadershipChanges(_assignments.keySet)
      _topics.foreach { topic =>
        val topicExpansionListener = new TopicExpansionListener(topic)
        _topicExpansionListeners += topic -> topicExpansionListener
        zkUtils.zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), topicExpansionListener)
      }
      _reassignments.foreach { case (partition, replicas) =>
        PartitionReassignment(partition, replicas).process()
      }
      incompletePreferredReplicaLeaderElections.foreach(partition => PreferredReplicaLeaderElection(partition).process())
      broadcastMetadata()
    }

    def loadDeleteTopicStateFromZk(): Unit = {
      val topicsToDelete = zkUtils.getChildren(ZkUtils.DeleteTopicsPath).toSet
      val (existingTopicsToDelete, nonExistentTopicsToDelete) = topicsToDelete.partition(_topics.contains)

      nonExistentTopicsToDelete.foreach(topic => zkUtils.deletePathRecursive(ZkUtils.getDeleteTopicPath(topic)))
      _topicsToDelete ++= existingTopicsToDelete
      _replicasForTopicDeletion ++= existingTopicsToDelete
        .flatMap(topic => partitionsPerTopic(topic))
        .map(partition => partition -> _reassignments.getOrElse(partition, _assignments(partition)).toSet)
    }
  }

  // TODO: use zookeeper multiop to atomically delete /controller znode if my /controller_epoch znode version matches
  case class Resign(newController: Int) extends ControllerEvent {
    override def process(): Unit = {
      if (!_isController || newController == kafkaConfig.brokerId) return
      _isController = false
      unregisterZkListeners()
      _scheduler.shutdown()
      _topicExpansionListeners = Map.empty[String, TopicExpansionListener]
      _partitionReassignmentIsrChangeListeners = Map.empty[TopicAndPartition, PartitionReassignmentIsrChangeListener]
      _queue.clear()
      _brokerChannelManager.reset()
      _epoch = -1
      _topics = Set.empty[String]
      _brokerIds = Set.empty[Int]
      _liveBrokers = Map.empty[Int, Broker]
      _shuttingDownBrokerIds = Set.empty[Int]
      _assignments = Map.empty[TopicAndPartition, Seq[Int]]
      _reassignments = Map.empty[TopicAndPartition, Seq[Int]]
      _leaders = Map.empty[TopicAndPartition, Option[Int]]
      _isr = Map.empty[TopicAndPartition, Seq[Int]]
      _leaderEpochs = Map.empty[TopicAndPartition, Int]
      _zkVersions = Map.empty[TopicAndPartition, Int]
    }
  }

  class TopicCreationListener extends IZkChildListener {
    import scala.collection.JavaConverters._
    override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
      _queue.put(TopicCreation(currentChilds.asScala.toSet))
    }
  }

  class TopicExpansionListener(topic: String) extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: scala.Any): Unit = {
      _queue.put(TopicExpansion(zkUtils.getReplicaAssignmentForTopics(Seq(topic)).toMap))
    }

    override def handleDataDeleted(dataPath: String): Unit = {}
  }

  class TopicDeletionListener extends IZkChildListener {
    import scala.collection.JavaConverters._
    override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
      _queue.put(TopicDeletion(currentChilds.asScala.toSet))
    }
  }

  class PartitionReassignmentListener extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: scala.Any): Unit = {
      val assignments = ZkUtils.parsePartitionReassignmentData(data.toString)
      assignments.foreach { case (partition, replicas) => _queue.put(PartitionReassignment(partition, replicas)) }
    }

    override def handleDataDeleted(dataPath: String): Unit = {}
  }

  class PartitionReassignmentIsrChangeListener(partition: TopicAndPartition) extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: scala.Any): Unit = {
      val leaderAndIsrOpt = zkUtils.getLeaderAndIsrForPartition(partition.topic, partition.partition)
      leaderAndIsrOpt.foreach(leaderAndIsr => _queue.put(PartitionReassignmentIsrChange(partition, leaderAndIsr.isr)))
    }

    override def handleDataDeleted(dataPath: String): Unit = {}
  }

  class IsrChangeNotificationListener extends IZkChildListener {
    import scala.collection.JavaConverters._
    override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
      _queue.add(IsrChangeNotification(currentChilds.asScala))
    }
  }

  class PreferredReplicaLeaderElectionListener extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: scala.Any): Unit = {
      val partitions = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString)
      partitions.foreach(partition => _queue.put(PreferredReplicaLeaderElection(partition)))
    }

    override def handleDataDeleted(dataPath: String): Unit = {}
  }

  class BrokerChangeListener extends IZkChildListener {
    import scala.collection.JavaConverters._
    override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
      _queue.add(BrokerChange(currentChilds.asScala.map(_.toInt).toSet))
    }
  }

  class ControllerChangeListener extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: scala.Any): Unit = {
      _queue.add(Resign(parseControllerId(data.toString)))
    }

    override def handleDataDeleted(dataPath: String): Unit = {
      _queue.add(Resign(fetchControllerId))
      _queue.add(Elect)
    }
  }
}
