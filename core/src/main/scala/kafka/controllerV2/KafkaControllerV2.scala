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

import kafka.common.TopicAndPartition
import kafka.server.KafkaConfig
import kafka.utils.ZkUtils
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

private[kafka] class KafkaControllerV2(kafkaConfig: KafkaConfig, zkUtils: ZkUtils, time: Time, metrics: Metrics) {
  private val _controllerThread = new ControllerThread(kafkaConfig, zkUtils, time, metrics)

  def startup(): Unit = {
    _controllerThread.enqueueStartup()
    _controllerThread.enqueueElect()
    _controllerThread.start()
  }

  def shutdown(): Unit = {
    _controllerThread.shutdown()
  }

  def controlledShutdown(id: Int): Set[TopicAndPartition] = {
    _controllerThread.enqueueControlledShutdown(id)
  }
}
