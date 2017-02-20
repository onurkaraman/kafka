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

object PartitionLeaderSelector {
  def defaultPartitionLeaderSelect(assignment: Seq[Int],
                                   isr: Seq[Int],
                                   liveBrokers: Set[Int],
                                   uncleanLeaderElectionEnabled: Boolean): Option[Int] = {
    assignment.find(id => liveBrokers.contains(id) && isr.contains(id)).orElse {
      if (uncleanLeaderElectionEnabled) {
        assignment.find(liveBrokers.contains)
      } else {
        None
      }
    }
  }

  def reassignedPartitionLeaderSelect(reassignment: Seq[Int],
                                      isr: Seq[Int],
                                      liveBrokers: Set[Int]): Option[Int] = {
    reassignment.find(id => liveBrokers.contains(id) && isr.contains(id))
  }

  def preferredReplicaPartitionLeaderSelect(assignment: Seq[Int],
                                            isr: Seq[Int],
                                            liveBrokers: Set[Int]): Option[Int] = {
    assignment.headOption.filter(id => liveBrokers.contains(id) && isr.contains(id))
  }

  def controlledShutdownPartitionLeaderSelect(isr: Seq[Int],
                                              liveBrokers: Set[Int],
                                              shuttingDownBrokers: Set[Int]): Option[Int] = {
    isr.find(id => liveBrokers.contains(id) && !shuttingDownBrokers.contains(id))
  }
}