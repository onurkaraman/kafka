package kafka.controller

import kafka.common.TopicAndPartition

import scala.collection.{Map, Set, mutable}

class ClusterStateMachine {
  private val partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty
  private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty

  def initPartitionState(initialPartitionState: Map[TopicAndPartition, PartitionState]): Unit = {
    partitionState ++= initialPartitionState
  }

  def initReplicaState(initialReplicaState: Map[PartitionAndReplica, ReplicaState]): Unit = {
    replicaState ++= initialReplicaState
  }

  def clear(): Unit = {
    partitionState.clear()
    replicaState.clear()
  }

  def handlePartitionStateChange(topicAndPartition: TopicAndPartition, targetState: PartitionState): Unit = {
    partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
    assertValidPartitionStateTransition(topicAndPartition, targetState)
    partitionState.put(topicAndPartition, targetState)
  }

  def handleReplicaStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
    assertValidReplicaStateTransition(partitionAndReplica, targetState)
    replicaState.put(partitionAndReplica, targetState)
  }

  def currentPartitionState(topicAndPartition: TopicAndPartition) = partitionState.get(topicAndPartition)
  def currentReplicaState(partitionAndReplica: PartitionAndReplica) = replicaState.get(partitionAndReplica)

  def partitionsInStates(states: Set[PartitionState]) = partitionState.filter(kv => states.contains(kv._2)).keySet
  def replicasInStates(states: Set[ReplicaState]) = replicaState.filter(kv => states.contains(kv._2)).keySet

  private def assertValidPartitionStateTransition(topicAndPartition: TopicAndPartition, targetState: PartitionState): Unit = {
    if (!targetState.validPreviousStates.contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, targetState.validPreviousStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  private def assertValidReplicaStateTransition(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    if (!targetState.validPreviousStates.contains(replicaState(partitionAndReplica)))
      throw new IllegalStateException("Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, targetState.validPreviousStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(replicaState(partitionAndReplica)))
  }
}

sealed trait PartitionState {
  def state: Byte
  def validPreviousStates: Set[PartitionState]
}

case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}

sealed trait ReplicaState {
  def state: Byte
  def validPreviousStates: Set[ReplicaState]
}

case object NewReplica extends ReplicaState {
  val state: Byte = 1
  val validPreviousStates: Set[ReplicaState] = Set(NonExistentReplica)
}

case object OnlineReplica extends ReplicaState {
  val state: Byte = 2
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object OfflineReplica extends ReplicaState {
  val state: Byte = 3
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object ReplicaDeletionStarted extends ReplicaState {
  val state: Byte = 4
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica)
}

case object ReplicaDeletionSuccessful extends ReplicaState {
  val state: Byte = 5
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object ReplicaDeletionIneligible extends ReplicaState {
  val state: Byte = 6
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object NonExistentReplica extends ReplicaState {
  val state: Byte = 7
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionSuccessful)
}