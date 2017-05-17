package kafka.controller

import kafka.common.TopicAndPartition

import scala.collection.{Map, Set, mutable}

class ClusterStateMachine {
  private val partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty
  private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty
  private val validPreviousPartitionStates: Map[PartitionState, Set[PartitionState]] =
    Map(NewPartition -> Set(NonExistentPartition),
      OnlinePartition -> Set(NewPartition, OnlinePartition, OfflinePartition),
      OfflinePartition -> Set(NewPartition, OnlinePartition, OfflinePartition),
      NonExistentPartition -> Set(OfflinePartition))
  private val validPreviousReplicaStates: Map[ReplicaState, Set[ReplicaState]] =
    Map(NewReplica -> Set(NonExistentReplica),
      OnlineReplica -> Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible),
      OfflineReplica -> Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible),
      ReplicaDeletionStarted -> Set(OfflineReplica),
      ReplicaDeletionSuccessful -> Set(ReplicaDeletionStarted),
      ReplicaDeletionIneligible -> Set(ReplicaDeletionStarted),
      NonExistentReplica -> Set(ReplicaDeletionSuccessful))

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

  private def assertValidPartitionStateTransition(topicAndPartition: TopicAndPartition, targetState: PartitionState): Unit = {
    if (!validPreviousPartitionStates(targetState).contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, validPreviousPartitionStates(targetState).mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  private def assertValidReplicaStateTransition(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    if (!validPreviousReplicaStates(targetState).contains(replicaState(partitionAndReplica)))
      throw new IllegalStateException("Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, validPreviousReplicaStates(targetState).mkString(","), targetState) + ". Instead it is in %s state"
        .format(replicaState(partitionAndReplica)))
  }
}