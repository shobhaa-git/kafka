package integration.kafka.tiered.storage

import kafka.tiered.storage.{TieredStorageTestBuilder, TieredStorageTestHarness}

import scala.collection.Seq

class ReassignReplicaShrinkTest extends TieredStorageTestHarness {
  private val (broker0, broker1, topicA, p0, p1) = (0, 1, "topicA", 0, 1)

  /* Cluster of two brokers */
  override protected def brokerCount: Int = 2

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    val assignment = Map(p0 -> Seq(broker0, broker1), p1 -> Seq(broker1, broker0))
    builder
      .createTopic(topicA, partitionsCount = 2, replicationFactor = 2, maxBatchCountPerSegment = 1, assignment)
      // send records to partition 0
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker0, topicA, p0, baseOffset = 1, ("k2", "v2"))
      .expectEarliestOffsetInLogDirectory(topicA, p0, earliestOffset = 2)
      // send records to partition 1
      .produce(topicA, p1, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectSegmentToBeOffloaded(broker1, topicA, p1, baseOffset = 0, ("k1", "v1"))
      .expectSegmentToBeOffloaded(broker1, topicA, p1, baseOffset = 1, ("k2", "v2"))
      .expectEarliestOffsetInLogDirectory(topicA, p1, earliestOffset = 2)
      .shrinkReplica(topicA, p0, replicaIds = Seq(broker1))
      .shrinkReplica(topicA, p1, replicaIds = Seq(broker0))
      .expectLeader(topicA, p0, broker1)
      .expectLeader(topicA, p1, broker0)
      .expectFetchFromTieredStorage(fromBroker = 1, topicA, p0, remoteFetchRequestCount = 2)
      .consume(topicA, p0, fetchOffset = 0, expectedTotalRecord = 3, expectedRecordsFromSecondTier = 2)
      .expectFetchFromTieredStorage(fromBroker = 0, topicA, p1, remoteFetchRequestCount = 2)
      .consume(topicA, p1, fetchOffset = 0, expectedTotalRecord = 3, expectedRecordsFromSecondTier = 2)
  }
}
