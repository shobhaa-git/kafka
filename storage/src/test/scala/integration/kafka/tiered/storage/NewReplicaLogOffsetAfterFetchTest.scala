/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tiered.storage

class NewReplicaLogOffsetAfterFetchTest extends TieredStorageTestHarness {
  private val (broker1, broker2, broker3, topicA, p0) = (0, 1, 2, "topicA", 0)

  /* Cluster of two brokers */
  override protected def brokerCount: Int = 3

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    val assignment = Map(p0 -> Seq(broker1, broker2, broker3))

    builder
      .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 2, assignment)
      .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"))
      .withBatchSize(topicA, p0, 2)
      .stop(broker3) // TopicPartition is replicated with broker 1 (leader) and broker 2 (follower)
      .expectLeader(topicA, p0, broker1)
      .expectInIsr(topicA, p0, broker2)
      .produce(topicA, p0, ("k3", "v3"), ("k4", "v4"))
      .withBatchSize(topicA, p0, 2)
      .expectSegmentToBeOffloaded(broker1, topicA, p0, baseOffset = 0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
      .expectLogStartOffset(topicA, p0, broker1, 0)
      .expectLogStartOffset(topicA, p0, broker1, 2, true)
      .start(broker3)
      .expectLeader(topicA, p0, broker1)
      .expectInIsr(topicA, p0, broker2)
      .stop(broker2) // TopicPartition is replicated with broker 1 (leader)  and broker 3 (follower)
      .expectLeader(topicA, p0, broker1)
      .expectInIsr(topicA, p0, broker3)
      .start(broker2)
      .expectLeader(topicA, p0, broker1)
      .stop(broker1) // TopicPartition is replicated with broker 3 (leader) and broker 2 (follower)
      .expectLeader(topicA, p0, broker3)
      .expectInIsr(topicA, p0, broker2)
      .expectLogStartOffset(topicA, p0, broker1, 0)
      .complete()
  }
}
