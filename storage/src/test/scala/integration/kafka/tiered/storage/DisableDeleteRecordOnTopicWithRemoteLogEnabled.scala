/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package integration.kafka.tiered.storage

import kafka.tiered.storage.{TieredStorageTestBuilder, TieredStorageTestHarness}
import org.apache.kafka.common.errors.UnsupportedActionForTopicException

class DisableDeleteRecordOnTopicWithRemoteLogEnabled extends TieredStorageTestHarness {
    private val (broker0, broker1, topicA, p0) = (0, 1, "topicA", 0)

    /* Cluster of two brokers */
    override protected def brokerCount: Int = 2

    override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
        val assignment = Map(p0 -> Seq(broker0, broker1))
        builder
          .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 1, assignment)
          // send records to partition 0
          .produce(topicA, p0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
          .deleteRecords(topicA, p0, beforeOffset = 1, Some(classOf[UnsupportedActionForTopicException]))
    }
}
