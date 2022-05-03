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

package unit.kafka.server

import kafka.server.{BrokerTopicStats, BrokerTopicPartitionAggregatedMetrics}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * We test the main logic in singleMetric_* tests and sanity check the allMetrics_* logic.
 */
class BrokerTopicPartitionAggregatedMetricsTest {
  val brokerTopicPartitionAggregatedMetrics = new BrokerTopicPartitionAggregatedMetrics("topic")

  def aPartitionAggregatedMetric(): brokerTopicPartitionAggregatedMetrics.BrokerTopicPartitionAggregatedMetric =
    new brokerTopicPartitionAggregatedMetrics.BrokerTopicPartitionAggregatedMetric("testMetric")

  @Test
  def singleMetric_defaultValueIsZero(): Unit = {
    assertEquals(0L, aPartitionAggregatedMetric().value())
  }

  @Test
  def singleMetric_partitionValuesAreCumulated(): Unit = {
    val partitionAggregatedMetric = aPartitionAggregatedMetric()
    partitionAggregatedMetric.setPartitionMetricValue(0, 3)
    partitionAggregatedMetric.setPartitionMetricValue(2, 5)

    assertEquals(8L, partitionAggregatedMetric.value())
  }

  @Test
  def singleMetric_partitionValuesCanBeUpdated(): Unit = {
    val partitionAggregatedMetric = aPartitionAggregatedMetric()
    partitionAggregatedMetric.setPartitionMetricValue(0, 3)
    partitionAggregatedMetric.setPartitionMetricValue(2, 5)

    assertEquals(8L, partitionAggregatedMetric.value())

    partitionAggregatedMetric.setPartitionMetricValue(2, 1)
    assertEquals(4L, partitionAggregatedMetric.value())

    partitionAggregatedMetric.setPartitionMetricValue(1, 1)
    assertEquals(5L, partitionAggregatedMetric.value())
  }

  @Test
  def singleMetric_partitionsCanBeRemoved(): Unit = {
    val partitionAggregatedMetric = aPartitionAggregatedMetric()
    partitionAggregatedMetric.setPartitionMetricValue(0, 3)
    partitionAggregatedMetric.setPartitionMetricValue(2, 5)

    partitionAggregatedMetric.removePartition(0)
    assertEquals(5L, partitionAggregatedMetric.value())

    partitionAggregatedMetric.removePartition(2)
    assertEquals(0L, partitionAggregatedMetric.value())

    partitionAggregatedMetric.removePartition(3)
    assertEquals(0L, partitionAggregatedMetric.value())
  }

  @Test
  def allMetrics_defaultValuesAreZero(): Unit = {
    val brokerTopicPartitionAggregatedMetrics = new BrokerTopicStats().partitionAggregatedStats("topic")

    assertEquals(0L, brokerTopicPartitionAggregatedMetrics.offsetLag())
    assertEquals(0L, brokerTopicPartitionAggregatedMetrics.bytesLag())
    assertEquals(0L, brokerTopicPartitionAggregatedMetrics.remoteLogSize())
    assertEquals(0L, brokerTopicPartitionAggregatedMetrics.remoteLogMetadataCount())
  }

  @Test
  def allMetrics_setPartitionLag(): Unit = {
    val brokerTopicPartitionAggregatedMetrics = new BrokerTopicStats().partitionAggregatedStats("topic")
    brokerTopicPartitionAggregatedMetrics.setLag(0, 3, 10)
    brokerTopicPartitionAggregatedMetrics.setLag(2, 5, 11)

    assertEquals(8L, brokerTopicPartitionAggregatedMetrics.offsetLag())
    assertEquals(21L, brokerTopicPartitionAggregatedMetrics.bytesLag())

    brokerTopicPartitionAggregatedMetrics.setLag(2, 1, 2)
    assertEquals(4L, brokerTopicPartitionAggregatedMetrics.offsetLag())
    assertEquals(12L, brokerTopicPartitionAggregatedMetrics.bytesLag())

    brokerTopicPartitionAggregatedMetrics.removePartition(2)
    assertEquals(3L, brokerTopicPartitionAggregatedMetrics.offsetLag())
    assertEquals(10L, brokerTopicPartitionAggregatedMetrics.bytesLag())

    // Should not change these stats
    assertEquals(0L, brokerTopicPartitionAggregatedMetrics.remoteLogSize())
    assertEquals(0L, brokerTopicPartitionAggregatedMetrics.remoteLogMetadataCount())
  }

  @Test
  def allMetrics_setRemoteLogAggregateData(): Unit = {
    val brokerTopicPartitionAggregatedMetrics = new BrokerTopicStats().partitionAggregatedStats("topic")
    brokerTopicPartitionAggregatedMetrics.setRemoteLogAggregateStats(0, 3, 10)
    brokerTopicPartitionAggregatedMetrics.setRemoteLogAggregateStats(2, 5, 11)

    assertEquals(8L, brokerTopicPartitionAggregatedMetrics.remoteLogSize())
    assertEquals(21L, brokerTopicPartitionAggregatedMetrics.remoteLogMetadataCount())

    brokerTopicPartitionAggregatedMetrics.setRemoteLogAggregateStats(2, 1, 2)
    assertEquals(4L, brokerTopicPartitionAggregatedMetrics.remoteLogSize())
    assertEquals(12L, brokerTopicPartitionAggregatedMetrics.remoteLogMetadataCount())

    brokerTopicPartitionAggregatedMetrics.removePartition(2)
    assertEquals(3L, brokerTopicPartitionAggregatedMetrics.remoteLogSize())
    assertEquals(10L, brokerTopicPartitionAggregatedMetrics.remoteLogMetadataCount())

    // Should not change these stats
    assertEquals(0L, brokerTopicPartitionAggregatedMetrics.offsetLag())
    assertEquals(0L, brokerTopicPartitionAggregatedMetrics.bytesLag())
  }
}
