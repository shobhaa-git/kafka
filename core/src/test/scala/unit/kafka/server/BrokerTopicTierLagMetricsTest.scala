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

import kafka.server.BrokerTopicStats
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BrokerTopicTierLagMetricsTest {
  @Test
  def defaultTierLagIsZero(): Unit = {
    assertEquals(0L, new BrokerTopicStats().tierLagStats("topic").offsetLag())
    assertEquals(0L, new BrokerTopicStats().tierLagStats("topic").bytesLag())
  }

  @Test
  def partitionLagsAreCumulated(): Unit = {
    val tierLagStats = new BrokerTopicStats().tierLagStats("topic")
    tierLagStats.setLag(0, 3, 10)
    tierLagStats.setLag(2, 5, 11)

    assertEquals(8L, tierLagStats.offsetLag())
    assertEquals(21L, tierLagStats.bytesLag())
  }

  @Test
  def partitionLagsCanBeUpdated(): Unit = {
    val tierLagStats = new BrokerTopicStats().tierLagStats("topic")
    tierLagStats.setLag(0, 3, 10)
    tierLagStats.setLag(2, 5, 11)

    assertEquals(8L, tierLagStats.offsetLag())
    assertEquals(21L, tierLagStats.bytesLag())

    tierLagStats.setLag(2, 1, 2)
    assertEquals(4L, tierLagStats.offsetLag())
    assertEquals(12L, tierLagStats.bytesLag())


    tierLagStats.setLag(1, 1, 1)
    assertEquals(5L, tierLagStats.offsetLag())
    assertEquals(13L, tierLagStats.bytesLag())
  }

  @Test
  def partitionsCanBeRemovedFromLag(): Unit = {
    val tierLagStats = new BrokerTopicStats().tierLagStats("topic")
    tierLagStats.setLag(0, 3, 10)
    tierLagStats.setLag(2, 5, 11)

    tierLagStats.removePartition(0)
    assertEquals(5L, tierLagStats.offsetLag())
    assertEquals(11L, tierLagStats.bytesLag())

    tierLagStats.removePartition(2)
    assertEquals(0L, tierLagStats.offsetLag())
    assertEquals(0L, tierLagStats.bytesLag())

    tierLagStats.removePartition(3)
    assertEquals(0L, tierLagStats.offsetLag())
    assertEquals(0L, tierLagStats.bytesLag())
  }
}
