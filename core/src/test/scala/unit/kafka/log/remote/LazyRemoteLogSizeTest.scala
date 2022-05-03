/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import kafka.utils.MockTime
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.time.Duration

class LazyRemoteLogSizeTest {
  val topicIdPartition = new TopicIdPartition(
    Uuid.randomUuid(),
    new TopicPartition("LazyRemoteLogSizeTest", 0)
  )

  val brokerId = 1
  val time = new MockTime()

  @Test
  def getOrCompute_loadsSizeOnlyWhenRequired(): Unit = {
    var numComputes = 0
    val computeRemoteLogSizeWithCount = () => {
      numComputes += 1
      RemoteLogAggregateStats(0, 0)
    }

    val lazyRemoteLogSize = new LazyRemoteLogSize(brokerId, topicIdPartition, time)

    assertEquals(0, numComputes)

    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    lazyRemoteLogSize.clear()
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(2, numComputes)

    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(2, numComputes)
  }

  @Test
  def getOrCompute_refreshesSizeRegularly(): Unit = {
    var numComputes = 0
    val computeRemoteLogSizeWithCount = () => {
      numComputes += 1
      RemoteLogAggregateStats(0, 0)
    }

    val lazyRemoteLogSize = new LazyRemoteLogSize(brokerId, topicIdPartition, time)

    assertEquals(0, numComputes)

    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    // Does not recompute immediately
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    // Does not recompute before the specified time
    time.sleep(Duration.ofHours(12).toMillis)
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    // Recomputes after long enough
    time.sleep(Duration.ofHours(12).toMillis)
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(2, numComputes)

    time.sleep(Duration.ofHours(25).toMillis)
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(3, numComputes)
  }

  @Test
  def addAndSubtract_WhenValueIsPresent(): Unit = {
    var numComputes = 0
    val computeRemoteLogSizeWithCount = () => {
      numComputes += 1
      RemoteLogAggregateStats(0, 0)
    }

    val lazyRemoteLogSize = new LazyRemoteLogSize(brokerId, topicIdPartition, time)

    assertEquals(0, numComputes)
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    lazyRemoteLogSize.add(2)
    assertEquals(RemoteLogAggregateStats(2, 1), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    lazyRemoteLogSize.subtract(2)
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    lazyRemoteLogSize.add(5)
    assertEquals(RemoteLogAggregateStats(5, 1), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    // Subtract more than the existing size, so the original size was invalid and we recompute
    lazyRemoteLogSize.subtract(7)
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(2, numComputes)

    // Subtract more than the existing number of segments, so the original segment count was invalid and we recompute
    lazyRemoteLogSize.add(2)
    lazyRemoteLogSize.subtract(1)
    assertEquals(RemoteLogAggregateStats(1, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(2, numComputes)

    lazyRemoteLogSize.subtract(1)
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(3, numComputes)
  }

  @Test
  def addAndSubtract_WhenValueIsNotPresent(): Unit = {
    var numComputes = 0
    val computeRemoteLogSizeWithCount = () => {
      numComputes += 1
      RemoteLogAggregateStats(0, 0)
    }

    val lazyRemoteLogSize = new LazyRemoteLogSize(brokerId, topicIdPartition, time)

    assertEquals(0, numComputes)
    lazyRemoteLogSize.add(2000)
    lazyRemoteLogSize.subtract(1)
    lazyRemoteLogSize.subtract(991)
    lazyRemoteLogSize.add(200)
    assertEquals(0, numComputes)

    // Test that the value is computed
    assertEquals(RemoteLogAggregateStats(0, 0), lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)
  }
}