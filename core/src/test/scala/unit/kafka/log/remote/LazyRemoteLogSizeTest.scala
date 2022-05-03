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

import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class LazyRemoteLogSizeTest {
  val topicIdPartition = new TopicIdPartition(
    Uuid.randomUuid(),
    new TopicPartition("LazyRemoteLogSizeTest", 0)
  )

  val brokerId = 1

  @Test
  def loadsSizeOnlyWhenRequired(): Unit = {
    var numComputes = 0
    val computeRemoteLogSizeWithCount = () => {
      numComputes += 1
      0L
    }

    val lazyRemoteLogSize = new LazyRemoteLogSize(brokerId, topicIdPartition)

    assertEquals(0, numComputes)

    assertEquals(0, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    assertEquals(0, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(0, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    lazyRemoteLogSize.clear()
    assertEquals(0, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(2, numComputes)

    assertEquals(0, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(0, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(2, numComputes)
  }

  @Test
  def addAndSubtract_WhenValueIsPresent(): Unit = {
    var numComputes = 0
    val computeRemoteLogSizeWithCount = () => {
      numComputes += 1
      0L
    }

    val lazyRemoteLogSize = new LazyRemoteLogSize(brokerId, topicIdPartition)

    assertEquals(0, numComputes)
    assertEquals(0, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    lazyRemoteLogSize.add(2)
    assertEquals(2, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    lazyRemoteLogSize.subtract(1)
    assertEquals(1, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    lazyRemoteLogSize.add(4)
    assertEquals(5, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)

    lazyRemoteLogSize.subtract(7)
    // This will recompute since we subtracted more than the existing, so the original size was invalid
    assertEquals(0, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(2, numComputes)
  }

  @Test
  def addAndSubtract_WhenValueIsNotPresent(): Unit = {
    var numComputes = 0
    val computeRemoteLogSizeWithCount = () => {
      numComputes += 1
      0L
    }

    val lazyRemoteLogSize = new LazyRemoteLogSize(brokerId, topicIdPartition)

    assertEquals(0, numComputes)
    lazyRemoteLogSize.add(2000)
    lazyRemoteLogSize.subtract(1)
    lazyRemoteLogSize.subtract(991)
    lazyRemoteLogSize.add(200)
    assertEquals(0, numComputes)

    // Test that the value is computed
    assertEquals(0, lazyRemoteLogSize.getOrCompute(computeRemoteLogSizeWithCount))
    assertEquals(1, numComputes)
  }
}