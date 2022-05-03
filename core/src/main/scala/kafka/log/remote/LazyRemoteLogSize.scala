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

import kafka.utils.Logging
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.utils.Time

import java.time.Duration
import scala.util.Random

private[remote] class LazyRemoteLogSize(brokerId: Int, tpId: TopicIdPartition, time: Time) extends Logging {
  this.logIdent = s"[RemoteLogManager=$brokerId partition=$tpId]"
  /**
   * We maintain a cache of the total remote log size and incrementally update it to avoid having to recompute it
   * each time
   */
  private var _remoteLogSizeOption: Option[Long] = None
  private var lastRefreshMillis: Long = -1

  def clear(): Unit = {
    this.synchronized {
      _remoteLogSizeOption = None
    }
  }

  def getOrCompute(computeRemoteLogSize: () => Long): Long = {
    this.synchronized {
      if (_remoteLogSizeOption.isEmpty || shouldRefresh()) {
        val originalRemoteLogSize = _remoteLogSizeOption

        _remoteLogSizeOption = Some(computeRemoteLogSize())
        lastRefreshMillis = time.milliseconds()

        info(s"Recomputed remote log size as ${_remoteLogSizeOption}, was previously $originalRemoteLogSize")
      }

      _remoteLogSizeOption.get
    }
  }

  /**
   * Refresh the cache at a regular interval to mitigate against unexpected divergence from the true value.
   * Such divergence should not happen, but this protects against unforeseen bugs
   */
  private def shouldRefresh(): Boolean = {
    // Use equal jitter strategy. This spreads out execution over a wide timeframe, while also helping us to bound
    // the cache refresh to e.g. between 12-24 hours
    val cacheRefreshInterval = Duration.ofHours(12).toMillis
    val equalJitter = Random.nextLong(cacheRefreshInterval)

    time.milliseconds() - lastRefreshMillis > cacheRefreshInterval + equalJitter
  }

  def add(uploadedSegmentSizeInBytes: Long): Unit = {
    this.synchronized {
      _remoteLogSizeOption = _remoteLogSizeOption.map(_ + uploadedSegmentSizeInBytes)
    }
  }

  def subtract(deletedSegmentSizeInBytes: Long): Unit = {
    this.synchronized {
      _remoteLogSizeOption = _remoteLogSizeOption match {
        case Some(currentSize) if currentSize < deletedSegmentSizeInBytes =>
          warn(s"Segment size to delete $deletedSegmentSizeInBytes is larger than current remote log size $currentSize. " +
            s"Computed size of log is wrong")
          None
        case Some(currentSize) => Some(currentSize - deletedSegmentSizeInBytes)
        case None => None
      }
    }
  }
}