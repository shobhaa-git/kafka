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

/**
 * This class gives us aggregate information about the remote log. We avoid calling it metadata since the name is
 * heavily overloaded
 */
private[remote] case class RemoteLogAggregateStats(remoteLogSizeBytes: Long, numMetadataSegments: Long) {
  def add(addedSegmentBytes: Long): RemoteLogAggregateStats = add(RemoteLogAggregateStats(addedSegmentBytes, 1))

  def add(other: RemoteLogAggregateStats): RemoteLogAggregateStats =
    RemoteLogAggregateStats(this.remoteLogSizeBytes + other.remoteLogSizeBytes,
      this.numMetadataSegments + other.numMetadataSegments)

  def subtract(removedSegmentBytes: Long): Option[RemoteLogAggregateStats] = {
    if (remoteLogSizeBytes < removedSegmentBytes || numMetadataSegments < 1) {
      None
    } else Some(RemoteLogAggregateStats(remoteLogSizeBytes - removedSegmentBytes, numMetadataSegments - 1))
  }
}

private[remote] class RemoteLogAggregateStatsCache(brokerId: Int, tpId: TopicIdPartition, time: Time) extends Logging {
  this.logIdent = s"[RemoteLogManager=$brokerId partition=$tpId]"
  /**
   * We maintain a cache of the remote log aggregate stats and incrementally update it to avoid having to recompute it
   * each time
   */
  private var _remoteLogAggregateStats: Option[RemoteLogAggregateStats] = None
  private var lastRefreshMillis: Long = -1

  def clear(): Unit = {
    this.synchronized {
      _remoteLogAggregateStats = None
    }
  }

  def getOrCompute(computeRemoteLogAggregateStats: () => RemoteLogAggregateStats): RemoteLogAggregateStats = {
    this.synchronized {
      if (_remoteLogAggregateStats.isEmpty || shouldRefresh()) {
        val originalRemoteLogAggStats = _remoteLogAggregateStats

        _remoteLogAggregateStats = Some(computeRemoteLogAggregateStats())
        lastRefreshMillis = time.milliseconds()

        info(s"Recomputed remote log stats as ${_remoteLogAggregateStats}, was previously $originalRemoteLogAggStats")
      }

      _remoteLogAggregateStats.get
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
      _remoteLogAggregateStats = _remoteLogAggregateStats.map(_.add(uploadedSegmentSizeInBytes))
    }
  }

  def subtract(deletedSegmentSizeInBytes: Long): Unit = {
    this.synchronized {
      _remoteLogAggregateStats = _remoteLogAggregateStats.flatMap(_.subtract(deletedSegmentSizeInBytes))
    }
  }
}