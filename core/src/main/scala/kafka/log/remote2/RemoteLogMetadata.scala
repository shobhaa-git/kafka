package kafka.log.remote2

import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}

import kafka.log.remote.spi.RemoteLogSegmentKey

class RemoteLogMetadata {
  private val earliestOffset = 0
  private val earliestTimestamp = 0
  private val segmentKeysByOffset: ConcurrentNavigableMap[Long, RemoteLogSegmentKey] = new ConcurrentSkipListMap[Long, RemoteLogSegmentKey]
  private val segmentKeysByTimestamp: ConcurrentNavigableMap[Long, RemoteLogSegmentKey] = new ConcurrentSkipListMap[Long, RemoteLogSegmentKey]



}
