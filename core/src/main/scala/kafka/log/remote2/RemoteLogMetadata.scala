package kafka.log.remote2

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}

import kafka.log.remote.spi.RemoteLogSegmentId
import kafka.utils.CoreUtils

class RemoteLogMetadata {

  private val lock = new ReentrantLock()

  private val segmentKeysByOffset: ConcurrentNavigableMap[Long, RemoteLogSegmentId]= new ConcurrentSkipListMap[Long, RemoteLogSegmentId]
  private val segmentKeysByTimestamp: ConcurrentNavigableMap[Long, RemoteLogSegmentId] = new ConcurrentSkipListMap[Long, RemoteLogSegmentId]

  def addSegment(startOffset: Long, endOffset: Long,
                 startTimestamp: Long, endTimestamp: Long,
                 remoteSegmentKey: RemoteLogSegmentId) = {

    CoreUtils.inLock(lock) {
      val offsetOverlap = segmentKeysByOffset.subMap(startOffset, endOffset)
      if (!offsetOverlap.isEmpty) {
        throw new RemoteIndexOverlapException(
          s"New segment offsets [$startOffset; $endOffset] overlaps" +
          s"with offsets for already existing remote segments from " +
          s"${offsetOverlap.firstKey()} to ${offsetOverlap.lastKey()}")
      }

      val timestampOverlap = segmentKeysByTimestamp.subMap(startTimestamp, endTimestamp)
      if (!timestampOverlap.isEmpty) {
        throw new RemoteIndexOverlapException(
          s"New segment timestamps [$startTimestamp; $endTimestamp] overlaps" +
          s"with offsets for already existing remote segments from " +
          s"${timestampOverlap.firstKey()} to ${timestampOverlap.lastKey()}")
      }
      
      segmentKeysByOffset.put(startOffset, remoteSegmentKey)
      segmentKeysByTimestamp.put(startTimestamp, remoteSegmentKey)
    }
  }

  def getSegmentKeyByOffset(offset: Long): Option[RemoteLogSegmentId] = {
    CoreUtils.inLock(lock) {
      Option(segmentKeysByOffset.ceilingEntry(offset)).map(_.getValue)
    }
  }

  def getSegmentKeyByTimestamp(timestamp: Long): Option[RemoteLogSegmentId] = {
    CoreUtils.inLock(lock) {
      Option(segmentKeysByTimestamp.ceilingEntry(timestamp)).map(_.getValue)
    }
  }



}
