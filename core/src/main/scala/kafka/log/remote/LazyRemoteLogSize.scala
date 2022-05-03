package kafka.log.remote

import kafka.utils.Logging
import org.apache.kafka.common.TopicIdPartition

private[remote] class LazyRemoteLogSize(brokerId: Int, tpId: TopicIdPartition) extends Logging {
  this.logIdent = s"[RemoteLogManager=$brokerId partition=$tpId]"
  /**
   * We maintain a cache of the total remote log size and update it accordingly to avoid having to recompute it
   * each time
   */
  private var _remoteLogSizeOption: Option[Long] = None

  def clear(): Unit = {
    this.synchronized {
      _remoteLogSizeOption = None
    }
  }

  def getOrCompute(computeRemoteLogSize: () => Long): Long = {
    this.synchronized {
      if (_remoteLogSizeOption.isEmpty) {
        _remoteLogSizeOption = Some(computeRemoteLogSize())
      }

      _remoteLogSizeOption.get
    }
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