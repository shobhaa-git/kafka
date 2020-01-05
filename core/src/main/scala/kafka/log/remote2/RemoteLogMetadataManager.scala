package kafka.log.remote2

import kafka.utils.{KafkaScheduler, Pool}
import org.apache.kafka.common.TopicPartition

class RemoteLogMetadataManager {

  /**
    * Use {@code KafkaScheduler} for consistency with the codebase.
    *
    * TODO:
    *  - Of course the number of threads will be sized according to configuration of the remote log manager.
    *  - Additional configuration of the underlying scheduled executor service would need to be made possible.
    */
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "remote-log-manager-")

  /* Sensors and gauges. */

  private val remoteLogMetadataCache = new Pool[TopicPartition, RemoteLogMetadata]

  def loadRemoteLogMetadata(): Unit = {

  }


}
