package kafka.log.remote2

import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Properties, UUID}

import kafka.log.remote.spi.RemoteLogSegmentId
import kafka.server.ReplicaManager
import kafka.utils.{KafkaScheduler, Logging, Pool}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.types.Type.{INT32, INT64, STRING}
import org.apache.kafka.common.protocol.types.{Field, Schema, Struct}

class RemoteLogMetadataManager(replicaManager: ReplicaManager) extends Logging {

  /**
    * Use {@code KafkaScheduler} for consistency with the codebase.
    *
    * TODO:
    *  - Of course the number of threads will be sized according to configuration of the remote log manager.
    *  - Additional configuration of the underlying scheduled executor service would need to be made possible.
    */
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "remote-log-manager-")

  private val isActive = new AtomicBoolean(false)

  private val remoteLogMetadataCache = new Pool[TopicPartition, RemoteLogMetadata]

  val metadataConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](new Properties)
  val metadataProducer = new KafkaProducer[Array[Byte], Array[Byte]](new Properties)

  def handlePartitionLeaderChange(topicPartition: TopicPartition) = {
    val remoteIndexesTopicPartition = new TopicPartition(
      Topic.REMOTE_LOG_METADATA_TOPIC_PREFIX + topicPartition.topic(), topicPartition.partition())

    remoteLogMetadataCache.putIfNotExists(remoteIndexesTopicPartition, assignAndReplicateFromLeader)
  }

  def onSegmentUploaded(): {

  }

  def maybeLoadMetadata(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    val key = RemoteLogMetadataManager.readRecordKey(ByteBuffer.wrap(record.key()))
    val remoteId = RemoteLogMetadataManager.readRecordValue(key.topicPartition, ByteBuffer.wrap(record.value()))
    val topicPartition = key.topicPartition

    if (remoteLogMetadataCache.contains(topicPartition)) {
      remoteLogMetadataCache.get(key.topicPartition)
        .addSegment(key.startOffset, key.endOffset, key.startTimestamp, key.endTimestamp, remoteId)
    }

  }

  def startup(): Unit = {

    //
    // consumer.assign()

}

object RemoteLogMetadataManager {

  private val CURRENT_SEGMENT_META_KEY_SCHEMA_VERSION = 0.toShort
  private val CURRENT_SEGMENT_META_VALUE_SCHEMA_VERSION = 0.toShort

  private val SEGMENT_META_KEY_SCHEMA = new Schema(
    new Field("topic", STRING),
    new Field("partition", INT32),
    new Field("startOffset", INT64),
    new Field("startTimestamp", INT64)
  )

  private val SEGMENT_META_TOPIC_FIELD = SEGMENT_META_KEY_SCHEMA.get("topic")
  private val SEGMENT_META_PARTITION_FIELD = SEGMENT_META_KEY_SCHEMA.get("partition")
  private val SEGMENT_META_START_OFFSET = SEGMENT_META_KEY_SCHEMA.get("startOffset")
  private val SEGMENT_META_START_TS = SEGMENT_META_KEY_SCHEMA.get("startTimestamp")

  private val SEGMENT_META_VALUE_SCHEMA = new Schema(new Field("id", STRING))

  private val SEGMENT_META_UUID_FIELD = SEGMENT_META_VALUE_SCHEMA.get("id")

  def readRecordKey(buffer: ByteBuffer): RemoteSegmentMetadataKey = {
    val key = SEGMENT_META_KEY_SCHEMA.read(buffer)

    val topicPartition = new TopicPartition(
      key.get(SEGMENT_META_TOPIC_FIELD).asInstanceOf[String],
      key.get(SEGMENT_META_PARTITION_FIELD).asInstanceOf[Int]
    )
    val startOffset = key.get(SEGMENT_META_START_OFFSET).asInstanceOf[Long]
    val startTimestamp = key.get(SEGMENT_META_START_TS).asInstanceOf[Long]

    RemoteSegmentMetadataKey(topicPartition, startOffset, startTimestamp)
  }

  def readRecordValue(topicPartition: TopicPartition, buffer: ByteBuffer): RemoteLogSegmentId = {
    val value = SEGMENT_META_VALUE_SCHEMA.read(buffer).asInstanceOf[String]
    val uuid = UUID.fromString(value)

    new RemoteLogSegmentId(topicPartition, uuid)
  }

  case class RemoteSegmentMetadataKey(topicPartition: TopicPartition,
                                      startOffset: Long,
                                      endOffset: Long,
                                      startTimestamp: Long,
                                      endTimestamp: Long)

}


