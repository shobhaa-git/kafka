package kafka.log.remote2

import collection.JavaConverters._

import java.time.Duration
import java.util.Properties

import kafka.cluster.Partition
import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

class RemoteLogMetadataConsumer(load: ConsumerRecord[Array[Byte], Array[Byte]] => Unit)
  extends ShutdownableThread("remote-log-metadata-consumer", true) {

  val metadataConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](new Properties)

  def consumePartitions(partitions: Set[Partition]): Unit = {
    metadataConsumer.assign(partitions.map(_.topicPartition).asJavaCollection)
  }

  override def doWork(): Unit = {
    metadataConsumer.poll(Duration.ofMillis(1000)).forEach(load(_))
  }

}
