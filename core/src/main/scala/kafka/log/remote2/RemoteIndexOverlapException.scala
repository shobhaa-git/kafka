package kafka.log.remote2

import org.apache.kafka.common.KafkaException

class RemoteIndexOverlapException(message: String) extends KafkaException(message) {
}
