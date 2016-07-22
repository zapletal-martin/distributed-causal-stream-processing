import java.util.Properties

import scala.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serializer

object WriterFactory {

  def create[K: Serializer, V: Serializer](
      properties: Properties
    ): Option[Writer[K, V]] = {
    Some(
      Writer(
        new KafkaProducer(
          properties,
          implicitly[Serializer[K]],
          implicitly[Serializer[V]])))
  }
}

case class Writer[K, V](
    private val producer: KafkaProducer[K, V]) {

  def write(topic: String, partition: Int, key: K, value: V): Future[RecordMetadata] =
    Async.send(producer, new ProducerRecord[K, V](topic, partition, key, value))
}