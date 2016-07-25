import java.util.Properties

import scala.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serializer

object Writer {

  def create[K: Serializer, V: Serializer](
      properties: Properties
    ): Option[Writer[K, V]] = {
    Some(
      writer(
        new KafkaProducer(
          properties,
          implicitly[Serializer[K]],
          implicitly[Serializer[V]])))
  }

  type Writer[K, V] = (String, Int, K, V) => Future[RecordMetadata]

  def writer[K, V](producer: KafkaProducer[K, V]): Writer[K, V] = (topic, partition, key, value) => {
    println(s"Writing $key$value to $topic $partition")
    Async.send(producer, new ProducerRecord[K, V](topic, partition, key, value))
  }
}
