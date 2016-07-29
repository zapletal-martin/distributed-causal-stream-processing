package interface

import java.util.Properties

import scala.concurrent.Future

import interface.KeyValue.{KeySerializer, ValueSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object Writer {

  def create[KV <: KeyValue : KeySerializer : ValueSerializer](
      properties: Properties
    ): Option[Writer[KV]] = {
    Some(
      writer(
        new KafkaProducer[KV#K, KV#V](
          properties,
          implicitly[KeySerializer[KV]],
          implicitly[ValueSerializer[KV]])))
  }

  type Writer[KV <: KeyValue] = (String, Int, KV#K, KV#V) => Future[RecordMetadata]

  def writer[KV <: KeyValue](
      producer: KafkaProducer[KV#K, KV#V]
    ): Writer[KV] = (topic, partition, key, value) =>
    Async.send(producer, new ProducerRecord[KV#K, KV#V](topic, partition, key, value))
}
