package impl

import java.util.Properties

import interface.Writer.Writer
import interface.{Async, KVSerializer, KeyValue}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

object WriterImpl {
  def create[KV <: KeyValue : KVSerializer](
      properties: Properties
    ): Option[Writer[KV]] = {
    Some(
      writer(
        new KafkaProducer[KV#K, KV#V](
          properties,
          implicitly[KVSerializer[KV]].keySerializer,
          implicitly[KVSerializer[KV]].valueSerializer)))
  }

  def writer[KV <: KeyValue : KVSerializer](
      producer: KafkaProducer[KV#K, KV#V]
    ): Writer[KV] = (topic, partition, keyValue) => {
    val (k, v) = implicitly[KVSerializer[KV]].serializer(keyValue)
    Async.send(producer, new ProducerRecord[KV#K, KV#V](topic, partition, k, v))
  }
}
