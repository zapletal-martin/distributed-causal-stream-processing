package impl

import java.util.Properties

import interface.Writer.Writer
import interface.{Async, ViewSerializer, KeyValue}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

object WriterImpl {
  def create[KV <: KeyValue : ViewSerializer](
      properties: Properties
    ): Option[Writer[KV]] = {
    Some(
      writer(
        new KafkaProducer[KV#K, KV#V](
          properties,
          implicitly[ViewSerializer[KV]].keySerializer,
          implicitly[ViewSerializer[KV]].valueSerializer)))
  }

  def writer[KV <: KeyValue : ViewSerializer](
      producer: KafkaProducer[KV#K, KV#V]
    ): Writer[KV] = (topic, partition, keyValue) => {
    val (k, v) = implicitly[ViewSerializer[KV]].serializer(keyValue)
    Async.send(producer, new ProducerRecord[KV#K, KV#V](topic, partition, k, v))
  }
}
