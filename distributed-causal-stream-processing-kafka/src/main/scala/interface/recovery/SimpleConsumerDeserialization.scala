package interface.recovery

import interface.{KVDeserializer, KeyValue}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndOffset
import org.apache.kafka.clients.consumer.ConsumerRecord

object SimpleConsumerDeserialization {

  def deserialize[KV <: KeyValue : KVDeserializer](
      messageWithTopicAndPartition: (TopicAndPartition, MessageAndOffset)
    ): ConsumerRecord[KV#K, KV#V] = {
    val (tp, message) = messageWithTopicAndPartition
    val deserializer = implicitly[KVDeserializer[KV]]

    new ConsumerRecord[KV#K, KV#V](
      tp.topic,
      tp.partition,
      message.offset,
      Option(message.message.key)
        .map(k => deserializer.keyDeserializer.deserialize(tp.topic, k.array())).orNull,
      Option(message.message.payload)
        .map { v =>
          val array = new Array[Byte](v.limit())
          v.get(array)

          deserializer
            .valueDeserializer
            .deserialize(tp.topic, array)
        }.orNull)
  }

  def lastPerTopicPartition[KV <: KeyValue](
      topicPartitions: Iterable[ConsumerRecord[KV#K, KV#V]]
    ): Iterable[ConsumerRecord[KV#K, KV#V]] = {
      topicPartitions
        .groupBy(r => (r.topic(), r.partition()))
        .map(r => r._2.maxBy(_.offset()))
        .toSeq
    }

  def firstPerTopicPartition[KV <: KeyValue](
      topicPartitions: Iterable[ConsumerRecord[KV#K, KV#V]]
    ): Iterable[ConsumerRecord[KV#K, KV#V]] = {
    topicPartitions
      .groupBy(r => (r.topic(), r.partition()))
      .map(r => r._2.minBy(_.offset()))
      .toSeq
  }
}
