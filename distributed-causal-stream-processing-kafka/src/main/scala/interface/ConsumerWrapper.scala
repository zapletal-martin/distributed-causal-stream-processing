package interface

import scala.concurrent.Future

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

// trait KafkaConsumerWrapper[KV <: KeyValue]

final case class ConsumerWrapper[KV <: KeyValue](
    private val consumer: KafkaConsumer[KV#K, KV#V]) {

  // TODO: Try[_]
  def poll(timeout: Long): ConsumerRecords[KV#K, KV#V] = consumer.poll(timeout)

  def commit(): Future[Map[TopicPartition, OffsetAndMetadata]] = Async.commit(consumer)
}
