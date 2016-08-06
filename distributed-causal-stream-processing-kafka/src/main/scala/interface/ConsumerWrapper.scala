package interface

import scala.concurrent.Future

import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

trait ConsumerWrapper[KV <: KeyValue] {

  // TODO: Remove Consumer Records. Kafka specific
  def poll(timeout: Long): Future[ConsumerRecords[KV#K, KV#V]]
  def commit(): Future[Map[TopicPartition, OffsetAndMetadata]]
}
