package impl

import scala.concurrent.Future

import interface.{ConsumerWrapper, KeyValue, Async}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, ConsumerRecords}
import org.apache.kafka.common.TopicPartition

final case class KafkaConsumerWrapper[KV <: KeyValue](
    private val consumer: KafkaConsumer[KV#K, KV#V])
  extends ConsumerWrapper[KV] {

  def poll(timeout: Long): Future[ConsumerRecords[KV#K, KV#V]] =
    Future.successful(consumer.poll(timeout))

  def commit(): Future[Map[TopicPartition, OffsetAndMetadata]] = Async.commit(consumer)
}
