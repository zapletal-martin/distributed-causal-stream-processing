package interface.recovery

import scala.collection.JavaConverters._
import interface.KeyValue
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

final case class InputReader[KV <: KeyValue] private (
    private val consumer: KafkaConsumer[KV#K, KV#V]) {

  def lastCommitedMessageInEachAssignedPartition(
      timeout: Long
    ): Iterable[ConsumerRecord[KV#K, KV#V]] = {

    // Seek the last commited offset for each input partition and read event
    consumer
    .assignment()
    .asScala
    .map(pos =>
      pos -> (
        consumer
        .position(pos) - 1))
    .foreach(prevPos =>
      consumer.seek(prevPos._1, prevPos._2))

    val lastCommitted =
      consumer
        .poll(timeout)
        .asScala

    lastPerTopicPartition(lastCommitted)
  }

  // TODO: Dedup
  private def lastPerTopicPartition(
      topicPartitions: Iterable[ConsumerRecord[KV#K, KV#V]]
    ): Iterable[ConsumerRecord[KV#K, KV#V]] = {
    topicPartitions
      .groupBy(r => (r.topic(), r.partition()))
      .map(r => r._2.maxBy(_.offset()))
      .toSeq
  }
}
