/*
package interface.recovery

import scala.collection.JavaConverters._

import interface.KeyValue
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition


final case class ViewReader[KV <: KeyValue] private (
    private val consumer: KafkaConsumer[KV#K, KV#V],
  consumerRecords: ConsumerRecords[KV#K, KV#V]) {

  private def poll(
      timeout: Long,
      topicAndPartition: TopicPartition,
      offset: Long
    ): ViewReader[KV] = {

    consumer.seek(
      topicAndPartition,
      offset)

    ViewReader(consumer, consumer.poll(timeout))
  }

  def viewProgressOffests(): Set[(TopicPartition, Long)] = {
    val assignment = consumer.assignment()
    val _ = consumer.seekToEnd(assignment)
    assignment.asScala.toSet[TopicPartition].map(a => a -> consumer.position(a))
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

  def findLastInViewFromPartition(
      timeout: Long,
      originalTopicAndPartition: TopicPartition,
      inversePartitioner: (KV#K, KV#V) => TopicPartition
    ): Option[Set[ConsumerRecord[KV#K, KV#V]]] = {

    val progress = viewProgressOffests()

    findLastInViewFromPartitionInternal(
      timeout,
      originalTopicAndPartition,
      inversePartitioner,
      progress)
  }

  def findLastInViewFromPartitionInternal(
      timeout: Long,
      originalTopicAndPartition: TopicPartition,
      inversePartitioner: (KV#K, KV#V) => TopicPartition,
      progress: Set[(TopicPartition, Long)]
    ): Option[Set[ConsumerRecord[KV#K, KV#V]]] = {

    val newReaders =
      progress.map(tp =>
        poll(timeout, tp._1, tp._2))

    val results: Set[ConsumerRecord[KV#K, KV#V]] =
      newReaders.map(_.consumerRecords.asScala.head)

    val found: Set[ConsumerRecord[KV#K, KV#V]] = results
      .filter(event =>
        inversePartitioner(event.key(), event.value()) == originalTopicAndPartition)

    val foundLast = lastPerTopicPartition(found)

    // TODO: What if reaches "start" not finding anything?
    // TODO: How do we find "start"?
    if (foundLast.nonEmpty) {
      Some(found)
    } else {
      // Try previous
      findLastInViewFromPartitionInternal(
        timeout,
        originalTopicAndPartition,
        inversePartitioner,
        progress.map(p => p.copy(_2 = p._2 - 1)))
    }
  }
}
*/
