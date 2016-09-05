package interface.recovery

import scala.collection.JavaConverters._

import interface.{KVDeserializer, KeyValue}
import kafka.api._
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

final case class InputReader[KV <: KeyValue : KVDeserializer] private (
    // private val consumer: KafkaConsumer[KV#K, KV#V],
    // SimpleConsumer is used, because in the 0.9 KafkaConsumer
    // finding message with last committed offset for a different group
    // (using the same group seems to trigger rebalance recursively)
    // is not very convenient
    private val simpleConsumer: SimpleConsumer) {

  private def lastCommitedMessageInEachAssignedPartition(
      consumer: KafkaConsumer[KV#K, KV#V],
      timeout: Long,
      topicPartition: Seq[TopicAndPartition],
      group: String
    ): Iterable[ConsumerRecord[KV#K, KV#V]] = {

    // val offset = simpleConsumer.fetchOffsets(new OffsetFetchRequest(group, topicPartition))
    val committed =
      topicPartition.map(tp => tp -> consumer.committed(new TopicPartition(tp.topic, tp.partition)))

    /*val requestInfo = offset.requestInfo

    val response = simpleConsumer.fetch(
      new FetchRequest(requestInfo =
        requestInfo.map { case (tp, of) =>
            tp -> PartitionFetchInfo(of.offset, Int.MaxValue)
        }))

    val all = response.data
      .flatMap { case (tp, data) => data.messages.iterator.toSeq.map(tp -> _) }
      .map(SimpleConsumerDeserialization.deserialize[KV])*/

    val all = committed.map(c =>
      new ConsumerRecord[KV#K, KV#V](c._1.topic, c._1.partition, c._2.offset(), null, null))

    SimpleConsumerDeserialization.firstPerTopicPartition(all)
  }

  def findAllAfterLastCommitted(
      consumer: KafkaConsumer[KV#K, KV#V],
      timeout: Long,
      topicPartition: Seq[TopicAndPartition],
      group: String
    ): Set[ConsumerRecord[KV#K, KV#V]] = {

    val lastCommittedInEachPartition =
      lastCommitedMessageInEachAssignedPartition(
        consumer,
        timeout,
        topicPartition,
        group)

    println(s"findAllAfterLastCommitted start with $lastCommittedInEachPartition")
    findAllAfterLastCommittedInternal(
      timeout,
      topicPartition,
      group,
      lastCommittedInEachPartition).toSet
  }

  private def findAllAfterLastCommittedInternal(
      timeout: Long,
      topicPartition: Seq[TopicAndPartition],
      group: String,
      currentRecords: Iterable[ConsumerRecord[KV#K, KV#V]]
    ): Iterable[ConsumerRecord[KV#K, KV#V]] = {

    val fetch = FetchRequest(
      requestInfo = currentRecords
        .map(r =>
          TopicAndPartition(
            r.topic(), r.partition()) -> PartitionFetchInfo(r.offset(), Int.MaxValue))
        .toMap)

    println(s"Fetching $fetch")

    val fetched = simpleConsumer.fetch(fetch)

    println(s"Fetched $fetched")
    println(s"Fetched ${fetched.data.values.map(_.messages.iterator.mkString(","))}")

    val newRecords = fetched
      .data
      .toSeq
      .flatMap(tpd => tpd._2.messages.iterator.toSeq.map(tpd._1 -> _))
      .map(SimpleConsumerDeserialization.deserialize[KV])

    println(s"newRecords $newRecords")

    if (newRecords.isEmpty) {
      newRecords
    } else {
      val last = SimpleConsumerDeserialization.lastPerTopicPartition(newRecords)

      println(s"newRecords last $last")

      newRecords ++ findAllAfterLastCommittedInternal(
        timeout,
        topicPartition,
        group,
        last.map(r =>
          new ConsumerRecord(r.topic(), r.partition(), r.offset() + 1, r.key(), r.value())))
    }
  }

  // TODO: THIS HAS TO BE THE CONSUMER THAT WAS ASSIGNED THE PARTITIONS DURING THIS REBALANCE!
  def commit(
      consumer: KafkaConsumer[KV#K, KV#V],
      topicPartition: Map[TopicAndPartition, OffsetAndMetadata],
      group: String
    ): Unit = {

  consumer.commitSync(
    topicPartition.map { case(tp, om) =>
      new TopicPartition(tp.topic, tp.partition) -> new org.apache.kafka.clients.consumer.OffsetAndMetadata(om.offset, om.metadata)}.asJava)

  // simpleConsumer.commitOffsets(new OffsetCommitRequest(group, topicPartition))
  }
}