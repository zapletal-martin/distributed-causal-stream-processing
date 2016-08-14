package interface.recovery

import interface.{KVDeserializer, KeyValue}
import kafka.api.{FetchRequest, OffsetFetchRequest, PartitionFetchInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

final case class InputReader[KV <: KeyValue : KVDeserializer] private (
    private val consumer: KafkaConsumer[KV#K, KV#V],
    // SimpleConsumer is used, because in the 0.9 KafkaConsumer
    // finding message with last committed offset for a different group
    // (using the same group seems to trigger rebalance recursively)
    // is not very convenient
    private val simpleConsumer: SimpleConsumer) {

  def lastCommitedMessageInEachAssignedPartition(
      timeout: Long,
      topicPartition: Seq[TopicAndPartition],
      group: String
    ): Iterable[ConsumerRecord[KV#K, KV#V]] = {

    val offset = simpleConsumer.fetchOffsets(new OffsetFetchRequest(group, topicPartition))

    val requestInfo = offset.requestInfo

    val response = simpleConsumer.fetch(
      new FetchRequest(requestInfo =
        requestInfo.map { case (tp, of) =>
            tp -> PartitionFetchInfo(of.offset, Int.MaxValue)
        }))

    val all = response.data
      .flatMap { case (tp, data) => data.messages.iterator.toSeq.map(tp -> _) }
      .map(SimpleConsumerDeserialization.deserialize[KV])

    SimpleConsumerDeserialization.firstPerTopicPartition(all)
  }
}
