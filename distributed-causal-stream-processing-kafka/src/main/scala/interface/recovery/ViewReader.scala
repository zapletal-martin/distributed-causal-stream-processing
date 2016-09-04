package interface.recovery

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import interface.recovery.ExactlyOnceDeliveryRecovery.Partitioner
import interface.{KVDeserializer, KeyValue, View, ViewRecord}
import kafka.api.{Request, OffsetRequest, PartitionFetchInfo, FetchRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

trait ViewReaderBehaviour[KV <: KeyValue] {
  protected def simpleConsumer: SimpleConsumer

  protected def consumer: KafkaConsumer[KV#K, KV#V]
  protected def deserializer: KVDeserializer[KV]

  private[this] implicit def deserializerImplicit = deserializer

  protected def poll(
      timeout: Long,
      topicAndPartition: TopicAndPartition,
      offset: Long
    ): ViewReader[KV] = {

    val response = simpleConsumer
      .fetch(
        new FetchRequest(requestInfo =
          Map(topicAndPartition -> PartitionFetchInfo(offset, Int.MaxValue))))

    println(s"View poll $topicAndPartition -> $offset")
    val all = response
      .data
      .flatMap { case (tp, data) => data.messages.iterator.toSeq.map(tp -> _) }
      .map(SimpleConsumerDeserialization.deserialize[KV])
      .toSeq

    println(s"Got $all")

    ViewReader(simpleConsumer, consumer, all)
  }

  private def viewProgressOffests(
      viewTopicAndPartition: TopicAndPartition
    ): Set[(TopicPartition, Long)] = {

    // This does not seem to work
    // val assignment = consumer.assignment()
    // println(s"assignment $assignment")

    println(s"View progress seeker $viewTopicAndPartition")

    val latest = simpleConsumer.earliestOrLatestOffset(
      viewTopicAndPartition,
      OffsetRequest.LatestTime,
      Request.OrdinaryConsumerId)

    val tp = new TopicPartition(viewTopicAndPartition.topic, viewTopicAndPartition.partition)

    val a = Set(tp -> (latest - 1))
    println(s"View latest offset $a")
    a
  }

  def findLastInViewFromPartition(
      timeout: Long,
      originalTopicAndPartition: TopicPartition,
      partitioner: Partitioner[KV],
      view: View[KV],
      viewTopicAndPartition: TopicAndPartition
    ): Option[Set[ConsumerRecord[KV#K, KV#V]]] = {

    val progress = viewProgressOffests(viewTopicAndPartition)

    findLastInViewFromPartitionInternal(
      timeout,
      originalTopicAndPartition,
      partitioner,
      view,
      progress)
  }

  @tailrec
  private def findLastInViewFromPartitionInternal(
      timeout: Long,
      originalTopicAndPartition: TopicPartition,
      partitioner: Partitioner[KV],
      view: View[KV],
      progress: Set[(TopicPartition, Long)]
    ): Option[Set[ConsumerRecord[KV#K, KV#V]]] = {

    val newReaders =
      progress.map(tp =>
        poll(timeout, TopicAndPartition(tp._1.topic(), tp._1.partition()), tp._2))

    println(s"Loop gotten $newReaders")

    val results: Set[Option[ConsumerRecord[KV#K, KV#V]]] =
      newReaders.map(_.consumerRecords.headOption)

    val found: Set[ConsumerRecord[KV#K, KV#V]] = results
      .flatMap(_.fold(Set[ConsumerRecord[KV#K, KV#V]]())(x => Set(x)))
      .filter { event =>
        val deserialized =
          implicitly[KVDeserializer[KV]].deserializer(event.key(), event.value())

        val originalRecord =
          view.inverseTransformation(
            ViewRecord[KV](deserialized, event.topic(), event.partition()))

        partitioner(originalRecord) == originalTopicAndPartition
      }

    val foundLast = SimpleConsumerDeserialization.lastPerTopicPartition(found)

    // TODO: What if reaches "start" not finding anything?
    // TODO: How do we find "start"?
    if (foundLast.nonEmpty) {
      println(s"FOUND $foundLast")
      Some(found)
    } else if (results.isEmpty || !results.exists(_.isDefined)) {
      None
    } else {
      // Try previous
      findLastInViewFromPartitionInternal(
        timeout,
        originalTopicAndPartition,
        partitioner,
        view,
        progress.map(p => p.copy(_2 = p._2 - 1)))
    }
  }
}

final case class PollableViewReader[KV <: KeyValue : KVDeserializer] private(
    override val simpleConsumer: SimpleConsumer,
    override val consumer: KafkaConsumer[KV#K, KV#V])
  extends ViewReaderBehaviour[KV] {
  override protected val deserializer: KVDeserializer[KV] = implicitly[KVDeserializer[KV]]
}

final case class ViewReader[KV <: KeyValue : KVDeserializer] private(
    override val simpleConsumer: SimpleConsumer,
    override val consumer: KafkaConsumer[KV#K, KV#V],
    consumerRecords: Seq[ConsumerRecord[KV#K, KV#V]])
  extends ViewReaderBehaviour[KV] {
  override protected val deserializer: KVDeserializer[KV] = implicitly[KVDeserializer[KV]]
}
