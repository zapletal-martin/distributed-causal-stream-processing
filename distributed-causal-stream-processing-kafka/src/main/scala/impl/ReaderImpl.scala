package impl

import java.util.Properties

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import impl.ReaderImpl._
import interface.Reader.{CommittableReader, PollableReader}
import interface._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

object ReaderImpl {

  def apply[KV <: KeyValue : KVDeserializer](
    topic: String,
    properties: Properties
  )(implicit ec: ExecutionContext
  ): Option[PollableReader[KV]] = {

    Option(properties.getProperty("enable.auto.commit"))
      .fold[Option[Properties]] {
      properties.put("enable.auto.commit", "false")
      Some(properties) } {
        case "true" =>
          None
        case _ =>
          Some(properties)
      }
      .map { p =>
        val consumer =
          new KafkaConsumer[KV#K, KV#V](
            p,
            implicitly[KVDeserializer[KV]].keyDeserializer,
            implicitly[KVDeserializer[KV]].valueDeserializer)

        consumer.subscribe(
          Set(topic).asJava)
        // TODO: Use persistenceId partitioner
        /*new ExactlyOnceDeliveryConsumerRebalanceListener(
          ExactlyOnceDeliveryRecovery[KV](
            1000L,
            (_, _) => new TopicPartition("", 0),
            Seq(),
            InputRecovery(null))))*/

        PollableReaderImpl(ConsumerWrapper(consumer))
      }
  }

  def deserialise[KV <: KeyValue : KVDeserializer](
      records: ConsumerRecords[KV#K, KV#V]
    ): Seq[KV] =
    records
      .asScala
      .toSeq
      .map(r => implicitly[KVDeserializer[KV]].deserializer(r.key(), r.value()))
}

final case class CommittableReaderImpl[KV <: KeyValue : KVDeserializer] private (
    private val consumer: ConsumerWrapper[KV],
    override val records: Seq[KV])
  extends CommittableReader[KV] {

  override def commit(
    pollTimeout: Long
  )(implicit ec: ExecutionContext
  ): Future[CommittableReader[KV]] = {

    val future = if (records.isEmpty) {
      Future.successful(Map.empty[TopicPartition, OffsetAndMetadata])
    } else {
      consumer.commit()
    }

    // This is needed. Unfortunately commitAsync request is not sent
    // asynchronously by the Kafka client, only with the next poll.
    // http://grokbase.com/t/kafka/users/1625ezxyc4/new-client-commitasync-problem
    future.map(_ => CommittableReaderImpl[KV](consumer, deserialise(consumer.poll(pollTimeout))))
  }
}


final case class PollableReaderImpl[KV <: KeyValue : KVDeserializer] private (
    private val consumer: ConsumerWrapper[KV])
  extends PollableReader[KV] {

  // We must not commit the offset until processed
  def poll(timeout: Long): CommittableReader[KV] =
    CommittableReaderImpl(consumer, deserialise(consumer.poll(timeout)))
}
