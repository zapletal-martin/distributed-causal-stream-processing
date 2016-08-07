package impl

import java.util.Properties

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

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

        PollableReaderImpl(KafkaConsumerWrapper(consumer))
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

    val committed = if (records.isEmpty) {
      Future.successful(Map.empty[TopicPartition, OffsetAndMetadata])
    } else {
      println("COMMITTING")
      consumer.commit()
    }

    // This is needed. Unfortunately commitAsync request is not sent
    // asynchronously by the Kafka client, only with the next poll.
    // http://grokbase.com/t/kafka/users/1625ezxyc4/new-client-commitasync-problem
    // Unfortunately the synchronization is difficult so we may need to try .poll()
    // multiple times to ensure commit completed
    // TODO: Is it worth it? Can we continue without committing safely or
    // TODO: commit synchronously instead?
    val promise = Promise[Unit]()
    committed.onComplete(_ => promise.trySuccess(()))

    def pollUntilCommitted(): Future[ConsumerRecords[KV#K, KV#V]] = {
      consumer.poll(pollTimeout).flatMap { result =>
        if (promise.isCompleted) {
          Future.successful(result)
        } else {
          pollUntilCommitted()
        }
      }
    }

    val pollResult = pollUntilCommitted()

    committed
      .flatMap(_ =>
        pollResult.map(pr => CommittableReaderImpl[KV](consumer, deserialise(pr))))
  }
}


final case class PollableReaderImpl[KV <: KeyValue : KVDeserializer] private (
    private val consumer: ConsumerWrapper[KV])
  extends PollableReader[KV] {

  // We must not commit the offset until processed
  override def poll(
      timeout: Long
    )(implicit ec: ExecutionContext
    ): Future[CommittableReader[KV]] =
    consumer.poll(timeout).map(pr => CommittableReaderImpl(consumer, deserialise(pr)))
}
