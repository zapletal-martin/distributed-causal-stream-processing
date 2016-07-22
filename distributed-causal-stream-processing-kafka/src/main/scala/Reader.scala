import java.util.Properties

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.Deserializer

object ReaderFactory {

  def create[K: Deserializer, V: Deserializer](
      topic: String,
      properties: Properties
    ): Option[PollableReader[K, V]] = {

    Option(properties.getProperty("enable.auto.commit"))
      .fold[Option[Properties]] {
        properties.put("enable.auto.commit", "false")
        Some(properties)
      } {
        case "true" =>
          None
        case _ =>
          Some(properties)
      }
      .map { p =>
        val consumer =
          new KafkaConsumer[K, V](p, implicitly[Deserializer[K]], implicitly[Deserializer[V]])

        consumer.subscribe(
          Set(topic).asJava,
          // TODO: Use persistenceId partitioner
          new ExactlyOnceDeliveryConsumerRebalanceListener(ExactlyOnceDeliveryRecovery(_ => 0)))

        PollableReader(consumer)
      }
  }
}

sealed trait Reader

final case class CommittableReader[K, V] private (
    private val consumer: KafkaConsumer[K, V],
    consumerRecords: ConsumerRecords[K, V])
  extends Reader {

  def commit()(implicit ec: ExecutionContext): Future[PollableReader[K, V]] =
    Async.  commit(consumer).map(_ => PollableReader(consumer))
}

final case class PollableReader[K, V] private (
    private val consumer: KafkaConsumer[K, V])
  extends Reader {

  // We must not commit the offset until processed
  def poll(timeout: Long): CommittableReader[K, V] = {
    CommittableReader(consumer, consumer.poll(timeout))
  }
}
