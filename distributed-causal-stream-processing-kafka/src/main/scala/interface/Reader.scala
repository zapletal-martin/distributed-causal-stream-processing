package interface

import java.util.Properties

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import interface.KeyValue.{KeyDeserializer, ValueDeserializer}
import interface.recovery.{ExactlyOnceDeliveryConsumerRebalanceListener, ExactlyOnceDeliveryRecovery, InputRecovery}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

object ReaderFactory {

  def create[KV <: KeyValue : KeyDeserializer : ValueDeserializer](
      topic: String,
      properties: Properties
    )(implicit ec: ExecutionContext
    ): Option[PollableReader[KV]] = {

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
          new KafkaConsumer[KV#K, KV#V](
            p,
            implicitly[KeyDeserializer[KV]],
            implicitly[ValueDeserializer[KV]])

        consumer.subscribe(
          Set(topic).asJava,
          // TODO: Use persistenceId partitioner
          new ExactlyOnceDeliveryConsumerRebalanceListener(
            ExactlyOnceDeliveryRecovery[KV](
              1000L,
              (_, _) => new TopicPartition("", 0),
              Seq(),
              InputRecovery(null))))

        PollableReader(consumer)
      }
  }
}

sealed trait Reader

final case class CommittableReader[KV <: KeyValue] private (
    private val consumer: KafkaConsumer[KV#K, KV#V],
    consumerRecords: ConsumerRecords[KV#K, KV#V])
  extends Reader {

  def commit(
      pollTimeout: Long
    )(implicit ec: ExecutionContext
    ): Future[CommittableReader[KV]] = {

    val future = if (consumerRecords.isEmpty) {
      Future.successful(Map.empty[TopicPartition, OffsetAndMetadata])
    } else {
      Async.commit(consumer)
    }

    // This is needed. Unfortunately commitAsync request is not sent
    // asynchronously by the Kafka client, only with the next poll.
    // http://grokbase.com/t/kafka/users/1625ezxyc4/new-client-commitasync-problem
    val poll = consumer.poll(pollTimeout)
    future.map(_ => CommittableReader(consumer, poll))
  }
}


final case class PollableReader[KV <: KeyValue] private (
    private val consumer: KafkaConsumer[KV#K, KV#V])
  extends Reader {

  // We must not commit the offset until processed
  def poll(timeout: Long): CommittableReader[KV] = {
    CommittableReader(consumer, consumer.poll(timeout))
  }
}
