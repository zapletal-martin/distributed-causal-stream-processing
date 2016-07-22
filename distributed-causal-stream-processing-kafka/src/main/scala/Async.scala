import java.util

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

import org.apache.kafka.clients.consumer.{Consumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

object Async {

  def commit[K, V](
      consumer: Consumer[K, V]
    ): Future[Map[TopicPartition, OffsetAndMetadata]] = {
    val promise = Promise[Map[TopicPartition, OffsetAndMetadata]]()

    println("Commit async")
    consumer.commitAsync(
      new OffsetCommitCallback {
        override def onComplete(
            offsets: util.Map[TopicPartition, OffsetAndMetadata],
            exception: Exception): Unit = {
          println(s"Commit $offsets $exception")
          Option(exception).fold(promise.trySuccess(offsets.asScala.toMap))(promise.tryFailure)
        }
      }
    )

    promise.future
  }

  def send[K, V](
      producer: KafkaProducer[K, V],
      record: ProducerRecord[K, V]
    ): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()

    producer.send(
      record,
      new Callback() {
        override def onCompletion(
            metadata: RecordMetadata,
            exception: Exception): Unit = {
          println(s"Send $metadata $exception")
          Option(exception).fold(promise.trySuccess(metadata))(promise.tryFailure)
        }
      }
    )

    promise.future
  }
}
