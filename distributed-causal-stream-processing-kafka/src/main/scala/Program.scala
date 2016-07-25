import java.util.Properties

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import _root_.Processor.{Processor, ViewRecord}
import _root_.Merger.Merger
import _root_.Writer.Writer
import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}

object Program extends App {

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "groupId")
  props.put("enable.auto.commit", "false")

  val topic = "topic"
  val viewTopic = "viewTopic"
  val viewTopic2 = "viewTopic2"

  implicit val keyDeserializer: Deserializer[String] = new StringDeserializer()
  implicit val valueDeserializer: Deserializer[String] = new StringDeserializer()

  implicit val keySerializer: Serializer[String] = new StringSerializer()
  implicit val valueSerializer: Serializer[String] = new StringSerializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  def program(
      timeout: Long,
      committable: Seq[CommittableReader[String, String]],
      merger: Merger[String, String],
      processor: Processor[String, String],
      writer: Writer[String, String],
    ): Future[Seq[PollableReader[String, String]]] = {

    val merged: Seq[ConsumerRecord[String, String]] =
      merger(committable.map(_.consumerRecords))

    val views: Seq[ViewRecord[String, String]] = processor(merged)

    val written: Future[Seq[Seq[RecordMetadata]]] =
      Future.sequence(views.map(vws =>
        Future.sequence(
          vws.records.map(r => writer(vws.topic, vws.partition, r.key(), r.value())))))

    val committed: Future[Seq[CommittableReader[String, String]]] =
      written.flatMap { com =>
        Future.sequence(committable.map(_.commit(timeout)))
      }

    committed.flatMap { r =>
      program(timeout, r, merger, processor, writer)
    }
  }

  // TODO: Do we need this to be tailrec or will the call happen in separate stack anyway?
  def runProgram(timeout: Long): Future[Seq[PollableReader[String, String]]] = {
    val reader =
      ReaderFactory.create[String, String](topic, props)(keyDeserializer, valueDeserializer).get

    program(
      timeout,
      Seq(reader.poll(timeout)),
      Merger.merger[String, String],
      Processor.processor[String, String](
        TopicAndPartition(viewTopic, 0),
        TopicAndPartition(viewTopic2, 0)),
      Writer.create[String, String](props)(keySerializer, valueSerializer).get)
  }

  Await.result(runProgram(5000L), Duration.Inf)
}
