import java.util.Properties

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import Processor.ViewRecord
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

  val reader =
    ReaderFactory.create[String, String](topic, props)(keyDeserializer, valueDeserializer).get

  val writer =
    WriterFactory.create[String, String](props)(keySerializer, valueSerializer).get

  def program(
      timeout: Long,
      committable: Seq[CommittableReader[String, String]]
    ): Future[Seq[PollableReader[String, String]]] = {

    val merged: Seq[ConsumerRecord[String, String]] =
      Merger.merge[String, String](committable.map(_.consumerRecords))

    val processor = Processor.process[String, String]
    val views: Seq[ViewRecord[String, String]] = processor(merged)

    val written: Future[Seq[Seq[RecordMetadata]]] =
      Future.sequence(views.map(vws =>
        Future.sequence(
          vws.records.map(r => writer.write(vws.topic, vws.partition, r.key(), r.value())))))

    val committed: Future[Seq[CommittableReader[String, String]]] =
      written.flatMap { com =>
        Future.sequence(committable.map(_.commit(timeout)))
      }

    committed.flatMap { r =>
      program(timeout, r)
    }
  }

  // http://grokbase.com/t/kafka/users/1625ezxyc4/new-client-commitasync-problem
  // TODO: Do we need this to be tailrec or will the call happen in separate stack anyway?
  def runProgram(timeout: Long): Future[Seq[PollableReader[String, String]]] =
    program(timeout, Seq(reader.poll(timeout)))

  Await.result(runProgram(5000L), Duration.Inf)
}
