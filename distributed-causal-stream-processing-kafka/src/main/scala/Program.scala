import java.util.Properties

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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

    println("program!")

    val merged: Seq[ConsumerRecord[String, String]] =
      Merger.merge[String, String](committable.map(_.consumerRecords))

    val processor = Processor.process[String, String]
    val views: Seq[Seq[ConsumerRecord[String, String]]] = processor(merged)

    val written: Future[Seq[Seq[RecordMetadata]]] =
      Future.sequence(views.map(vws =>
        Future.sequence(
          vws.map(r => writer.write(viewTopic, 0, r.key(), r.value())))))

    val committed: Future[Seq[PollableReader[String, String]]] =
      written.flatMap { com => println(s"Committed $com"); Future.sequence(committable.map(_.commit()))}

    committed.flatMap { r => println("Running program!"); program(timeout, r.map(_.poll(timeout))) }
  }

  // TODO: Do we need this to be tailrec or will the call happen in separate stack anyway?
  def runProgram(timeout: Long): Future[Seq[PollableReader[String, String]]] = {
    val committable = Seq(
      reader.poll(timeout),
      reader.poll(timeout))

    program(timeout, committable)
  }

  Await.result(runProgram(1000L), Duration.Inf)
}
