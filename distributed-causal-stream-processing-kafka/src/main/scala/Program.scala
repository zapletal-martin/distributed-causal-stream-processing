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

  def program(committable: Seq[Future[CommittableReader[String, String]]]) = {
    val merged: Future[Seq[ConsumerRecord[String, String]]] = Future.sequence(committable)
      .map(c => Merger.merge[String, String](c.map(_.consumerRecords)))

    val processor = Processor.process[String, String]
    val views: Future[Seq[Seq[ConsumerRecord[String, String]]]] = merged.map(processor)
    val written: Future[Seq[Seq[RecordMetadata]]] =
      views.flatMap(f =>
        Future.sequence(f.map(vws =>
          Future.sequence(
            vws.map(r => writer.write(viewTopic, 0, r.key(), r.value()))))))

    val a: String = written.map(_ => committable.map(_.map(_.commit())))

    program()
  }

  // TODO: Do we need this to be tailrec or will the call happen in separate stack anyway?
  def runProgram(): Future[Unit] = {
    val committable = Seq(
      Future(reader.poll()),
      Future(reader.poll()))

    program(committable).flatMap(_ => runProgram())
  }

  Await.result(runProgram(), Duration.Inf)
}
