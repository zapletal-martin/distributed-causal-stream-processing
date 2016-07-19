import java.util.Properties

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
    WriterFactory.create[String, String](viewTopic, props)(keySerializer, valueSerializer).get

  def program() = Merger.merge[String, String](reader, Processor.process, writer, viewTopic, 0)

  // TODO: Do we need this to be tailrec or will the call happen in separate stack anyway?
  def runProgram(): Future[Unit] =
    program().flatMap(_ => runProgram())

  Await.result(runProgram(), Duration.Inf)
}
