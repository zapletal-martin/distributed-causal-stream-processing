import java.util.Properties

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import impl.{MergerImpl, ReaderImpl, WriterImpl}
import interface._
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}

object Main extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  private def readerProps(group: String) = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", group)
    props.put("enable.auto.commit", "false")
    props
  }

  private def writerProps = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props
  }

  val timeout = 5000L
  val topic = "topic"
  val viewTopic = "viewTopic"
  val viewTopic2 = "viewTopic2"

  case class KVImpl(key: String, value: String) extends KeyValue {
    override type V = String
    override type K = String
  }

  implicit val deserializer: KVDeserializer[KVImpl] = new KVDeserializer[KVImpl] {
    override def deserializer: (String, String) => KVImpl = (a, b) => KVImpl(a, b)
    override def keyDeserializer: Deserializer[String] = new StringDeserializer()
    override def valueDeserializer: Deserializer[String] = new StringDeserializer()
  }

  implicit val serializer: KVSerializer[KVImpl] = new KVSerializer[KVImpl] {
    override def serializer: (KVImpl) => (String, String) = kvImpl => (kvImpl.key, kvImpl.value)
    override def valueSerializer: Serializer[String] = new StringSerializer()
    override def keySerializer: Serializer[String] = new StringSerializer()
  }

  val reader1 =
    ReaderImpl[KVImpl](
      topic,
      readerProps("group1")).get
  val reader2 =
    ReaderImpl[KVImpl](
      topic,
      readerProps("group2")).get

  val inputs = Seq(reader1, reader2)

  implicit val merger = MergerImpl.merger[KVImpl]

  val views = Set(
    View[KVImpl](r =>
      Some(
        ViewRecord(KVImpl(r.key, r.value.toUpperCase()), viewTopic, 0)), _.record),
    View[KVImpl](r =>
      Some(
        ViewRecord(KVImpl(r.key, r.value + r.value), viewTopic2, 0)), _.record))

  implicit val writer = WriterImpl[KVImpl](writerProps).get

  Await.result(Program.run(timeout)(inputs, views), Duration.Inf)
}
