import java.util.Properties

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

import impl.{MergerImpl, WriterImpl}
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

  trait KV extends KeyValue {
    override type V = String
    override type K = String

    def key: K
    def value: V
  }

  case class KVImpl(key: String, value: String) extends KV

  implicit val deserializer: InputDeserializer[KVImpl] = new InputDeserializer[KVImpl] {
    override def deserializer: (String, String) => KVImpl = (a, b) => KVImpl(a, b)
    override def keyDeserializer: Deserializer[String] = new StringDeserializer()
    override def valueDeserializer: Deserializer[String] = new StringDeserializer()
  }

  implicit val serializer: ViewSerializer[KVImpl] = new ViewSerializer[KVImpl] {
    override def serializer: (KVImpl) => (String, String) = kvImpl => (kvImpl.key, kvImpl.value)
    override def valueSerializer: Serializer[String] = new StringSerializer()
    override def keySerializer: Serializer[String] = new StringSerializer()
  }

  val reader1 =
    ReaderFactory.create[KVImpl](
      topic,
      readerProps("group1"))(deserializer, implicitly[ExecutionContext]).get
  val reader2 =
    ReaderFactory.create[KVImpl](
      topic,
      readerProps("group2"))(deserializer, implicitly[ExecutionContext]).get

  val inputs = Seq(reader1, reader2)

  implicit val merger = MergerImpl.merger[KVImpl]

  val views = Set(
    View[KVImpl](r => ViewRecord(r, viewTopic, 0), r => r.record),
    View[KVImpl](r => ViewRecord(r, viewTopic2, 0), r => r.record))

  implicit val writer = WriterImpl.create[KVImpl](writerProps)(serializer).get

  Await.result(Program.run(timeout)(inputs, views), Duration.Inf)
}
