import java.util.Properties

import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration.Duration

import impl.{MergerImpl, ProcessorImpl}
import kafka.common.TopicAndPartition
import interface.KeyValue.{KeyDeserializer, KeySerializer, ValueDeserializer, ValueSerializer}
import interface._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

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
  val viewTopic = TopicAndPartition("viewTopic", 0)
  val viewTopic2 = TopicAndPartition("viewTopic2", 0)

  val kv = new KeyValue {
    override type V = String
    override type K = String
  }

  implicit val keyDeserializer: KeyDeserializer[kv.type] = new StringDeserializer()
  implicit val valueDeserializer: ValueDeserializer[kv.type] = new StringDeserializer()

  implicit val keySerializer: KeySerializer[kv.type] = new StringSerializer()
  implicit val valueSerializer: ValueSerializer[kv.type] = new StringSerializer()

  implicit val merger = MergerImpl.merger[kv.type]
  implicit val processor = ProcessorImpl.processor[kv.type](
    viewTopic,
    viewTopic2)

  implicit val writer = Writer.create[kv.type](writerProps)(keySerializer, valueSerializer).get

  val reader1 =
    ReaderFactory.create[kv.type](
      topic,
      readerProps("group1"))(keyDeserializer, valueDeserializer, implicitly[ExecutionContext]).get
  val reader2 =
    ReaderFactory.create[kv.type](
      topic,
      readerProps("group2"))(keyDeserializer, valueDeserializer, implicitly[ExecutionContext]).get

  Await.result(Program.run(timeout)(Seq(reader1, reader2)), Duration.Inf)
}
