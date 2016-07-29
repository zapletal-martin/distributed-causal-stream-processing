import java.util.Properties

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import impl.{MergerImpl, ProcessorImpl}
import kafka.common.TopicAndPartition
import interface.KeyValue.{KeyDeserializer, KeySerializer, ValueDeserializer, ValueSerializer}
import interface._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object Main extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "groupId")
  props.put("enable.auto.commit", "false")

  val timeout = 5000L
  val topic = "topic"
  val viewTopic = "viewTopic"
  val viewTopic2 = "viewTopic2"

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
    TopicAndPartition(viewTopic, 0),
    TopicAndPartition(viewTopic2, 0))

  implicit val writer = Writer.create[kv.type](props)(keySerializer, valueSerializer).get
  val reader = ReaderFactory.create[kv.type](topic, props)(keyDeserializer, valueDeserializer).get

  Await.result(Program.run(timeout)(Seq(reader)), Duration.Inf)
}
