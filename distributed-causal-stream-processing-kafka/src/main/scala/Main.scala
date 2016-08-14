import java.util.Properties

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import impl.{MergerImpl, ReaderImpl, WriterImpl}
import interface._
import interface.recovery.{InputReader, InputRecovery, PollableViewReader, ViewRecovery}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.RecordMetadata
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

  implicit val merger = MergerImpl.merger[KVImpl]

  implicit val writer = WriterImpl[KVImpl](writerProps).get

  val view1 = View[KVImpl](
    r => Some(ViewRecord(KVImpl(r.key, r.value.toUpperCase()), viewTopic, 0)),
    r => KVImpl(r.record.key, r.record.value.toLowerCase()))

  val view2 = View[KVImpl](
    r => Some(ViewRecord(KVImpl(r.key, r.value + r.value), viewTopic2, 0)),
    r => KVImpl(r.record.key, r.record.value.drop(r.record.value.length / 2)))

  val views = Set(view1, view2)

  val viewReaderProps = readerProps("x1")
  val viewReaderProps2 = readerProps("x2")
  viewReaderProps.put("enable.auto.commit", "false")
  viewReaderProps2.put("enable.auto.commit", "false")

  val viewRecoveryConsumer = new KafkaConsumer[String, String](
    viewReaderProps,
    deserializer.keyDeserializer,
    deserializer.valueDeserializer)

  viewRecoveryConsumer.subscribe(Set(viewTopic).asJava)

  val viewRecoveryConsumer2 = new KafkaConsumer[String, String](
    viewReaderProps2,
    deserializer.keyDeserializer,
    deserializer.valueDeserializer)

  viewRecoveryConsumer2.subscribe(Set(viewTopic2).asJava)

  val viewSimpleConsumer = new SimpleConsumer("localhost", 9092, timeout.toInt, 10, "0")
  val viewSimpleConsumer2 = new SimpleConsumer("localhost", 9092, timeout.toInt, 10, "1")

  val viewRecovery = Seq(
    ViewRecovery[KVImpl, RecordMetadata](
      TopicAndPartition(viewTopic, 0),
      PollableViewReader[KVImpl](viewSimpleConsumer, viewRecoveryConsumer),
      writer,
      view1),
    ViewRecovery[KVImpl, RecordMetadata](
      TopicAndPartition(viewTopic2, 0),
      PollableViewReader[KVImpl](viewSimpleConsumer2, viewRecoveryConsumer2),
      writer,
      view2))

  val recoveryConsumer =
    new KafkaConsumer[String, String](
      readerProps("r1"),
      implicitly[KVDeserializer[KVImpl]].keyDeserializer,
      implicitly[KVDeserializer[KVImpl]].valueDeserializer)

  val recoveryConsumer2 =
    new KafkaConsumer[String, String](
      readerProps("r2"),
      implicitly[KVDeserializer[KVImpl]].keyDeserializer,
      implicitly[KVDeserializer[KVImpl]].valueDeserializer)

  recoveryConsumer.subscribe(Set(topic).asJava)
  recoveryConsumer2.subscribe(Set(topic).asJava)

  val simpleConsumer = new SimpleConsumer("localhost", 9092, timeout.toInt, 10, "0")
  val simpleConsumer2 = new SimpleConsumer("localhost", 9092, timeout.toInt, 10, "1")

  val inputRecovery = InputRecovery(
    InputReader[KVImpl](recoveryConsumer, simpleConsumer),
    Seq(new TopicAndPartition(topic, 0)),
    "group1")

  val inputRecovery2 = InputRecovery(
    InputReader[KVImpl](recoveryConsumer2, simpleConsumer2),
    Seq(new TopicAndPartition(topic, 0)),
    "group2")

  val reader1 =
    ReaderImpl[KVImpl, RecordMetadata](
      timeout,
      100L
    )(topic,
      readerProps("group1"),
      viewRecovery,
      inputRecovery).get

  val reader2 =
    ReaderImpl[KVImpl, RecordMetadata](
      timeout,
      100L
    )(topic,
      readerProps("group2"),
      viewRecovery,
      inputRecovery2).get

  val inputs = Seq(reader1) //, reader2)

  Await.result(Program.run(timeout)(inputs, views), Duration.Inf)
}
