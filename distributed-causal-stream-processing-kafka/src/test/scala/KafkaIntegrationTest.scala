import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.scalatest.{BeforeAndAfterAll, FreeSpec}

class KafkaIntegrationTest
  extends FreeSpec
  with BeforeAndAfterAll {

  override protected def beforeAll() = {
    // create topics
    // publish events

    val properties = new Properties()
    properties.put("bootstrap.servers", "127.0.0.1:9092")
    properties.put("group.id", "properties")
    properties.put("enable.auto.commit", "false")
    properties.put("key.serializer",
      classOf[org.apache.kafka.common.serialization.StringSerializer])
    properties.put("value.serializer",
      classOf[org.apache.kafka.common.serialization.StringSerializer])

    val producer = new KafkaProducer[String, String](properties)

    storePartition(producer, 0, 100, 30, "tag1")
    storePartition(producer, 1, 100, 30, "tag1")
  }

  private def storePartition(
      producer: Producer[String, String],
      partition: Int,
      size: Int,
      tagEachN: Int,
      tag: String) = {

    var i = 0
    while (i < size) {
      val tag = if (i % tagEachN == 0) ";tag" else ""
      storeEvent(producer, partition, i.toString, s"data-$partition-$i$tag")
      i = i + 1
    }
  }

  private def storeEvent(
      producer: Producer[String, String],
      partition: Int,
      key: String,
      value: String) =
    producer.send(new ProducerRecord[String, String]("topic", 0, key, value))

  "Distributed causal stream processing using Kafka" - {
    "Should merge all events into a partially causally ordered stream" in {

    }
  }
}
