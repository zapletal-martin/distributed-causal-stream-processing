package interface

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import impl.CommittableReaderImpl
import interface.Merger.Merger
import interface.Writer.Writer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}

class ProgramTest
  extends FreeSpec
  with ScalaFutures
  with Matchers {

  "When running the full distributed stream processing program" - {
    "should succeed and create views" in {

      case class KV(key: String, value: String) extends KeyValue {
        override type K = String
        override type V = String
      }

      def mockConsumerWrapper(topic: String) = new ConsumerWrapper[KV]() {
        override def poll(timeout: Long): Future[ConsumerRecords[String, String]] =
          Future.successful(
            new ConsumerRecords(
              Map(
                new TopicPartition(topic, 0) -> List(
                  new ConsumerRecord(topic, 0, 0, "key1", "value1")).asJava)
                .asJava))

        override def commit(): Future[Map[TopicPartition, OffsetAndMetadata]] =
          Future.successful(Map())
      }

      val timeout = 10L

      implicit val kvDeserializer: KVDeserializer[KV] = new KVDeserializer[KV] {
        override def deserializer: (String, String) => KV = KV
        override def keyDeserializer: Deserializer[String] = new StringDeserializer
        override def valueDeserializer: Deserializer[String] = new StringDeserializer
      }

      val readers = Seq(
        CommittableReaderImpl(mockConsumerWrapper("topic1"), Seq(KV("key0", "value0"))),
        CommittableReaderImpl(mockConsumerWrapper("topic1"), Seq(KV("key0", "value0"))))

      val views = Set(
        View[KV](
          kv => Some(ViewRecord(KV(kv.key + kv.key, kv.value + kv.value), "viewTopic1", 0)),
          _ => KV("", "")),
        View[KV](kv => Some(ViewRecord(
          KV(kv.key.toUpperCase(), kv.value.toUpperCase()),
          "viewTopic2",
          0)),
          _ => KV("", "")),
        View[KV](kv => None, _ => KV("", "")))

      implicit val merger: Merger[KV] = _.flatten
      implicit val writer: Writer[KV, (String, Int, KV)] =
        (s, i, kv) => Future.successful((s, i, kv))

      val result = Program.applyViewLogic(timeout)(readers, views).futureValue

      result shouldEqual Set(
        List(
          ("viewTopic1", 0, KV("key0key0", "value0value0")),
          ("viewTopic1", 0, KV("key0key0", "value0value0"))),
        List(
          ("viewTopic2", 0, KV("KEY0", "VALUE0")),
          ("viewTopic2", 0, KV("KEY0", "VALUE0"))),
        List())
    }
  }
}
