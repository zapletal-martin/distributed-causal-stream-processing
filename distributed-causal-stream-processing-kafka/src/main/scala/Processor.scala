import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.clients.producer.RecordMetadata

// Apply transformations
object Processor {
  type Processor[K, V] =
    (Seq[ConsumerRecord[K, V]]) => Seq[Seq[ConsumerRecord[K, V]]]

  // TODO: View allocation logic
  def process[K, V]: Processor[K, V] =
    records => {
      println(s"Processing $records")
      records.map(Seq(_))
    }
}
