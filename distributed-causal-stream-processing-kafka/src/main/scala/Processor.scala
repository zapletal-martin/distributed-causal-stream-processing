import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.RecordMetadata

// Apply transformations
object Processor {
  type Processor[K, V] =
    (Writer[K, V], ConsumerRecords[K, V], String, Int) =>
      Future[Seq[RecordMetadata]]

  // TODO: View allocation logic
  // TODO: Pass ec in the function call?
  def process[K, V](implicit ec: ExecutionContext): Processor[K, V] =
    (writer, records, topic, partition) =>
      Future.sequence(
        records.iterator().asScala.toSeq
          .map { rec =>
            writer.write(topic, partition, rec.key(), rec.value())
          }
      )
}
