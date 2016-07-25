import scala.concurrent.{ExecutionContext, Future}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

object Merger {

  type Merger[K, V] = Seq[ConsumerRecords[K, V]] =>  Seq[ConsumerRecord[K, V]]

  final def merger[K, V]: Merger[K, V] = records =>
    records.flatMap(_.iterator().asScala)
}
