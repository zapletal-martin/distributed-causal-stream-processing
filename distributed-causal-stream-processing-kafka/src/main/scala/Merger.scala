import scala.concurrent.{ExecutionContext, Future}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

object Merger {

  final def merge[K, V](
      records: Seq[ConsumerRecords[K, V]]
    )(implicit ec: ExecutionContext
    ): Seq[ConsumerRecord[K, V]] = {
    records.flatMap(_.iterator().asScala)
  }

  /*final def merge[K, V](
      pollableReader: PollableReader[K, V],
      processor: Processor.Processor[K, V],
      writer: Writer[K, V],
      topic: String,
      partition: Int
    )(implicit ec: ExecutionContext): Future[Unit] = {

    val committableReader = pollableReader.poll()

    processor(writer, committableReader.consumerRecords, topic, partition)
      .map(_ => committableReader.commit())
  }*/
}
