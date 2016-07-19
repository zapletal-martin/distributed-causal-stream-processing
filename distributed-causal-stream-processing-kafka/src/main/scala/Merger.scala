import scala.concurrent.{ExecutionContext, Future}

object Merger {

  final def merge[K, V](
      pollableReader: PollableReader[K, V],
      processor: Processor.Processor[K, V],
      writer: Writer[K, V],
      topic: String,
      partition: Int
    )(implicit ec: ExecutionContext): Future[Unit] = {

    val committableReader = pollableReader.poll()

    processor(writer, committableReader.consumerRecords, topic, partition)
      .map(_ => committableReader.commit())
  }
}
