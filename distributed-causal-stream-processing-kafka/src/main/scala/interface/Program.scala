package interface

import scala.concurrent.{ExecutionContext, Future}

import interface.Merger._
import interface.Processor._
import interface.Writer._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata

object Program {

  def run[KV <: KeyValue : Merger : Processor : Writer](
      timeout: Long
    )(readers: Seq[PollableReader[KV]]
    )(implicit ec: ExecutionContext
    ): Future[Seq[PollableReader[KV]]] = {

    runInternal(timeout)(readers.map(_.poll(timeout)))
  }

  private def runInternal[KV <: KeyValue : Merger : Processor : Writer](
      timeout: Long
    )(readers: Seq[CommittableReader[KV]]
    )(implicit ec: ExecutionContext
    ): Future[Seq[PollableReader[KV]]] = {

    val merger = implicitly[Merger[KV]]
    val processor = implicitly[Processor[KV]]
    val writer = implicitly[Writer[KV]]

    val merged: Seq[ConsumerRecord[KV#K, KV#V]] =
      merger(readers.map(_.consumerRecords))

    val views: Seq[ViewRecord[KV]] = processor(merged)

    val written: Future[Seq[Seq[RecordMetadata]]] =
      Future.sequence(views.map(vws =>
        Future.sequence(
          vws.records.map(r => writer(vws.topic, vws.partition, r.key(), r.value())))))

    val committed: Future[Seq[CommittableReader[KV]]] =
      written.flatMap { com =>
        Future.sequence(readers.map(_.commit(timeout)))
      }

    committed.flatMap { r =>
      runInternal(timeout)(r)
    }
  }
}
