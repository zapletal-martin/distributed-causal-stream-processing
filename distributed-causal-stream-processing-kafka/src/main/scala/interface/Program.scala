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

    println(Thread.currentThread().getStackTrace().mkString(","))

    val merged: Seq[ConsumerRecord[KV#K, KV#V]] =
      implicitly[Merger[KV]].apply(readers.map(_.consumerRecords))

    val views: Seq[ViewRecord[KV]] = implicitly[Processor[KV]].apply(merged)

    val written: Future[Seq[Seq[RecordMetadata]]] =
      Future.sequence(views.map(vws =>
        Future.sequence(
          vws.records.map(r =>
            implicitly[Writer[KV]].apply(vws.topic, vws.partition, r.key(), r.value())))))

    val committed: Future[Seq[CommittableReader[KV]]] =
      written.flatMap(com => Future.sequence(readers.map(_.commit(timeout))))

    committed.flatMap(runInternal(timeout)(_))
  }
}
