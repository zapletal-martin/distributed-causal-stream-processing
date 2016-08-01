package interface

import scala.concurrent.{ExecutionContext, Future}

import interface.Merger._
import interface.Writer._

object Program {

  def run[KV <: KeyValue : Merger : Writer](
      timeout: Long
    )(readers: Seq[PollableReader[KV]],
      views: Set[View[KV]]
    )(implicit ec: ExecutionContext
    ): Future[Seq[PollableReader[KV]]] = {

    runInternal(timeout)(readers.map(_.poll(timeout)), views)
  }

  private def runInternal[KV <: KeyValue : Merger : Writer](
      timeout: Long
    )(readers: Seq[CommittableReader[KV]],
      views: Set[View[KV]]
    )(implicit ec: ExecutionContext
    ): Future[Seq[PollableReader[KV]]] = {

    println(Thread.currentThread().getStackTrace().mkString(","))

    val merged: Seq[KV] =
      implicitly[Merger[KV]].apply(readers.map(_.consumerRecords))

    val written = Future.sequence(
      merged.map { m =>
        Future.sequence(
          views.map{ w =>
            val transformed = w.transformation(m)
            implicitly[Writer[KV]].apply(
              transformed.topic,
              transformed.partition,
              transformed.record)
          }
        )
      }
    )

    val committed: Future[Seq[CommittableReader[KV]]] =
      written.flatMap(com => Future.sequence(readers.map(_.commit(timeout))))

    committed.flatMap(runInternal(timeout)(_, views))
  }
}
