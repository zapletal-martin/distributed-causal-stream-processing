package interface

import scala.concurrent.{ExecutionContext, Future}

import interface.Merger._
import interface.Reader.{CommittableReader, PollableReader}
import interface.Writer._

object Program {

  def run[KV <: KeyValue : Merger, R](
      timeout: Long
    )(readers: Seq[PollableReader[KV]],
      views: Set[View[KV]]
    )(implicit writer: Writer[KV, R],
      ec: ExecutionContext
    ): Future[Seq[PollableReader[KV]]] = {
    Future.sequence(readers.map(_.poll(timeout))).flatMap(pr => runInternal(timeout)(pr, views))
  }

  private def runInternal[KV <: KeyValue : Merger, R](
      timeout: Long
    )(readers: Seq[CommittableReader[KV]],
      views: Set[View[KV]]
    )(implicit writer: Writer[KV, R],
      ec: ExecutionContext
    ): Future[Seq[PollableReader[KV]]] = {

    println(Thread.currentThread().getStackTrace().mkString(","))

    applyViewLogicAndCommit(timeout)(readers, views)
      .flatMap(runInternal(timeout)(_, views))
  }

  def applyViewLogicAndCommit[KV <: KeyValue : Merger, R](
      timeout: Long
    )(readers: Seq[CommittableReader[KV]],
      views: Set[View[KV]]
    )(implicit writer: Writer[KV, R],
      ec: ExecutionContext
    ): Future[Seq[CommittableReader[KV]]] =
    applyViewLogic(timeout)(readers, views)
      .flatMap(com => Future.sequence(readers.map(_.commit(timeout))))

  def applyViewLogic[KV <: KeyValue : Merger, R](
      timeout: Long
    )(readers: Seq[CommittableReader[KV]],
      views: Set[View[KV]]
    )(implicit writer: Writer[KV, R],
      ec: ExecutionContext
    ): Future[Set[Seq[R]]] =
    Future.sequence(
      views.map{ w =>
        Future.sequence(
          implicitly[Merger[KV]].apply(readers.map(_.records)).flatMap { m =>
            val transformed = w.transformation(m)
            transformed.fold[Seq[Future[R]]](Seq.empty) { t =>
              Seq(
                implicitly[Writer[KV, R]].apply(
                  t.topic,
                  t.partition,
                  t.record))
            }
          }
        )
      }
    )
}
