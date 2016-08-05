package interface

import scala.concurrent.{ExecutionContext, Future}

object Reader {
  trait CommittableReader[KV <: KeyValue] {
    def commit(
        pollTimeout: Long
    )(implicit ec: ExecutionContext
    ): Future[CommittableReader[KV]]

    def records: Seq[KV]
  }


  trait PollableReader[KV <: KeyValue] {
    def poll(timeout: Long): CommittableReader[KV]
  }
}
