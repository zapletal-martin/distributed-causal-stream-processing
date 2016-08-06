package interface

import scala.concurrent.Future

object Writer {
  type Writer[KV <: KeyValue, R] = (String, Int, KV) => Future[R]
}
