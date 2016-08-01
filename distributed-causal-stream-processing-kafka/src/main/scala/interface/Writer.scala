package interface

import scala.concurrent.Future

object Writer {
  type Writer[KV <: KeyValue] = (String, Int, KV) => Future[Unit]
}
