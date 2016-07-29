package impl

import scala.collection.JavaConverters._

import lib.KeyValue
import lib.Merger.Merger

object MergerImpl {

  final def merger[KV <: KeyValue]: Merger[KV] = records =>
    records.flatMap(_.iterator().asScala)
}
