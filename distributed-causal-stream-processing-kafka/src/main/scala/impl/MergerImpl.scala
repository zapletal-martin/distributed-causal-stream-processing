package impl

import scala.collection.JavaConverters._

import interface.KeyValue
import interface.Merger.Merger

object MergerImpl {

  final def merger[KV <: KeyValue]: Merger[KV] = records =>
    records.flatMap(_.iterator().asScala)
}
