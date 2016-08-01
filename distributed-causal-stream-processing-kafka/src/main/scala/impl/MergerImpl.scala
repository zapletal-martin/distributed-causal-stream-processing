package impl

import interface.KeyValue
import interface.Merger.Merger

object MergerImpl {
  final def merger[KV <: KeyValue]: Merger[KV] = records => records.flatten
}
