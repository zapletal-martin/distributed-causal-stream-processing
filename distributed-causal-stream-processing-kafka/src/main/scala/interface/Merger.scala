package interface

object Merger {
  type Merger[KV <: KeyValue] = Seq[Seq[KV]] => Seq[KV]
}
