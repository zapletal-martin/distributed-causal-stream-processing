package interface

case class View[KV <: KeyValue](
    transformation: KV => Option[ViewRecord[KV]],
    inverseTransformation: ViewRecord[KV] => KV)
