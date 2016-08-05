package interface

case class View[KV <: KeyValue](
    transformation: KV => ViewRecord[KV],
    inverseTransformation: ViewRecord[KV] => KV)
