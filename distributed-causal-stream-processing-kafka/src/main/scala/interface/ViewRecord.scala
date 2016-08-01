package interface

final case class ViewRecord[KV <: KeyValue](
    record: KV,
    topic: String,
    partition: Int)
