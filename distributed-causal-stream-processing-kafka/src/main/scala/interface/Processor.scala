package interface

import org.apache.kafka.clients.consumer.ConsumerRecord

// Apply transformations
object Processor {
  final case class ViewRecord[KV <: KeyValue](
    records: Seq[ConsumerRecord[KV#K, KV#V]],
    topic: String,
    partition: Int)

  type Processor[KV <: KeyValue] = (Seq[ConsumerRecord[KV#K, KV#V]]) => Seq[ViewRecord[KV]]
}
