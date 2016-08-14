package interface

import org.apache.kafka.common.serialization.{Deserializer, Serializer}

trait KeyValue {
  // K and V can both be null, because they are used with Kafka api
  type K >: Null
  type V >: Null
}

trait KVDeserializer[KV <: KeyValue] {
  def keyDeserializer: Deserializer[KV#K]
  def valueDeserializer: Deserializer[KV#V]
  def deserializer: (KV#K, KV#V) => KV
}

trait KVSerializer[KV <: KeyValue] {
  def keySerializer: Serializer[KV#K]
  def valueSerializer: Serializer[KV#V]
  def serializer: KV => (KV#K, KV#V)
}
