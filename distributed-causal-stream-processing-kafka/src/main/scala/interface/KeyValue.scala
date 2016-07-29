package interface

import org.apache.kafka.common.serialization.{Deserializer, Serializer}

trait KeyValue {
  type K
  type V
}

object KeyValue {

  type KeyDeserializer[KV <: KeyValue] = Deserializer[KV#K]
  type ValueDeserializer[KV <: KeyValue] = Deserializer[KV#V]
  type KeySerializer[KV <: KeyValue] = Serializer[KV#K]
  type ValueSerializer[KV <: KeyValue] = Serializer[KV#V]
}