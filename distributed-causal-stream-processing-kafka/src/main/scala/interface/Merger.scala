package interface

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

object Merger {

  type Merger[KV <: KeyValue] =
    Seq[ConsumerRecords[KV#K, KV#V]] => Seq[ConsumerRecord[KV#K, KV#V]]
}
