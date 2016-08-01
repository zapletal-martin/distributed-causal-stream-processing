/*
package interface.recovery

import interface.KeyValue
import interface.Writer.Writer
import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition

case class ViewRecovery[KV <: KeyValue](
    view: TopicAndPartition,
    viewReader: ViewReader[KV],
    viewWriter: Writer[KV],
    processor: (KV#K, KV#V) => (KV#K, KV#V),
    inverseTransformation: (KV#K, KV#V) => (KV#K, KV#V))

case class InputRecovery[KV <: KeyValue](reader: InputReader[KV])

case class ExactlyOnceDeliveryRecovery[KV <: KeyValue](
    timeout: Long,
    inversePartitioner: (KV#K, KV#V) => TopicPartition,
    views: Seq[ViewRecovery[KV]],
    input: InputRecovery[KV])
*/
