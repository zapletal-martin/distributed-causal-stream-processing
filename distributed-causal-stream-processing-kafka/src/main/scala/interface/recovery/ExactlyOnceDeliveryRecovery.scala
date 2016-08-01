package interface.recovery

import interface.{View, KeyValue}
import interface.Writer.Writer
import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition

case class ViewRecovery[KV <: KeyValue](
    viewTopicAndPartition: TopicAndPartition,
    viewReader: ViewReader[KV],
    viewWriter: Writer[KV],
    view: View[KV])

case class InputRecovery[KV <: KeyValue](reader: InputReader[KV])

case class ExactlyOnceDeliveryRecovery[KV <: KeyValue](
    timeout: Long,
    inversePartitioner: (KV#K, KV#V) => TopicPartition,
    views: Seq[ViewRecovery[KV]],
    input: InputRecovery[KV])
