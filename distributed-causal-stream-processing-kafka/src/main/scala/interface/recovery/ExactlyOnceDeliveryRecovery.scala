package interface.recovery

import interface.Writer.Writer
import interface.{KeyValue, View}
import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition

case class ViewRecovery[KV <: KeyValue, R](
    viewTopicAndPartition: TopicAndPartition,
    viewReader: ViewReader[KV],
    viewWriter: Writer[KV, R],
    view: View[KV])

case class InputRecovery[KV <: KeyValue](reader: InputReader[KV])

case class ExactlyOnceDeliveryRecovery[KV <: KeyValue, R](
    timeout: Long,
    inversePartitioner: (KV#K, KV#V) => TopicPartition,
    views: Seq[ViewRecovery[KV, R]],
    input: InputRecovery[KV])
