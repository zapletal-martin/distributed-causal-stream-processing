package interface.recovery

import interface.Writer.Writer
import interface.recovery.ExactlyOnceDeliveryRecovery.Partitioner
import interface.{KeyValue, View}
import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition

case class ViewRecovery[KV <: KeyValue, R](
    viewTopicAndPartition: TopicAndPartition,
    viewReader: PollableViewReader[KV],
    viewWriter: Writer[KV, R],
    view: View[KV])

case class InputRecovery[KV <: KeyValue](
  reader: InputReader[KV],
  inputTopicsAndPartitions: Seq[TopicAndPartition],
  inputGroup: String)

object ExactlyOnceDeliveryRecovery {
  type Partitioner[KV <: KeyValue] = KV => TopicPartition
}

case class ExactlyOnceDeliveryRecovery[KV <: KeyValue, R](
    timeout: Long,
    partitioner: Partitioner[KV],
    views: Seq[ViewRecovery[KV, R]],
    input: InputRecovery[KV])
