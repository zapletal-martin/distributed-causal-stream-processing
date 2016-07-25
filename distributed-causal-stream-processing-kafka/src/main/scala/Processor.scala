import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.ConsumerRecord

// Apply transformations
object Processor {
  final case class ViewRecord[K, V](
    records: Seq[ConsumerRecord[K, V]],
    topic: String,
    partition: Int)

  type Processor[K, V] =
    (Seq[ConsumerRecord[K, V]]) => Seq[ViewRecord[K, V]]

  // TODO: View allocation logic
  def processor[K, V](view1: TopicAndPartition, view2: TopicAndPartition): Processor[K, V] =
    records => {
        Seq(
          ViewRecord(records, view1.topic, view1.partition),
          ViewRecord(records, view2.topic, view2.partition))
    }
}
