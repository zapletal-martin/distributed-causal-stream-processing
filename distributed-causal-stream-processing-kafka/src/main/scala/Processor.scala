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
  def process[K, V]: Processor[K, V] =
    records => {
        Seq(
          ViewRecord(records, "viewTopic", 0),
          ViewRecord(records, "viewTopic2", 0))
    }
}
