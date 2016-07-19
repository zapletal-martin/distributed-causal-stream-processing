import java.util

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

class ExactlyOnceDeliveryConsumerRebalanceListener(
    exactlyOnceDeliveryRecovery: ExactlyOnceDeliveryRecovery)
  extends ConsumerRebalanceListener {

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    // Find last event from OUR partitions in each view / merged stream
    // We know which partitions are assigned to us, we need to be able to infer which
    // persistenceIds we are interested in
    // Read last offset, compare to views
    // Update views that are not up to date if any
    // Continue reading from next offset.
  }

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
    // I don't think we need this
  }
}
