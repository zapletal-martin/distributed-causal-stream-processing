package interface.recovery

import java.util

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import interface.{KVSerializer, KVDeserializer, ViewRecord, KeyValue}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition


// Find last event from OUR partitions in each view / merged stream (going from the end)
// We know which partitions are assigned to us, we need to be able to infer which
// persistenceIds we are interested in
// Read last offset, compare to views
// Update views that are not up to date if any
// Continue reading from next offset.

// so need - view references, partitioner to find viewEvent => topic and partition
// of the original stream, committable reader from original stream, reader from view
class ExactlyOnceDeliveryConsumerRebalanceListener[KV <: KeyValue : KVDeserializer : KVSerializer](
    exactlyOnceDeliveryRecovery: ExactlyOnceDeliveryRecovery[KV]
  )(implicit ec: ExecutionContext)
  extends ConsumerRebalanceListener {

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {

    val lastCommitted: Iterable[ConsumerRecord[KV#K, KV#V]] =
      exactlyOnceDeliveryRecovery
        .input
        .reader
        .lastCommitedMessageInEachAssignedPartition(exactlyOnceDeliveryRecovery.timeout)

    val update: Iterable[Seq[Future[Unit]]] =
      lastCommitted.map(commited =>
        exactlyOnceDeliveryRecovery
          .views
          .map { v =>
            val found = v.viewReader.findLastInViewFromPartition(
              exactlyOnceDeliveryRecovery.timeout,
              new TopicPartition(
                commited.topic(),
                commited.partition()),
              exactlyOnceDeliveryRecovery.inversePartitioner)

            found.map(
              _.find(r =>
                v.view.inverseTransformation(
                  ViewRecord(
                    // TODO: ViewDeserializer
                    implicitly[KVDeserializer[KV]].deserializer(r.key(), r.value()),
                    r.topic(),
                    r.partition())) ==
                      (commited.key(), commited.value())))

            val processed = v.view.transformation(
              implicitly[KVDeserializer[KV]].deserializer(commited.key(), commited.value()))

            v.viewWriter(
              v.viewTopicAndPartition.topic,
              v.viewTopicAndPartition.partition,
              processed.record)
          })

    // We need to await here. The kafka consumer would otherwise start
    // consuming before the operation is completed asynchronously
    Await.result(Future.sequence(update.map(Future.sequence(_))), 10.seconds)
  }

  // I don't think we need this
  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = { }
}
