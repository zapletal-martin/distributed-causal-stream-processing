package interface.recovery

import java.util

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import interface.{KVDeserializer, KVSerializer, KeyValue, ViewRecord}
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition


// Find last event from OUR partitions in each view / merged stream (going from the end)
// We know which partitions are assigned to us, we need to be able to infer which
// persistenceIds we are interested in
// Read last offset, compare to views
// Update views that are not up to date if any
// Continue reading from next offset.

// so need - view references, partitioner to find viewEvent => topic and partition
// of the original stream, committable reader from original stream, reader from view
class ExactlyOnceDeliveryConsumerRebalanceListener[KV <: KeyValue : KVDeserializer : KVSerializer, R](
    exactlyOnceDeliveryRecovery: ExactlyOnceDeliveryRecovery[KV, R],
    consumer: KafkaConsumer[KV#K, KV#V]
  )(implicit ec: ExecutionContext)
  extends ConsumerRebalanceListener {

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    println("Starting onPartitionsAssigned")

    val lastCommitted: Iterable[ConsumerRecord[KV#K, KV#V]] =
      exactlyOnceDeliveryRecovery
        .input
        .reader
        .lastCommitedMessageInEachAssignedPartition(
          exactlyOnceDeliveryRecovery.timeout,
          exactlyOnceDeliveryRecovery.input.inputTopicsAndPartitions,
          exactlyOnceDeliveryRecovery.input.inputGroup)

    println(s"Last committed in each partition ${lastCommitted.mkString(", ")}")

    val update: Iterable[Seq[Future[R]]] =
      lastCommitted.map(commited =>
        exactlyOnceDeliveryRecovery
          .views
          .flatMap { v =>
            val found = v.viewReader.findLastInViewFromPartition(
              exactlyOnceDeliveryRecovery.timeout,
              new TopicPartition(
                commited.topic(),
                commited.partition()),
              exactlyOnceDeliveryRecovery.partitioner,
              v.view,
              v.viewTopicAndPartition)

            println(s"Found in view ${v.viewTopicAndPartition.topic} -> ${found.mkString(", ")}")

            val toUpdate = found.flatMap(
              _.find { r =>
                val inverted = v.view.inverseTransformation(
                  ViewRecord(
                    implicitly[KVDeserializer[KV]].deserializer(r.key(), r.value()),
                    r.topic(),
                    r.partition()))

                val original = implicitly[KVDeserializer[KV]]
                  .deserializer(commited.key(), commited.value())

                println(s"Comparing ${v.viewTopicAndPartition.topic} -> $inverted vs $original")
                inverted != original
              })

            toUpdate.flatMap { up =>
              val processed = v.view.transformation(
                implicitly[KVDeserializer[KV]].deserializer(commited.key(), commited.value()))

              processed.fold[Option[Future[R]]](None) { p =>
                Some(
                  v.viewWriter(
                    v.viewTopicAndPartition.topic,
                    v.viewTopicAndPartition.partition,
                    p.record))
              }
            }
          })

    // We need to await here. The kafka consumer would otherwise start
    // consuming before the operation is completed asynchronously
    Await.result(Future.sequence(update.map(Future.sequence(_))), 10.seconds)
    // TODO: HANDLE CASE OF MULTIPLE RECORDS MISSING (TWO+ RECORDS PUBLISHED TO ONE VIEW BUT NOT OTHER)
    // TODO: Commit inputs!

    val toCommit = lastCommitted
      .map(l => TopicAndPartition(l.topic(), l.partition()) -> OffsetAndMetadata(l.offset() + 1))
      .groupBy(_._1)
      .map(x => x._2.maxBy(_._2.offset))


    println(s"BEFORE COMMITS ${exactlyOnceDeliveryRecovery
      .input
      .reader
      .lastCommitedMessageInEachAssignedPartition(
        exactlyOnceDeliveryRecovery.timeout,
        exactlyOnceDeliveryRecovery.input.inputTopicsAndPartitions,
        exactlyOnceDeliveryRecovery.input.inputGroup)}")

    println(s"To Commit $toCommit, group ${exactlyOnceDeliveryRecovery.input.inputGroup}")

    val committed = exactlyOnceDeliveryRecovery.input.reader.commit(
      consumer,
      toCommit,
      exactlyOnceDeliveryRecovery.input.inputGroup)

    println(s"COMMITTED $committed")

    println(s"AFTER COMMITS ${exactlyOnceDeliveryRecovery
      .input
      .reader
      .lastCommitedMessageInEachAssignedPartition(
        exactlyOnceDeliveryRecovery.timeout,
        exactlyOnceDeliveryRecovery.input.inputTopicsAndPartitions,
        exactlyOnceDeliveryRecovery.input.inputGroup)}")

    println("Rebalance done")
  }

  // I don't think we need this
  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = { }
}
