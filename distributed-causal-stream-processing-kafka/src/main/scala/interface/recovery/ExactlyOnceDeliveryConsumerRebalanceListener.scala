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

// We should really
// Find missing records from each partition in each view
// E.g. go backwards in each partition until we find the last from that partition in our view
// Run merger and processor on the missing records (we need to ensure same ordering as in
// all the other views where the records were already published), publish, commit
class ExactlyOnceDeliveryConsumerRebalanceListener[KV <: KeyValue : KVDeserializer : KVSerializer, R](
    exactlyOnceDeliveryRecovery: ExactlyOnceDeliveryRecovery[KV, R],
    consumer: KafkaConsumer[KV#K, KV#V]
  )(implicit ec: ExecutionContext)
  extends ConsumerRebalanceListener {

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    println("Starting onPartitionsAssigned")

    val toUpdateFuture = exactlyOnceDeliveryRecovery
      .views
      .flatMap { v =>
        // for each partition
        exactlyOnceDeliveryRecovery.input.inputTopicsAndPartitions.map { tp =>
          val found = v.viewReader.findLastInViewFromPartition(
            exactlyOnceDeliveryRecovery.timeout,
            new TopicPartition(
              tp.topic,
              tp.partition),
            exactlyOnceDeliveryRecovery.partitioner,
            v.view,
            v.viewTopicAndPartition)

          println(s"found $found")

          // TODO: Include the last committed?
          val potentiallyMissingInView =
            exactlyOnceDeliveryRecovery
              .input
              .reader
              .findAllAfterLastCommitted(
                consumer,
                exactlyOnceDeliveryRecovery.timeout,
                Seq(tp),
                exactlyOnceDeliveryRecovery.input.inputGroup)

          println(s"potentiallyMissingInView $potentiallyMissingInView")

          val inverted =
            found.map(_.map(f =>
              v.view.inverseTransformation(
              ViewRecord(
                implicitly[KVDeserializer[KV]].deserializer(f.key(), f.value()),
                f.topic(),
                f.partition()))))

          println(s"inverted $inverted")

          val invertedFoundInInput = potentiallyMissingInView
            .map(r =>
              inverted.fold[Option[ConsumerRecord[KV#K, KV#V]]](None)(i =>
                i.find(x => x == implicitly[KVDeserializer[KV]].deserializer(r.key(), r.value()))
                  .map(_ => r)))
            .flatMap(_.fold[Set[ConsumerRecord[KV#K, KV#V]]](Set())(Set(_)))

          println(s"invertedFoundInInput $invertedFoundInInput")

          Future.sequence(
            potentiallyMissingInView
              .filter { r =>
                val thisPartition = invertedFoundInInput.filter(vf =>
                  vf.topic() == r.topic() &&
                  vf.partition() == r.partition())

                if (thisPartition.isEmpty) {
                  true
                } else {
                  thisPartition.exists(vf => r.offset() > vf.offset())
                }
              }
              .flatMap { up =>
                val processed = v.view.transformation(
                  implicitly[KVDeserializer[KV]].deserializer(up.key(), up.value()))

                processed.fold[Option[Future[R]]](None) { p =>
                  Some(
                    v.viewWriter(
                      v.viewTopicAndPartition.topic,
                      v.viewTopicAndPartition.partition,
                      p.record))
                }
              })
            .map(_ => invertedFoundInInput)
        }
      }

      // We need to await here. The kafka consumer would otherwise start
      // consuming before the operation is completed asynchronously
      val foundInInput = Await.result(Future.sequence(toUpdateFuture), 10.seconds)

      val toCommit = foundInInput
        .flatten
        .map(l => TopicAndPartition(l.topic(), l.partition()) -> OffsetAndMetadata(l.offset() + 1))
        .groupBy(_._1)
        .map(x => x._2.maxBy(_._2.offset))

      println(s"TO COMMIT $toCommit")

      /*println(s"BEFORE COMMITS ${exactlyOnceDeliveryRecovery
        .input
        .reader
        .lastCommitedMessageInEachAssignedPartition(
          exactlyOnceDeliveryRecovery.timeout,
          exactlyOnceDeliveryRecovery.input.inputTopicsAndPartitions,
          exactlyOnceDeliveryRecovery.input.inputGroup)}")*/

      println(s"To Commit $toCommit, group ${exactlyOnceDeliveryRecovery.input.inputGroup}")

      exactlyOnceDeliveryRecovery.input.reader.commit(
        consumer,
        toCommit,
        exactlyOnceDeliveryRecovery.input.inputGroup)

      /*println(s"AFTER COMMITS ${exactlyOnceDeliveryRecovery
        .input
        .reader
        .lastCommitedMessageInEachAssignedPartition(
          exactlyOnceDeliveryRecovery.timeout,
          exactlyOnceDeliveryRecovery.input.inputTopicsAndPartitions,
          exactlyOnceDeliveryRecovery.input.inputGroup)}")*/

      println("Rebalance done")

    /*
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

    println("Rebalance done")*/
  }

  // I don't think we need this
  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = { }
}
