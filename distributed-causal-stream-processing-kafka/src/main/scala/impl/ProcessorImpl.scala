package impl

import kafka.common.TopicAndPartition
import lib.KeyValue
import lib.Processor.{Processor, ViewRecord}

object ProcessorImpl {

  final def processor[KV <: KeyValue](
      view1: TopicAndPartition,
      view2: TopicAndPartition
    ): Processor[KV] =
    records => {
      Seq(
        ViewRecord(records, view1.topic, view1.partition),
        ViewRecord(records, view2.topic, view2.partition))
    }
}
