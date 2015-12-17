package com.magnetic.streaming.samza

import org.apache.samza.config.Config
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStream, IncomingMessageEnvelope}
import org.apache.samza.task._

class MagneticJoinStreamsTask extends StreamTask with InitableTask with WindowableTask {

  val OUTPUT_STREAM = new SystemStream("kafka", "imp-bid-joined")
  var impStore: KeyValueStore[String, String] = null
  var bidStore: KeyValueStore[String, String] = null

  override def init(config: Config, context: TaskContext) {
    this.impStore = context.getStore("imp-store").asInstanceOf[KeyValueStore[String, String]]
    this.bidStore = context.getStore("bid-store").asInstanceOf[KeyValueStore[String, String]]
  }

  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    val key = envelope.getKey.asInstanceOf[String]
    val event = envelope.getMessage.asInstanceOf[String]
    envelope.getSystemStreamPartition.getSystemStream.getStream match {
      case "imp-raw-partitioned" =>
        Option(bidStore.get(key)) match {
          case Some(bid) =>
            collector.send(
              new OutgoingMessageEnvelope(OUTPUT_STREAM, key, s"{ raw_imp:$event, raw_bid:$bid }")
            )
            bidStore.delete(key)
          case None => impStore.put(key, event)
        }
      case "bid-raw-partitioned" =>
        Option(impStore.get(key)) match {
          case Some(imp) =>
            collector.send(
              new OutgoingMessageEnvelope(OUTPUT_STREAM, key, s"{raw_imp:$imp, raw_bid:$event }")
            )
            impStore.delete(key)
          case None => bidStore.put(key, event)
        }
      case notSupportedStream =>
          throw new RuntimeException(s"Not supported stream: $notSupportedStream")
    }
  }

  override def window(messageCollector: MessageCollector, taskCoordinator: TaskCoordinator) {

  }
}
