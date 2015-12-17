package com.magnetic.streaming.samza

import scala.collection.JavaConversions._
import com.magnetic.streaming.common.AdEventsParser.IMPRESSION
import org.apache.samza.config.Config
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStream, IncomingMessageEnvelope}
import org.apache.samza.task._

class MagneticJoinStreamsTask extends StreamTask with InitableTask with WindowableTask {

  val OUTPUT_STREAM = new SystemStream("kafka", "imp-bid-joined")
  var impStore: KeyValueStore[String, java.util.Map[String, Any]] = null
  var bidStore: KeyValueStore[String, java.util.Map[String, Any]] = null

  override def init(config: Config, context: TaskContext) {
    this.impStore = context.getStore("imp-store").asInstanceOf[KeyValueStore[String, java.util.Map[String, Any]]]
    this.bidStore = context.getStore("bid-store").asInstanceOf[KeyValueStore[String, java.util.Map[String, Any]]]
  }

  def buildJoinedEvent(metaImp:Map[String,Any], metaBid:Map[String,Any]):Map[String,Any] = {
    val d = scala.collection.mutable.Map[String, Any]()
    d("event_type") = IMPRESSION
    d("auction_id") = metaImp("auction_id")
    d("log_timestamp") = metaImp("log_timestamp")
    d("imp_log_line") = metaImp("imp_log_line")
    d("bid_log_line") = metaBid("bid_log_line")
    d.toMap
  }

  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    val key = envelope.getKey.asInstanceOf[String]
    println(envelope.getMessage)
    val event = mapAsScalaMap(envelope.getMessage.asInstanceOf[java.util.Map[String,Any]]).toMap
    envelope.getSystemStreamPartition.getSystemStream.getStream match {
      case "imp-meta" =>
        Option(bidStore.get(key)) match {
          case Some(bid) =>
            collector.send(
              new OutgoingMessageEnvelope(
                OUTPUT_STREAM, key, mapAsJavaMap(buildJoinedEvent(event, mapAsScalaMap(bid).toMap))
              )
            )
            bidStore.delete(key)
          case None => impStore.put(key, mapAsJavaMap(event))
        }
      case "bid-meta" =>
        Option(impStore.get(key)) match {
          case Some(imp) =>
            collector.send(
              new OutgoingMessageEnvelope(
                OUTPUT_STREAM, key, mapAsJavaMap(buildJoinedEvent(mapAsScalaMap(imp).toMap, event))
              )
            )
            impStore.delete(key)
          case None => bidStore.put(key, mapAsJavaMap(event))
        }
      case notSupportedStream =>
          throw new RuntimeException(s"Not supported stream: $notSupportedStream")
    }
  }

  override def window(messageCollector: MessageCollector, taskCoordinator: TaskCoordinator) {

  }
}
