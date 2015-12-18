package com.magnetic.streaming.samza

import scala.collection.JavaConversions._
import com.magnetic.streaming.common.AdEventsParser.IMPRESSION
import org.apache.samza.config.Config
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStream, IncomingMessageEnvelope}
import org.apache.samza.task._

/**
 * Reads impressions and bids pre-parsed by previous step.
 * Since they were partitioned by auction id, ims and bids with the same key end up in the same task.
 * Uses local KV store assigned for given task to lookup matching events.
 * If match found, creates a joined event and sends it downstream.
 * Otherwise persists event for further lookups.
 * Periodically deletes stale events from local KV store.
 */
class MagneticJoinStreamsTask extends StreamTask with InitableTask with WindowableTask {

  val OUTPUT_STREAM = new SystemStream("kafka", "imp-bid-joined")
  var impStore: KeyValueStore[String, java.util.Map[String, Any]] = null
  var bidStore: KeyValueStore[String, java.util.Map[String, Any]] = null
  var lastImpTimestamp = 0
  var lastBidTimestamp = 0

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
    val event = mapAsScalaMap(envelope.getMessage.asInstanceOf[java.util.Map[String,Any]]).toMap
    envelope.getSystemStreamPartition.getSystemStream.getStream match {
      case "imp-meta" =>
        lastImpTimestamp = event("log_timestamp").asInstanceOf[Integer]
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
        lastBidTimestamp = event("log_timestamp").asInstanceOf[Integer]
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

  def cleanUpEventStore(eventStore: KeyValueStore[String, java.util.Map[String, Any]], thresholdTimestamp: Integer) {
    val it = eventStore.all()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getValue.get("log_timestamp").asInstanceOf[Integer] < thresholdTimestamp) {
        eventStore.delete(entry.getKey)
      }
    }
  }

  override def window(messageCollector: MessageCollector, taskCoordinator: TaskCoordinator) {
    cleanUpEventStore(impStore, lastImpTimestamp - 3600) //TODO Keep one hour of events. Make it configurable
    cleanUpEventStore(bidStore, lastBidTimestamp - 3600) //TODO Keep one hour of events. Make it configurable
  }
}
