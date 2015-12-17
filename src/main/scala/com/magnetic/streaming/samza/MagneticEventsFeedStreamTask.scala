package com.magnetic.streaming.samza

import scala.collection.JavaConversions._
import com.magnetic.streaming.common.AdEventsParser
import org.apache.samza.Partition
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.TaskCoordinator.RequestScope

import scala.util.{Failure, Success, Try}

class MagneticEventsFeedStreamTask extends StreamTask {
  val NUM_PARTITIONS = 4
  val IMP_OUTPUT_STREAM = new SystemStream("kafka", "imp-meta")
  val BID_OUTPUT_STREAM = new SystemStream("kafka", "bid-meta")
  val IMP_ERROR_STREAM = new SystemStream("kafka", "imp-error")
  val BID_ERROR_STREAM = new SystemStream("kafka", "bid-error")

  def getPartitionKey(key: String) = {
    key.hashCode() % NUM_PARTITIONS
  }

  def send(event: Map[String, Any], collector: MessageCollector, system: SystemStream) {
    val auctionId = event("auction_id").asInstanceOf[String]
    collector.send(
      new OutgoingMessageEnvelope(system, getPartitionKey(auctionId), auctionId, mapAsJavaMap(event))
    )
  }

  def sendError(rawEvent: String, ex: Throwable, collector: MessageCollector, system: SystemStream) {
    val error = Map("event_type" -> AdEventsParser.ERROR, "log_line" -> rawEvent, "exception" -> ex.getMessage)
    collector.send(
      new OutgoingMessageEnvelope(system, mapAsJavaMap(error))
    )
  }

  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    val rawEvent = envelope.getMessage.asInstanceOf[String]
    envelope.getSystemStreamPartition.getSystemStream.getStream match {
      case "imp-raw" =>
        Try(AdEventsParser.parse_imp_meta(rawEvent)) match {
          case Success(metaEvent) => send(metaEvent, collector, IMP_OUTPUT_STREAM)
          case Failure(exception) => sendError(rawEvent, exception, collector, IMP_ERROR_STREAM)
        }
      case "bid-raw" =>
        Try(AdEventsParser.parse_bid_meta(rawEvent)) match {
          case Success(metaEvent) => send(metaEvent, collector, BID_OUTPUT_STREAM)
          case Failure(exception) => sendError(rawEvent, exception, collector, BID_ERROR_STREAM)
        }
      case notSupportedStream =>
        throw new RuntimeException(s"Not supported stream: $notSupportedStream")
    }
  }
}

object HelloMagneticSamza {
  def main(args: Array[String]): Unit = {
    val t = new MagneticEventsFeedStreamTask()
    val system = new SystemStream("bla", "imp-raw")
    val partition = new SystemStreamPartition(system, new Partition(0))
    val envelope = new IncomingMessageEnvelope(partition, "", "key", "msg")
    val collector = new MessageCollector() {
      def send(envelope: OutgoingMessageEnvelope) {
        println("Send method called:" +  envelope.getSystemStream.getStream)
        println("Message:" +  envelope.getMessage)
      }
    }
    val coordinator = new TaskCoordinator() {
      def commit(requestScope: RequestScope) {
        println("Commit method called")
      }

      def shutdown(requestScope: RequestScope) {
        System.out.println("Shautdown method called")
      }
    }
    t.process(envelope, collector, coordinator)
  }
}