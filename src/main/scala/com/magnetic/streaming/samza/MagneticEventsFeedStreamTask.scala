package com.magnetic.streaming.samza

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

class MagneticEventsFeedStreamTask extends StreamTask {
    val NUM_PARTITIONS = 4
    val IMP_OUTPUT_STREAM = new SystemStream("kafka", "imp-raw-partitioned")
    val BID_OUTPUT_STREAM = new SystemStream("kafka", "bid-raw-partitioned")
    val IMP_ERROR_STREAM = new SystemStream("kafka", "imp-error")
    val BID_ERROR_STREAM = new SystemStream("kafka", "bid-error")

    def getPartitionKey(key: String) = {
        key.hashCode() % NUM_PARTITIONS
    }

    def send(auctionId: String, event: String, collector: MessageCollector, system: SystemStream) {
        collector.send(
            new OutgoingMessageEnvelope(system, getPartitionKey(auctionId), auctionId, event)
        )
    }

    def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
        val event = envelope.getMessage.asInstanceOf[String]
        envelope.getSystemStreamPartition.getSystemStream.getStream match {
            case "imp-raw" =>
                AdEventsParser.extract_impression_auction_id(event) match {
                    case Some(auctionId) => send(auctionId, event, collector, IMP_OUTPUT_STREAM)
                    case None => collector.send(new OutgoingMessageEnvelope(IMP_ERROR_STREAM, event))
                }
            case "bid-raw" =>
                AdEventsParser.extract_bid_auction_id(event) match {
                    case Some(auctionId) => send(auctionId, event, collector, BID_OUTPUT_STREAM)
                    case None => collector.send(new OutgoingMessageEnvelope(BID_ERROR_STREAM, event))
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