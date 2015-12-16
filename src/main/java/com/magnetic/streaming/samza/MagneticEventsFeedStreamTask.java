package com.magnetic.streaming.samza;

import com.magnetic.streaming.common.AdEventsParser;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import scala.Option;

/**
 * Created by stanislav on 12/15/15.
 */
public class MagneticEventsFeedStreamTask implements StreamTask {
    private static final int NUM_PARTITIONS = 4;
    private static final SystemStream IMP_OUTPUT_STREAM = new SystemStream("kafka", "imp-raw-partitioned");
    private static final SystemStream BID_OUTPUT_STREAM = new SystemStream("kafka", "bid-raw-partitioned");
    private static final SystemStream IMP_ERROR_STREAM = new SystemStream("kafka", "imp-error");
    private static final SystemStream BID_ERROR_STREAM = new SystemStream("kafka", "bid-error");

    private int getPartitionKey(String key){
        return key.hashCode() % NUM_PARTITIONS;
    }

    private void send(Option key, String event, MessageCollector collector, SystemStream system){
        if(key.isDefined()){
            String auctionId = (String)key.get();
            collector.send(new OutgoingMessageEnvelope(system, getPartitionKey(auctionId), auctionId, event));
        }
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String event = (String)envelope.getMessage();
        if(envelope.getSystemStreamPartition().getSystemStream().getStream().equals("imp-raw")){
            Option key = AdEventsParser.extract_impression_auction_id(event);
            if(key.isDefined()) {
                send(key, event, collector, IMP_OUTPUT_STREAM);
            }
            else {
                collector.send(new OutgoingMessageEnvelope(IMP_ERROR_STREAM, event));
            }
        }
        else if (envelope.getSystemStreamPartition().getSystemStream().getStream().equals("bid-raw")){
            Option key = AdEventsParser.extract_bid_auction_id(event);
            if(key.isDefined()) {
                send(key, event, collector, BID_OUTPUT_STREAM);
            }
            else {
                collector.send(new OutgoingMessageEnvelope(BID_ERROR_STREAM, event));
            }
        }
        else {
            throw new RuntimeException("Not supported stream: " + envelope.getSystemStreamPartition().getSystemStream().getStream());
        }
    }

    /**
     * Sample run
     * @param arg
     * @throws Exception
     */
    public static void main(String[] arg) throws Exception {
        MagneticEventsFeedStreamTask t = new MagneticEventsFeedStreamTask();
        SystemStream system = new SystemStream("bla", "imp-raw");
        SystemStreamPartition partition = new SystemStreamPartition(system, new Partition(0));
        IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(partition, "", "key", "msg");
        MessageCollector collector = new MessageCollector() {
            @Override
            public void send(OutgoingMessageEnvelope envelope) {
                System.out.println("Send method called:" +  envelope.getSystemStream().getStream());
            }
        };
        TaskCoordinator coordinator = new TaskCoordinator() {
            @Override
            public void commit(RequestScope requestScope) {
                System.out.println("Commit method called");
            }

            @Override
            public void shutdown(RequestScope requestScope) {
                System.out.println("Shautdown method called");
            }
        };
        t.process(envelope, collector, coordinator);
    }
}
