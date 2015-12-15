package com.magnetic.streaming.samza;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

/**
 * Created by stanislav on 12/15/15.
 */
public class MagneticEventsFeedStreamTask implements StreamTask {
    //private static final SystemStream IMP_OUTPUT_STREAM = new SystemStream("kafka", "imp-raw-partitioned");
    //private static final SystemStream BID_OUTPUT_STREAM = new SystemStream("kafka", "bid-raw-partitioned");

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String event = (String)envelope.getMessage();
        if(envelope.getSystemStreamPartition().getSystemStream().getStream().equals("imp-raw")){

            System.out.println("Imp: " + event);
        }
        else if (envelope.getSystemStreamPartition().getSystemStream().getStream().equals("bid-raw")){
            System.out.println("Bid: " + event);
        }
        else {
            throw new RuntimeException("Not supported stream: " + envelope.getSystemStreamPartition().getSystemStream().getStream());
        }
    }
}
