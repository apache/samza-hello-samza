/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package samza.examples.join.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samza.examples.join.AdEventParser;
import samza.examples.join.AdEventProducer;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class AdEventFeedStreamTask implements StreamTask {

  private final int NUM_OF_PARTITIONS = 4;
  private final SystemStream ERROR_STREAM = new SystemStream("kafka", "ad-event-error");
  private final SystemStream IMP_META_STREAM = new SystemStream("kafka", "ad-imp-metadata");
  private final SystemStream CLK_META_STREAM = new SystemStream("kafka", "ad-clk-metadata");

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    String rawAdEvent = (String)envelope.getMessage();
    if (envelope.getKey() == null) {
      OutgoingMessageEnvelope ome = buildErrorEnvelope(ERROR_STREAM, rawAdEvent, new Exception("Envelope key cannot be null!"));
      collector.send(ome);
      return;
    }

    try {
      Map<String, String> adEvent = AdEventParser.parseAdEvent(rawAdEvent);
      adEvent.put("log-line", rawAdEvent);
      SystemStream outgoingStream = null;
      String adEventType = adEvent.get(AdEventProducer.TYPE);
      if(adEventType.equals(AdEventProducer.TYPE_IMPRESSION))
        outgoingStream = IMP_META_STREAM;
      else if(adEventType.equals(AdEventProducer.TYPE_CLICK))
        outgoingStream = CLK_META_STREAM;

      if(outgoingStream != null) {
        OutgoingMessageEnvelope outgoingEnvelope = new OutgoingMessageEnvelope(outgoingStream
            , envelope.getKey().hashCode()%NUM_OF_PARTITIONS
            , envelope.getKey()
            , adEvent);
        collector.send(outgoingEnvelope);
      } else {
        OutgoingMessageEnvelope ome = buildErrorEnvelope(ERROR_STREAM, rawAdEvent, new Exception("Ad event type ('" + adEventType + "') unknown"));
        collector.send(ome);
      }
    } catch (ParseException pe) {
      OutgoingMessageEnvelope ome = buildErrorEnvelope(ERROR_STREAM, rawAdEvent, pe);
      collector.send(ome);
    }
  }

  private OutgoingMessageEnvelope buildErrorEnvelope(SystemStream stream, String rawEvent, Throwable error) {
    Map<String, String> errorMessage = new HashMap<>();
    errorMessage.put("error", error.getClass().getCanonicalName());
    errorMessage.put("error-message", error.getMessage());
    errorMessage.put("raw-event", rawEvent);
    return new OutgoingMessageEnvelope(stream, errorMessage);
  }
}