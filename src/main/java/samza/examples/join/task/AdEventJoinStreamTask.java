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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import samza.examples.join.AdEventProducer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class AdEventJoinStreamTask implements StreamTask, InitableTask, WindowableTask {

  private final SystemStream ERROR_STREAM = new SystemStream("kafka", "ad-event-error");
  private final SystemStream JOIN_STREAM = new SystemStream("kafka", "ad-join");
  private KeyValueStore<String, Map<String, String>> impMetaStore;
  private KeyValueStore<String, Map<String, String>> clkMetaStore;
  private String lastTimestamp = null;

  @Override
  public void init(Config config, TaskContext taskContext) throws Exception {
    impMetaStore = (KeyValueStore) taskContext.getStore("imp-meta-store");
    clkMetaStore = (KeyValueStore) taskContext.getStore("clk-meta-store");
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    Map<String, String> adEvent = null;
    if (envelope.getKey() == null) {
      OutgoingMessageEnvelope ome = buildErrorEnvelope(ERROR_STREAM, new Exception("Envelope key cannot be null!"));
      collector.send(ome);
      return;
    }
    if(envelope.getMessage() instanceof Map) {
      adEvent = (Map<String, String>) envelope.getMessage();
    } else {
      OutgoingMessageEnvelope ome = buildErrorEnvelope(ERROR_STREAM, new Exception("Envelope message is not a map. Map is required"));
      collector.send(ome);
    }
    lastTimestamp = adEvent.get(AdEventProducer.TIMESTAMP);
    String key = (String)envelope.getKey();
    Map<String, String> joinEvent = null;
    try {
      if (adEvent.get(AdEventProducer.TYPE).equals(AdEventProducer.TYPE_IMPRESSION)) {
        Map<String, String> clkEvent = clkMetaStore.get(key);
        if (clkEvent != null){
          joinEvent = buildJoinEvent(adEvent, clkEvent);
          collector.send(new OutgoingMessageEnvelope(JOIN_STREAM, joinEvent));
          clkMetaStore.delete(key);
        }else
          impMetaStore.put(key, adEvent);
      } else if (adEvent.get(AdEventProducer.TYPE).equals(AdEventProducer.TYPE_CLICK)) {
        Map<String, String> impEvent = impMetaStore.get(key);
        if (impEvent != null){
          joinEvent = buildJoinEvent(impEvent, adEvent);
          collector.send(new OutgoingMessageEnvelope(JOIN_STREAM, joinEvent));
          impMetaStore.delete(key);
        } else
          clkMetaStore.put(key, adEvent);
      }
    } catch (ParseException pe) {
      collector.send(buildErrorEnvelope(ERROR_STREAM, pe));
    }
  }

  /**
   *  When window occurs all ad events older than latest event by more than 5 minutes are deleted from
   *  key-value stores.
   */
  @Override
  public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
    KeyValueIterator<String, Map<String, String>> iterator = impMetaStore.all();
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    long timestampThreshold = Long.MAX_VALUE;
    if(iterator.hasNext())
      timestampThreshold = dateFormatter.parse(lastTimestamp).getTime();
    while(iterator.hasNext()) {
      Entry<String, Map<String, String>> entry = iterator.next();
      if(dateFormatter.parse(entry.getValue().get(AdEventProducer.TIMESTAMP)).getTime() < timestampThreshold - 5*1000)
        impMetaStore.delete(entry.getKey());
    }
    iterator = clkMetaStore.all();
    if(iterator.hasNext())
      timestampThreshold = dateFormatter.parse(lastTimestamp).getTime();
    while(iterator.hasNext()) {
      Entry<String, Map<String, String>> entry = iterator.next();
      if(dateFormatter.parse(entry.getValue().get(AdEventProducer.TIMESTAMP)).getTime() < timestampThreshold - 5*1000)
        clkMetaStore.delete(entry.getKey());
    }
  }

  /**
   * Builds joined event from given impression and click events.
   * All it does is adding impression and click log-lines, timestamp and field that represents
   * time passed from when ad is shown to when the user clicked on it.
   *
   * @param impEvent impression event
   * @param clkEvent click event
   * @return joined data
   */
  private Map<String, String> buildJoinEvent(Map<String, String> impEvent, Map<String, String> clkEvent) throws ParseException {
    Map<String, String> joinEvent = new HashMap<>();

    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    Date impTimestamp = dateFormatter.parse(impEvent.get(AdEventProducer.TIMESTAMP));
    Date clkTimestamp = dateFormatter.parse(clkEvent.get(AdEventProducer.TIMESTAMP));
    joinEvent.put(AdEventProducer.IMPRESSION_ID, impEvent.get(AdEventProducer.IMPRESSION_ID));
    joinEvent.put(AdEventProducer.TIMESTAMP, dateFormatter.format(new Date()));
    joinEvent.put("passed-time-millis", Long.toString(clkTimestamp.getTime() - impTimestamp.getTime()));
    joinEvent.put("imp-log-line", impEvent.get("log-line"));
    joinEvent.put("clk-log-line", clkEvent.get("log-line"));
    return joinEvent;
  }

  private OutgoingMessageEnvelope buildErrorEnvelope(SystemStream stream, Throwable error) {
    Map<String, String> errorMessage = new HashMap<>();
    errorMessage.put("error", error.getClass().getCanonicalName());
    errorMessage.put("error-message", error.getMessage());
    errorMessage.put("stack-trace", ExceptionUtils.getFullStackTrace(error));
    return new OutgoingMessageEnvelope(stream, errorMessage);
  }
}