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

package samza.examples.join;

import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.log4j.*;
import org.apache.samza.serializers.StringSerde;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.List;
import java.util.Iterator;
import java.util.Arrays;
import java.util.concurrent.*;

/**
 *  Generates dummy AdImpression and AdClick events and sends it to kafka broker on localhost, port 9092.
 *
 *  AdEvent info contains impression-id, type (impression or click), advertiser-id, ip, agent, and timestamp.
 *  All info is dummy.
 */
public class AdEventProducer {

  private static final Logger logger = LogManager.getLogger(AdEventProducer.class);

  public static final String IMPRESSION_ID = "impression-id";
  public static final String TYPE = "type";
  public static final String TYPE_CLICK = "click";
  public static final String TYPE_IMPRESSION = "impression";
  public static final String ADVERTISER_ID = "advertiser-id";
  public static final String IP = "ip";
  public static final String AGENT = "agent";
  public static final String TIMESTAMP = "timestamp";

  private static int newImpressionId = 0;
  private static int numOfAdImpPartitions = 0;
  private static int numOfAdClkPartitions = 0;

  private static KafkaProducer<String, String> producer;

  private static String[] agents;

  static {
    agents = new String[6];
    agents[0] = "Edge";
    agents[1] = "Safari";
    agents[2] = "Firefox";
    agents[3] = "InternetExplorer";
    agents[4] = "Chrome";
    agents[5] = "Opera";
  }

  public static void main(String args[]) {

    // Configuring logger
    LogManager.getRootLogger().removeAllAppenders();
    ConsoleAppender console = new ConsoleAppender();
    final String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN));
    console.setThreshold(Level.INFO);
    console.activateOptions();
    Logger.getRootLogger().addAppender(console);

    findLatestImpressionId();

    int lambda = 6, generatedNumber;
    long milliseconds;
    final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    while(true) {
      generatedNumber = generatePoissonRandomNumber(lambda);
      milliseconds = (long)(generatedNumber * (Math.random() * 10 + 30) + 20);

      try {
        Thread.sleep(milliseconds);

        final String impression = generateImpression();
        final int impressionId = newImpressionId;

        logger.info("Attempting to send an ad impression with id " + impressionId + " to kafka...");
        producer.send(new ProducerRecord<String, String>("ad-impression", impressionId % numOfAdImpPartitions, Integer.toString(impressionId), impression), new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(recordMetadata != null) {
              logger.info("KafkaProducer has sent ad impression with id " + impressionId + ". Offset: " + recordMetadata.offset());
            } else {
              logger.info("KafkaProducer has failed to sent ad impression with id " + impressionId);
              logger.info("Record metadata is null");
            }
            if(e != null) {
              logger.info("Exception occurred: ");
              e.printStackTrace();
            }
            // Call a task to send a click event for this impression with possibility of 10%
            if(recordMetadata != null && Math.random() < 0.1) {
              // Starting task with random delay between 2 and 20 seconds
              long delay = Math.round(2000 + Math.random() * 18000);
              logger.info("Scheduling task for sending ad click for the impression with id " + impressionId + " in " + delay + " milliseconds.");
              scheduledExecutor.schedule(adClickSender(impression), delay, TimeUnit.MILLISECONDS);
            }
          }
        });

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static String generateImpression() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(IMPRESSION_ID + "=" + Integer.toString(++newImpressionId));
    stringBuilder.append(" " + TYPE + "=" + TYPE_IMPRESSION);
    stringBuilder.append(" " + ADVERTISER_ID + "=" + Integer.toString((int)Math.round(Math.random()*99 + 1)));
    stringBuilder.append(" " + IP + "=" + generateRandomIpAddress());
    stringBuilder.append(" " + AGENT + "=" + agents[(int)Math.round(Math.random()*5)]);
    Date timestamp = new Date();
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    stringBuilder.append(" " + TIMESTAMP + "=" + dateFormatter.format(timestamp));

    return stringBuilder.toString();
  }

  private static Runnable adClickSender(String impressionRaw) {
    Map<String, String> impression = null;
    try {
      impression = AdEventParser.parseAdEvent(impressionRaw);
    } catch (ParseException parEx) {
      parEx.printStackTrace();
    }
    final Map<String, String> fImpression = new HashMap<>(impression);

    Runnable adClickSenderRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          String impressionId =  fImpression.get(IMPRESSION_ID);
          logger.info("Attempting to send an ad click with impression id " + impressionId + " to kafka...");

          String clickEvent = generateClick(fImpression);

          RecordMetadata recordMetadata = producer.send(new ProducerRecord<String, String>("ad-click",
              Integer.parseInt(impressionId)%numOfAdClkPartitions,
              impressionId,
              clickEvent)).get();

          if(recordMetadata != null) {
            logger.info("KafkaProducer has sent ad click with impression id " + impressionId + " and offset " + recordMetadata.offset());
          } else {
            logger.info("KafkaProducer has failed to sent ad click with impression id " + impressionId);
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
    };

    return adClickSenderRunnable;
  }

  private static String generateClick(Map<String, String> forImpression) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(IMPRESSION_ID + "=" + forImpression.get(IMPRESSION_ID));
    stringBuilder.append(" " + TYPE + "=" + TYPE_CLICK);
    stringBuilder.append(" " + ADVERTISER_ID + "=" + forImpression.get(ADVERTISER_ID));
    stringBuilder.append(" " + IP + "=" + forImpression.get(IP));
    stringBuilder.append(" " + AGENT + "=" + forImpression.get(AGENT));
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    stringBuilder.append(" " + TIMESTAMP + "=" + dateFormatter.format(new Date()));

    return stringBuilder.toString();
  }

  /**
   * @return ip address in format "n.n.n.*" where n is random number between 0 and 255
   */
  private static String generateRandomIpAddress(){
    Integer ipNumberSegment;
    StringBuilder sb = new StringBuilder(13);
    for( int i = 0 ; i < 3 ; i++ ) {
      ipNumberSegment = (int)(Math.random()*255);
      sb.append(ipNumberSegment);
      sb.append(".");
    }
    sb.append("*");
    return sb.toString();
  }

  private static int generatePoissonRandomNumber(int lambda) {
    double l = Math.exp(-lambda);
    int k = 0;
    double p = 1;
    do{
      k++;
      p *= Math.random();
    } while (p > l);
    return k - 1;
  }

  /**
   * Checking for the latest impression present in kafka so we can set id for the new impression after latest id.
   * Required in case that user start producer multiple times so we don't start producing from id = 0 every time.
   */
  private static void findLatestImpressionId() {;
    producer = new KafkaProducer<String, String>(getProducerProperties());
    logger.info("Checking kafka for the latest ad-impression...");

    // Fetching partition metadata
    List<PartitionInfo> adImpPartitionInfo = producer.partitionsFor("ad-impression");
    numOfAdImpPartitions = adImpPartitionInfo.size();
    List<PartitionInfo> adClkPartitionInfo = producer.partitionsFor("ad-click");
    numOfAdClkPartitions = adClkPartitionInfo.size();

    // Sending request for every partition's latest offset
    Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo = new HashMap<>();
    for (int i = 0 ; i < numOfAdImpPartitions ; i++)
      offsetRequestInfo.put(new TopicAndPartition("ad-impression", i), new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
    OffsetRequest offsetRequest = new OffsetRequest(offsetRequestInfo, (short)0, kafka.api.OffsetRequest.DefaultClientId());
    SimpleConsumer consumer = new SimpleConsumer("localhost", 9092, 30000, 100000, "ad-event-consumer");
    OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

    // Making fetch requests for last message in every partition to see the latest impression-id
    long[] adImpressionPartitionOffsets = new long[numOfAdImpPartitions];
    Map<TopicAndPartition, PartitionFetchInfo> fetchRequestInfo = new HashMap<>(numOfAdImpPartitions);
    boolean topicHasMessages = false;
    for (int i = 0 ; i < numOfAdImpPartitions ; i++) {
      adImpressionPartitionOffsets[i] = offsetResponse.offsets("ad-impression", i)[0];
      if(adImpressionPartitionOffsets[i] > 0)
        topicHasMessages = true;
      fetchRequestInfo.put(new TopicAndPartition("ad-impression", i), new PartitionFetchInfo(adImpressionPartitionOffsets[i] - 1, 1024 * 1024));
    }

    // Fetching messages with latest offset in every partition
    int[] lastImpressionIds = new int[numOfAdImpPartitions];
    if(topicHasMessages) {
      FetchResponse fetchResponse = consumer.fetch(new FetchRequest(kafka.api.FetchRequest.DefaultCorrelationId(), consumer.clientId(), 10000, 1000, fetchRequestInfo));
      for (int i = 0; i < numOfAdImpPartitions; i++) {

        ByteBufferMessageSet messageSet = fetchResponse.messageSet("ad-impression", i);
        Iterator<MessageAndOffset> iter = messageSet.iterator();
        if (!iter.hasNext())
          lastImpressionIds[i] = 0;
        while (iter.hasNext()) {
          MessageAndOffset message = iter.next();
          if (!iter.hasNext()) {
            ByteBuffer keyBuffer = message.message().key();
            byte[] messageKey = Utils.toArray(keyBuffer);
            StringSerde stringSerde = new StringSerde("UTF-8");
            String impressionId = stringSerde.fromBytes(messageKey);
            lastImpressionIds[i] = Integer.parseInt(impressionId);
          }
        }
      }
    }

    // Setting next impression-id
    Arrays.sort(lastImpressionIds);
    newImpressionId = lastImpressionIds[numOfAdImpPartitions - 1];
    if(newImpressionId == 0) {
      logger.info("No ad-impressions found in kafka.");
    } else {
      logger.info("Starting producer. Latest ad-impression found with id: " + newImpressionId);
    }
  }

  private static Map<String, Object> getProducerProperties() {
    Map<String, Object> props = new HashMap<>();

    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "dummy-ad-producer");
    props.put("key.serializer", StringSerializer.class.getCanonicalName());
    props.put("value.serializer", StringSerializer.class.getCanonicalName());

    return props;
  }

}