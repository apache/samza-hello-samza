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
package samza.examples.cookbook;

import java.io.Serializable;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import samza.examples.cookbook.data.PageView;
import samza.examples.cookbook.data.UserPageViews;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * In this example, we group a stream of page views by country, and compute the number of page views over a tumbling time
 * window.
 *
 * <p> Concepts covered: Performing Group-By style aggregations on tumbling time windows.
 *
 * <p> Tumbling windows divide a stream into a set of contiguous, fixed-sized, non-overlapping time intervals.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Ensure that the topic "pageview-tumbling-input" is created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic pageview-tumbling-input --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-path=$PWD/deploy/samza/config/tumbling-window-example.properties
 *   </li>
 *   <li>
 *     Produce some messages to the "pageview-tumbling-input" topic, waiting for some time between messages <br/>
       ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-tumbling-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com/home"} <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com/search"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com/home"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com/sports"} <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com/news"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com/fashion"}
 *   </li>
 *   <li>
 *     Consume messages from the "pageview-tumbling-output" topic (e.g. bin/kafka-console-consumer.sh)
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pageview-tumbling-output --property print.key=true <br/>
 *   </li>
 * </ol>
 *
 */
public class TumblingWindowExample implements StreamApplication, Serializable {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "pageview-tumbling-input";
  private static final String OUTPUT_STREAM_ID = "pageview-tumbling-output";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KVSerde<String, PageView> pageViewSerde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageView.class));
    KVSerde<String, UserPageViews> userPageViewSerde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>(UserPageViews.class));

    KafkaInputDescriptor<KV<String, PageView>> pageViewInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, pageViewSerde);
    KafkaOutputDescriptor<KV<String, UserPageViews>> userPageViewOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, userPageViewSerde);

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);
    MessageStream<KV<String, PageView>> pageViews = appDescriptor.getInputStream(pageViewInputDescriptor);
    OutputStream<KV<String, UserPageViews>> outputStream = appDescriptor.getOutputStream(userPageViewOutputDescriptor);

    pageViews
        .partitionBy(kv -> kv.value.userId, kv -> kv.value, pageViewSerde, "userId")
        .window(Windows.keyedTumblingWindow(
            kv -> kv.key, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
            new StringSerde(), new IntegerSerde()), "count")
        .map(windowPane -> {
          String userId = windowPane.getKey().getKey();
          int views = windowPane.getMessage();
          return KV.of(userId, new UserPageViews(userId, views));
        })
        .sendTo(outputStream);
  }
}
