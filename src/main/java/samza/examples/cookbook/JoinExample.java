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
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import samza.examples.cookbook.data.AdClick;
import samza.examples.cookbook.data.PageView;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * In this example, we join a stream of Page views with a stream of Ad clicks. For instance, this is helpful for
 * analysis on what pages served an Ad that was clicked.
 *
 * <p> Concepts covered: Performing stream to stream Joins.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Ensure that the topics "pageview-join-input", "adclick-join-input" are created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic pageview-join-input --partitions 2 --replication-factor 1
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic adclick-join-input --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-path=$PWD/deploy/samza/config/join-example.properties
 *   </li>
 *   <li>
 *     Produce some messages to the "pageview-join-input" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-join-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com"}
 *   </li>
 *   <li>
 *     Produce some messages to the "adclick-join-input" topic with the same pageKey <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic adclick-join-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "adId": "adClickId1", "pageId":"google.com"} <br/>
 *     {"userId": "user1", "adId": "adClickId2", "pageId":"yahoo.com"}
 *   </li>
 *   <li>
 *     Consume messages from the "pageview-adclick-join-output" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pageview-adclick-join-output --property print.key=true
 *   </li>
 * </ol>
 *
 */
public class JoinExample implements StreamApplication, Serializable {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String PAGEVIEW_STREAM_ID = "pageview-join-input";
  private static final String ADCLICK_STREAM_ID = "adclick-join-input";
  private static final String OUTPUT_STREAM_ID = "pageview-adclick-join-output";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    StringSerde stringSerde = new StringSerde();
    JsonSerdeV2<PageView> pageViewSerde = new JsonSerdeV2<>(PageView.class);
    JsonSerdeV2<AdClick> adClickSerde = new JsonSerdeV2<>(AdClick.class);
    JsonSerdeV2<JoinResult> joinResultSerde = new JsonSerdeV2<>(JoinResult.class);

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<PageView> pageViewInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(PAGEVIEW_STREAM_ID, pageViewSerde);
    KafkaInputDescriptor<AdClick> adClickInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(ADCLICK_STREAM_ID, adClickSerde);
    KafkaOutputDescriptor<JoinResult> joinResultOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, joinResultSerde);

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    MessageStream<PageView> pageViews = appDescriptor.getInputStream(pageViewInputDescriptor);
    MessageStream<AdClick> adClicks = appDescriptor.getInputStream(adClickInputDescriptor);
    OutputStream<JoinResult> joinResults = appDescriptor.getOutputStream(joinResultOutputDescriptor);

    JoinFunction<String, PageView, AdClick, JoinResult> pageViewAdClickJoinFunction =
        new JoinFunction<String, PageView, AdClick, JoinResult>() {
          @Override
          public JoinResult apply(PageView pageView, AdClick adClick) {
            return new JoinResult(pageView.pageId, pageView.userId, pageView.country, adClick.getAdId());
          }

          @Override
          public String getFirstKey(PageView pageView) {
            return pageView.pageId;
          }

          @Override
          public String getSecondKey(AdClick adClick) {
            return adClick.getPageId();
          }
        };

    MessageStream<PageView> repartitionedPageViews =
        pageViews
            .partitionBy(pv -> pv.pageId, pv -> pv, KVSerde.of(stringSerde, pageViewSerde), "pageview")
            .map(KV::getValue);

    MessageStream<AdClick> repartitionedAdClicks =
        adClicks
            .partitionBy(AdClick::getPageId, ac -> ac, KVSerde.of(stringSerde, adClickSerde), "adclick")
            .map(KV::getValue);

    repartitionedPageViews
        .join(repartitionedAdClicks, pageViewAdClickJoinFunction,
            stringSerde, pageViewSerde, adClickSerde, Duration.ofMinutes(3), "join")
        .sendTo(joinResults);
  }

  static class JoinResult {
    public String pageId;
    public String userId;
    public String country;
    public String adId;

    public JoinResult(String pageId, String userId, String country, String adId) {
      this.pageId = pageId;
      this.userId = userId;
      this.country = country;
      this.adId = adId;
    }
  }
}
