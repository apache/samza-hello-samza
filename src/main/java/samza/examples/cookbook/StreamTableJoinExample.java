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

import java.util.Objects;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import samza.examples.cookbook.data.PageView;
import samza.examples.cookbook.data.Profile;

import java.util.List;
import java.util.Map;

/**
 * In this example, we join a stream of Page views with a table of user profiles, which is populated from an
 * user profile stream. For instance, this is helpful for analysis that required additional information from
 * user's profile.
 *
 * <p> Concepts covered: Performing stream-to-table joins.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Ensure that the topics "pageview-join-input", "profile-table-input" are created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic pageview-join-input --partitions 2 --replication-factor 1
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic profile-table-input --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-path=$PWD/deploy/samza/config/stream-table-join-example.properties
 *   </li>
 *   <li>
 *     Consume messages from the "enriched-pageview-join-output" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic enriched-pageview-join-output
 *   </li>
 *   <li>
 *     Produce some messages to the "profile-table-input" topic with the same userId <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic profile-table-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "company": "LNKD"} <br/>
 *     {"userId": "user2", "company": "MSFT"}
 *   </li>
 *   <li>
 *     Produce some messages to the "pageview-join-input" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-join-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com"}
 *   </li>
 * </ol>
 *
 */
public class StreamTableJoinExample implements StreamApplication {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String PROFILE_STREAM_ID = "profile-table-input";
  private static final String PAGEVIEW_STREAM_ID = "pageview-join-input";
  private static final String OUTPUT_TOPIC = "enriched-pageview-join-output";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    Serde<Profile> profileSerde = new JsonSerdeV2<>(Profile.class);
    Serde<PageView> pageViewSerde = new JsonSerdeV2<>(PageView.class);
    Serde<EnrichedPageView> joinResultSerde = new JsonSerdeV2<>(EnrichedPageView.class);

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<Profile> profileInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(PROFILE_STREAM_ID, profileSerde);
    KafkaInputDescriptor<PageView> pageViewInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(PAGEVIEW_STREAM_ID, pageViewSerde);
    KafkaOutputDescriptor<EnrichedPageView> joinResultOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_TOPIC, joinResultSerde);

    RocksDbTableDescriptor<String, Profile> profileTableDescriptor =
        new RocksDbTableDescriptor<String, Profile>("profile-table", KVSerde.of(new StringSerde(), profileSerde));

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    MessageStream<Profile> profileStream = appDescriptor.getInputStream(profileInputDescriptor);
    MessageStream<PageView> pageViewStream = appDescriptor.getInputStream(pageViewInputDescriptor);
    OutputStream<EnrichedPageView> joinResultStream = appDescriptor.getOutputStream(joinResultOutputDescriptor);
    Table<KV<String, Profile>> profileTable = appDescriptor.getTable(profileTableDescriptor);

    profileStream
        .map(profile -> KV.of(profile.userId, profile))
        .sendTo(profileTable);

    pageViewStream
        .partitionBy(pv -> pv.userId, pv -> pv, KVSerde.of(new StringSerde(), pageViewSerde), "join")
        .join(profileTable, new JoinFn())
        .sendTo(joinResultStream);
  }

  private static class JoinFn implements StreamTableJoinFunction<String, KV<String, PageView>, KV<String, Profile>, EnrichedPageView> {
    @Override
    public EnrichedPageView apply(KV<String, PageView> message, KV<String, Profile> record) {
      return record == null ? null :
          new EnrichedPageView(message.getKey(), record.getValue().company, message.getValue().pageId);
    }
    @Override
    public String getMessageKey(KV<String, PageView> message) {
      return message.getKey();
    }
    @Override
    public String getRecordKey(KV<String, Profile> record) {
      return record.getKey();
    }
  }

  static public class EnrichedPageView {

    public final String userId;
    public final String company;
    public final String pageId;

    public EnrichedPageView(String userId, String company, String pageId) {
      this.userId = userId;
      this.company = company;
      this.pageId = pageId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EnrichedPageView that = (EnrichedPageView) o;
      return Objects.equals(userId, that.userId) && Objects.equals(company, that.company) && Objects.equals(pageId,
          that.pageId);
    }
  }

}
