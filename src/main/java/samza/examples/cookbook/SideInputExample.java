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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
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
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.TableDescriptor;
import samza.examples.cookbook.data.PageView;
import samza.examples.cookbook.data.Profile;


/**
 * In this example, we demonstrate stream table join with the source of data for table coming from side inputs.
 * The side inputs are no different than a normal stream except they act as the source to populate the table for your
 * application. Additionally, you should supply a {@link org.apache.samza.storage.SideInputsProcessorFactory} which is used
 * to instantiate {@link org.apache.samza.storage.SideInputsProcessor} in runtime and applied on the source message prior to
 * writing it to the table.
 *
 * <p>
 *   You can specify the side inputs and the processor factory using the TableDescriptor as follows
 *   <pre>
 *     TableDescriptor<K,V,?> myTableDescriptor = new RocksDbTableDescriptor<>(...)
 *           .withSideInputs(ImmutableList.of("source1"))
 *           .withSideInputsProcessor((message, store) -> {
 *             K key = message.getKey(); // extract the key from the message
 *             V value = (V) message.getValue(); // extract the payload from the message
 *
 *             ... // any additional logic to enrich, project data prior to writing it to the table
 *
 *             return new Entry<>(key, value); // return the entry that will be written to the table
 *           });
 *   </pre>
 * </p>
 *
 * To run the example:
 *  <ol>
 *    <li>
 *      Ensure the topic pageview-input and member-profile-side-input is created <br/>
 *      ./deploy/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic pageview-input --partitions 2 --replication-factor 1
 *      ./deploy/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic member-profile-side-input --partitions 2 --replication-factor 1
 *    </li>
 *    <li>
 *      Produce some messages to the "member-profile-side-input" topic, waiting for some time between messages <br/>
 *      ./deploy/kafka/bin/kafka-console-producer.sh --topic member-profile-side-input --broker-list localhost:9092 <br/>
 *      {"userId": "bob", "company": "IBM"} <br/>
 *      {"userId": "alice", "company": "DB"} <br/>
 *      {"userId": "ron", "company": "KLM"}
 *    </li>
 *    <li>
 *      Run the application using run-app.sh script <br/>
 *      ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/side-input-example.properties
 *    </li>
 *    <li>
 *      Produce some messages to the "pageview-input" topic, waiting for some time between messages <br/>
 *       ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-input --broker-list localhost:9092 <br/>
 *       {"userId": "bob", "country": "finland", "pageId":"google.com/home"} <br/>
 *       {"userId": "ron", "country": "finland", "pageId":"google.com/search"} <br/>
 *       {"userId": "alice", "country": "germany", "pageId":"yahoo.com/home"} <br/>
 *       {"userId": "ron", "country": "germany", "pageId":"yahoo.com/sports"} <br/>
 *       {"userId": "bob", "country": "finland", "pageId":"google.com/news"} <br/>
 *       {"userId": "ron", "country": "germany", "pageId":"yahoo.com/fashion"}
 *    </li>
 *    <li>
 *      Consume messages from the "user-pageview-output" topic (e.g. bin/kafka-console-consumer.sh)
 *      ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic member-pageview-output --property print.key=true <br/>
 *    </li>
 *  </ol>
 */
public class SideInputExample implements StreamApplication {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String PAGEVIEW_SIDE_INPUT = "pageview-input";
  private static final String MEMBER_PROFILE_INPUT = "member-profile-side-input";
  private static final String PROFILE_PAGEVIEW_OUTPUT = "member-pageview-output";
  private static final String PROFILE_TABLE = "member-profile-table";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME).withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    Serde<PageView> pageViewSerde = new JsonSerdeV2<>(PageView.class);
    KVSerde<String, DecoratedPageView> userPageViewSerde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>(DecoratedPageView.class));

    KafkaInputDescriptor<PageView> memberProfileInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(PAGEVIEW_SIDE_INPUT, pageViewSerde);
    KafkaOutputDescriptor<KV<String, DecoratedPageView>> userPageViewOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(PROFILE_PAGEVIEW_OUTPUT, userPageViewSerde);

    TableDescriptor<String, Profile, ?> profileTableDescriptor =
        new RocksDbTableDescriptor<>(PROFILE_TABLE, KVSerde.of(new StringSerde(), new JsonSerdeV2<>(Profile.class))).withSideInputs(
            ImmutableList.of(MEMBER_PROFILE_INPUT)).withSideInputsProcessor((msg, store) -> {
          Profile profile = new Profile((Map<String, Object>)msg.getMessage());
          String key = profile.userId;
          return ImmutableList.of(new Entry<>(key, profile));
        });

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    MessageStream<PageView> pageViews = appDescriptor.getInputStream(memberProfileInputDescriptor);
    Table<KV<String, Profile>> profileTable = appDescriptor.getTable(profileTableDescriptor);
    OutputStream<KV<String, DecoratedPageView>> outputStream = appDescriptor.getOutputStream(userPageViewOutputDescriptor);


    pageViews
        .map(pageView -> KV.of(pageView.userId, pageView))
        .join(profileTable, new PageviewProfileTableJoinFunction())
        .map(decoratedPageView -> KV.of(decoratedPageView.getUserId(), decoratedPageView))
        .sendTo(outputStream);
  }

  private static class PageviewProfileTableJoinFunction implements StreamTableJoinFunction<String, KV<String, PageView>, KV<String, Profile>, DecoratedPageView> {
    @Override
    public DecoratedPageView apply(KV<String, PageView> pageView, KV<String, Profile> profileRecord) {
      return profileRecord == null ? null : new DecoratedPageView(profileRecord.value.userId, profileRecord.value.company, pageView.value.pageId);
    }

    @Override
    public String getMessageKey(KV<String, PageView> pageView) {
      return pageView.getKey();
    }

    @Override
    public String getRecordKey(KV<String, Profile> profileRecord) {
      return profileRecord.getKey();
    }
  }

  private static class DecoratedPageView {
    private final String userId;
    private final String company;
    private final String pageName;

    public DecoratedPageView(String userId, String company, String pageName) {
      this.userId = userId;
      this.company = company;
      this.pageName = pageName;
    }

    public String getUserId() {
      return userId;
    }

    public String getCompany() {
      return company;
    }

    public String getPageName() {
      return pageName;
    }
  }

}
