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
package samza.examples.wikipedia.task.application;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;
import samza.examples.wikipedia.system.descriptors.WikipediaInputDescriptor;
import samza.examples.wikipedia.system.descriptors.WikipediaSystemDescriptor;
import samza.examples.wikipedia.task.WikipediaFeedStreamTask;


/**
 * This TaskApplication is responsible for consuming data from wikipedia, wiktionary, and wikinews data sources, and
 * merging them into a single Kafka topic called wikipedia-raw.
 *
 *
 */
public class WikipediaFeedTaskApplication implements TaskApplication {

  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  @Override
  public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {

    // Define a SystemDescriptor for Wikipedia data
    WikipediaSystemDescriptor wikipediaSystemDescriptor = new WikipediaSystemDescriptor("irc.wikimedia.org", 6667);

    // Define InputDescriptors for consuming wikipedia data
    WikipediaInputDescriptor wikipediaInputDescriptor =
        wikipediaSystemDescriptor.getInputDescriptor("en-wikipedia").withChannel("#en.wikipedia");
    WikipediaInputDescriptor wiktionaryInputDescriptor =
        wikipediaSystemDescriptor.getInputDescriptor("en-wiktionary").withChannel("#en.wiktionary");
    WikipediaInputDescriptor wikiNewsInputDescriptor =
        wikipediaSystemDescriptor.getInputDescriptor("en-wikinews").withChannel("#en.wikinews");

    // Define a system descriptor for Kafka, which is our output system
    KafkaSystemDescriptor kafkaSystemDescriptor =
        new KafkaSystemDescriptor("kafka").withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
            .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
            .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    // Define an output descriptor
    KafkaOutputDescriptor kafkaOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor("wikipedia-raw", new JsonSerde<>());

    // Set the default system descriptor to Kafka, so that it is used for all
    // internal resources, e.g., kafka topic for checkpointing, coordinator stream.
    taskApplicationDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    // Set the inputs
    taskApplicationDescriptor.withInputStream(wikipediaInputDescriptor);
    taskApplicationDescriptor.withInputStream(wiktionaryInputDescriptor);
    taskApplicationDescriptor.withInputStream(wikiNewsInputDescriptor);

    // Set the output
    taskApplicationDescriptor.withOutputStream(kafkaOutputDescriptor);

    // Set the task factory
    taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new WikipediaFeedStreamTask());
  }
}
