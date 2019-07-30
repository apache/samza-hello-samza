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

package samza.examples.wikipedia.application;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.AsyncFlatMapFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;
import samza.examples.wikipedia.system.descriptors.WikipediaInputDescriptor;
import samza.examples.wikipedia.system.descriptors.WikipediaSystemDescriptor;


/**
 * This {@link StreamApplication} demonstrates the Samza fluent API by performing the same operations as
 * {@link samza.examples.wikipedia.task.WikipediaFeedStreamTask},
 * {@link samza.examples.wikipedia.task.WikipediaParserStreamTask}, and
 * {@link samza.examples.wikipedia.task.WikipediaStatsStreamTask} in one expression.
 *
 * The only functional difference is the lack of "wikipedia-raw" and "wikipedia-edits"
 * streams to connect the operators, as they are not needed with the fluent API.
 *
 * The application processes Wikipedia events in the following steps:
 * <ul>
 *   <li>Merge wikipedia, wiktionary, and wikinews events into one stream</li>
 *   <li>Parse each event to a more structured format</li>
 *   <li>Aggregate some stats over a 10s window</li>
 *   <li>Format each window output for public consumption</li>
 *   <li>Send the window output to Kafka</li>
 * </ul>
 *
 */
public class WikipediaAsyncApplication implements StreamApplication {
  private static final Logger log = LoggerFactory.getLogger(WikipediaAsyncApplication.class);

  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  public static final String WIKIPEDIA_CHANNEL = "#en.wikipedia";
  public static final String WIKINEWS_CHANNEL = "#en.wikinews";
  public static final String WIKTIONARY_CHANNEL = "#en.wiktionary";

  @Override
  @SuppressWarnings("Duplicates")
  public void describe(StreamApplicationDescriptor appDescriptor) {
    // Define a SystemDescriptor for Wikipedia data
    WikipediaSystemDescriptor wikipediaSystemDescriptor = new WikipediaSystemDescriptor("irc.wikimedia.org", 6667);

    // Define InputDescriptors for consuming wikipedia data
    WikipediaInputDescriptor wikipediaInputDescriptor = wikipediaSystemDescriptor
        .getInputDescriptor("en-wikipedia")
        .withChannel(WIKIPEDIA_CHANNEL);
    WikipediaInputDescriptor wiktionaryInputDescriptor = wikipediaSystemDescriptor
        .getInputDescriptor("en-wiktionary")
        .withChannel(WIKTIONARY_CHANNEL);
    WikipediaInputDescriptor wikiNewsInputDescriptor = wikipediaSystemDescriptor
        .getInputDescriptor("en-wikinews")
        .withChannel(WIKINEWS_CHANNEL);

    // Define a system descriptor for Kafka
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor("kafka")
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    // Define an output descriptor
    KafkaOutputDescriptor<WikipediaFeedEvent> statsOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor("wikipedia-stats-3", new JsonSerdeV2<>(
            WikipediaFeedEvent.class));

    // Messages come from WikipediaConsumer so we know the type is WikipediaFeedEvent
    MessageStream<WikipediaFeedEvent> wikipediaEvents = appDescriptor.getInputStream(wikipediaInputDescriptor);
    MessageStream<WikipediaFeedEvent> wiktionaryEvents = appDescriptor.getInputStream(wiktionaryInputDescriptor);
    MessageStream<WikipediaFeedEvent> wikiNewsEvents = appDescriptor.getInputStream(wikiNewsInputDescriptor);

    // Output (also un-keyed)
    OutputStream<WikipediaFeedEvent> wikipediaStats =
        appDescriptor.getOutputStream(statsOutputDescriptor);

    // Merge inputs
    MessageStream<WikipediaFeedEvent> allWikipediaEvents =
        MessageStream.mergeAll(ImmutableList.of(wikipediaEvents, wiktionaryEvents, wikiNewsEvents));

    // Parse, update stats, prepare output, and send
    allWikipediaEvents
        .flatMapAsync(new MyAsyncFlatMapFunction())
        .sendTo(wikipediaStats);
  }

  /**
   * A sample async map function to mimic asynchronous behavior in the pipeline.
   * In a real world example this would be replaced with remote IO.
   */
  static class MyAsyncFlatMapFunction implements AsyncFlatMapFunction<WikipediaFeedEvent, WikipediaFeedEvent> {
    @Override
    public CompletionStage<Collection<WikipediaFeedEvent>> apply(WikipediaFeedEvent wikipediaFeedEvent) {
      return CompletableFuture.supplyAsync(() -> {
        log.debug("Executing filter function for {}", wikipediaFeedEvent.getChannel());
        boolean res;
        try {
          Thread.sleep((long) (Math.random() * 10000));
          res = Math.random() > 0.5;
        } catch(InterruptedException ec) {
          res = false;
        }

        log.debug("Finished executing filter function for {} with result {}.", wikipediaFeedEvent.getChannel(), res);

        return res ? Collections.singleton(wikipediaFeedEvent) : Collections.emptyList();
      });
    }
  }


  /**
   * Executes the application using the local application runner.
   * It takes two required command line arguments
   *  config-factory: a fully {@link org.apache.samza.config.factories.PropertiesConfigFactory} class name
   *  config-path: path to application properties
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config config = cmdLine.loadConfig(options);

    WikipediaAsyncApplication app = new WikipediaAsyncApplication();
    LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    runner.run();
    runner.waitForFinish();
  }
}

