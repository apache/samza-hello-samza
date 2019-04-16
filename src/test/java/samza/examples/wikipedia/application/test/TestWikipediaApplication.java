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

package samza.examples.wikipedia.application.test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;
import samza.examples.wikipedia.application.WikipediaApplication;
import samza.examples.test.utils.TestUtils;


public class TestWikipediaApplication {

  @Test
  public void testWikipediaApplication() throws Exception {

    InMemorySystemDescriptor wikipediaSystemDescriptor = new InMemorySystemDescriptor("wikipedia");

    // These config must be removed once examples are refactored to use Table-API
    Map<String, String> conf = new HashMap<>();
    conf.put("stores.wikipedia-stats.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
    conf.put("stores.wikipedia-stats.key.serde", "string");
    conf.put("stores.wikipedia-stats.msg.serde", "integer");
    conf.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
    conf.put("serializers.registry.integer.class", "org.apache.samza.serializers.IntegerSerdeFactory");

    InMemoryInputDescriptor wikipediaInputDescriptor = wikipediaSystemDescriptor
        .getInputDescriptor("en-wikipedia", new NoOpSerde<>())
        .withPhysicalName(WikipediaApplication.WIKIPEDIA_CHANNEL);

    InMemoryInputDescriptor wiktionaryInputDescriptor = wikipediaSystemDescriptor
        .getInputDescriptor("en-wiktionary", new NoOpSerde<>())
        .withPhysicalName(WikipediaApplication.WIKTIONARY_CHANNEL);

    InMemoryInputDescriptor wikiNewsInputDescriptor = wikipediaSystemDescriptor
        .getInputDescriptor("en-wikinews", new NoOpSerde<>())
        .withPhysicalName(WikipediaApplication.WIKINEWS_CHANNEL);

    InMemorySystemDescriptor kafkaSystemDescriptor = new InMemorySystemDescriptor("kafka");

    InMemoryOutputDescriptor outputStreamDesc = kafkaSystemDescriptor
        .getOutputDescriptor("wikipedia-stats", new NoOpSerde<>());


    TestRunner
        .of(new WikipediaApplication())
        .addInputStream(wikipediaInputDescriptor, TestUtils.genWikipediaFeedEvents(WikipediaApplication.WIKIPEDIA_CHANNEL))
        .addInputStream(wiktionaryInputDescriptor, TestUtils.genWikipediaFeedEvents(WikipediaApplication.WIKTIONARY_CHANNEL))
        .addInputStream(wikiNewsInputDescriptor, TestUtils.genWikipediaFeedEvents(WikipediaApplication.WIKINEWS_CHANNEL))
        .addOutputStream(outputStreamDesc, 1)
        .addConfig(conf)
        .addConfig("deploy.test", "true")
        .run(Duration.ofMinutes(1));

    Assert.assertTrue(TestRunner.consumeStream(outputStreamDesc, Duration.ofMillis(100)).get(0).size() > 0);
  }

}
