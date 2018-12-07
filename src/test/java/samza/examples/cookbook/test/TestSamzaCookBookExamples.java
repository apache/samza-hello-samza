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
package samza.examples.cookbook.test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.StreamAssert;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;
import samza.examples.cookbook.FilterExample;
import samza.examples.cookbook.JoinExample;
import samza.examples.cookbook.SessionWindowExample;
import samza.examples.cookbook.StreamTableJoinExample;
import samza.examples.cookbook.TumblingWindowExample;
import samza.examples.cookbook.data.AdClick;
import samza.examples.cookbook.data.PageView;
import samza.examples.cookbook.data.Profile;
import samza.examples.cookbook.data.UserPageViews;
import samza.examples.test.utils.TestUtils;

import static samza.examples.cookbook.StreamTableJoinExample.EnrichedPageView;


public class TestSamzaCookBookExamples {
  @Test
  public void testFilterExample() {
    List<PageView> rawPageViewEvents = new ArrayList<>();
    rawPageViewEvents.add(new PageView("google.com", "user1", "india"));
    rawPageViewEvents.add(new PageView("facebook.com", "invalidUserId", "france"));
    rawPageViewEvents.add(new PageView("yahoo.com", "user2", "china"));

    InMemorySystemDescriptor inMemorySystem = new InMemorySystemDescriptor("kafka");

    InMemoryInputDescriptor<PageView> badPageViewEvents =
        inMemorySystem.getInputDescriptor("pageview-filter-input", new NoOpSerde<PageView>());

    InMemoryOutputDescriptor<PageView> goodPageViewEvents =
        inMemorySystem.getOutputDescriptor("pageview-filter-output", new NoOpSerde<PageView>());

    TestRunner
        .of(new FilterExample())
        .addInputStream(badPageViewEvents, rawPageViewEvents)
        .addOutputStream(goodPageViewEvents, 1)
        .run(Duration.ofMillis(1500));

    Assert.assertEquals(TestRunner.consumeStream(goodPageViewEvents, Duration.ofMillis(1000)).get(0).size(), 2);
  }

  @Test
  public void testJoinExample() {
    List<PageView> pageViewEvents = new ArrayList<>();
    pageViewEvents.add(new PageView("google.com", "user1", "india"));
    pageViewEvents.add(new PageView("yahoo.com", "user2", "china"));
    List<AdClick> adClickEvents = new ArrayList<>();
    adClickEvents.add(new AdClick("google.com", "adClickId1", "user1"));
    adClickEvents.add(new AdClick("yahoo.com", "adClickId2", "user1"));

    InMemorySystemDescriptor inMemorySystem = new InMemorySystemDescriptor("kafka");

    InMemoryInputDescriptor<PageView> pageViews =
        inMemorySystem.getInputDescriptor("pageview-join-input", new NoOpSerde<PageView>());

    InMemoryInputDescriptor<AdClick> adClicks =
        inMemorySystem.getInputDescriptor("adclick-join-input", new NoOpSerde<AdClick>());

    InMemoryOutputDescriptor pageViewAdClickJoin =
        inMemorySystem.getOutputDescriptor("pageview-adclick-join-output", new NoOpSerde<>());

    TestRunner
        .of(new JoinExample())
        .addInputStream(pageViews, pageViewEvents)
        .addInputStream(adClicks, adClickEvents)
        .addOutputStream(pageViewAdClickJoin, 1)
        .run(Duration.ofMillis(1500));

    Assert.assertEquals(TestRunner.consumeStream(pageViewAdClickJoin, Duration.ofMillis(1000)).get(0).size(), 2);
  }

  @Test
  public void testTumblingWindowExample() {
    List<PageView> pageViewEvents = TestUtils.genSamplePageViewData();

    InMemorySystemDescriptor inMemorySystem = new InMemorySystemDescriptor("kafka");

    InMemoryInputDescriptor<KV<String, PageView>> pageViewInputDescriptor =
        inMemorySystem.getInputDescriptor("pageview-tumbling-input", new NoOpSerde<KV<String, PageView>>());

    InMemoryOutputDescriptor<KV<String, UserPageViews>> userPageViewOutputDescriptor =
        inMemorySystem.getOutputDescriptor("pageview-tumbling-output", new NoOpSerde<KV<String, UserPageViews>>());

    TestRunner
        .of(new TumblingWindowExample())
        .addInputStream(pageViewInputDescriptor, pageViewEvents)
        .addOutputStream(userPageViewOutputDescriptor, 1)
        .run(Duration.ofMinutes(1));

    Assert.assertTrue(TestRunner.consumeStream(userPageViewOutputDescriptor, Duration.ofMillis(1000)).get(0).size() > 1);
  }

  @Test
  public void testSessionWindowExample() {
    List<PageView> pageViewEvents = TestUtils.genSamplePageViewData();

    InMemorySystemDescriptor inMemorySystem = new InMemorySystemDescriptor("kafka");

    InMemoryInputDescriptor<KV<String, PageView>> pageViewInputDescriptor =
        inMemorySystem.getInputDescriptor("pageview-session-input", new NoOpSerde<KV<String, PageView>>());

    InMemoryOutputDescriptor<KV<String, UserPageViews>> userPageViewOutputDescriptor =
        inMemorySystem.getOutputDescriptor("pageview-session-output", new NoOpSerde<KV<String, UserPageViews>>());

    TestRunner
        .of(new SessionWindowExample())
        .addInputStream(pageViewInputDescriptor, pageViewEvents)
        .addOutputStream(userPageViewOutputDescriptor, 1)
        .run(Duration.ofMinutes(1));

    Assert.assertEquals(2, TestRunner.consumeStream(userPageViewOutputDescriptor, Duration.ofMillis(1000)).get(0).size());
  }

  @Test
  public void testStreamTableJoinExample() throws InterruptedException{
    List<PageView> pageViewEvents = new ArrayList<>();
    pageViewEvents.add(new PageView("google.com", "user1", "india"));
    pageViewEvents.add(new PageView("yahoo.com", "user2", "china"));
    List<Profile> profiles = new ArrayList<>();
    profiles.add(new Profile("user1", "LNKD"));
    profiles.add(new Profile("user2", "MSFT"));

    InMemorySystemDescriptor inMemorySystem = new InMemorySystemDescriptor("kafka");

    InMemoryInputDescriptor<PageView> pageViews =
        inMemorySystem.getInputDescriptor("pageview-join-input", new NoOpSerde<PageView>());

    InMemoryInputDescriptor<Profile> profileViews =
        inMemorySystem.getInputDescriptor("profile-table-input", new NoOpSerde<Profile>());

    InMemoryOutputDescriptor<EnrichedPageView> joinResultOutputDescriptor =
        inMemorySystem.getOutputDescriptor("enriched-pageview-join-output", new NoOpSerde<EnrichedPageView>());

    TestRunner
        .of(new StreamTableJoinExample())
        .addInputStream(pageViews, pageViewEvents)
        .addInputStream(profileViews, profiles)
        .addOutputStream(joinResultOutputDescriptor, 1)
        .run(Duration.ofMillis(1500));

    List<EnrichedPageView> expectedOutput = new ArrayList<>();
    expectedOutput.add(new EnrichedPageView("user1", "LNKD", "google.com"));
    expectedOutput.add(new EnrichedPageView("user2", "MSFT", "yahoo.com"));

    StreamAssert.containsInAnyOrder(expectedOutput, joinResultOutputDescriptor, Duration.ofMillis(200));

  }

}
