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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samza.examples.wikipedia.model.WikipediaParser;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;


public class WikipediaApplication implements StreamApplication {
  private static final Logger log = LoggerFactory.getLogger(WikipediaApplication.class);

  private static final String STATS_STORE_NAME = "wikipedia-stats";
  private static final String EDIT_COUNT_KEY = "count-edits-all-time";

  private static final String WIKIPEDIA_STREAM_ID = "en-wikipedia";
  private static final String WIKTIONARY_STREAM_ID = "en-wiktionary";
  private static final String WIKINEWS_STREAM_ID = "en-wikinews";
  private static final String STATS_STREAM_ID = "wikipedia-stats";

  @Override
  public void init(StreamGraph graph, Config config) {
    // Inputs
    // Messages come from WikipediaConsumer so we know the type is WikipediaFeedEvent
    // They are un-keyed, so the 'k' parameter to the msgBuilder is not used
    MessageStream<WikipediaFeedEvent> wikipediaEvents = graph.getInputStream(WIKIPEDIA_STREAM_ID, (k, v) -> (WikipediaFeedEvent) v);
    MessageStream<WikipediaFeedEvent> wiktionaryEvents = graph.getInputStream(WIKTIONARY_STREAM_ID, (k, v) -> (WikipediaFeedEvent) v);
    MessageStream<WikipediaFeedEvent> wikiNewsEvents = graph.getInputStream(WIKINEWS_STREAM_ID, (k, v) -> (WikipediaFeedEvent) v);

    // Output (also un-keyed, so no keyExtractor)
    OutputStream<Void, Map<String, Integer>, Map<String, Integer>> wikipediaStats = graph.getOutputStream(STATS_STREAM_ID, m -> null, m -> m);

    // Merge inputs
    MessageStream<WikipediaFeedEvent> allWikipediaEvents = wikipediaEvents.merge(new ArrayList(Arrays.asList(new MessageStream[]{wiktionaryEvents, wikiNewsEvents})));

    // Parse, update stats, prepare output, and send
    allWikipediaEvents.map(WikipediaParser::parseEvent)
        .window(Windows.tumblingWindow(Duration.ofSeconds(10), WindowedStats::new, new WikipediaWindow()))
        .map(this::formatOutput)
        .sendTo(wikipediaStats);
  }

  /**
   * A few statistics to keep for each window.
   */
  private static class WindowedStats {
    int edits = 0;
    int byteDiff = 0;
    Set<String> titles = new HashSet<String>();
    Map<String, Integer> counts = new HashMap<String, Integer>();

    int totalEdits = 0;

    @Override
    public String toString() {
      return String.format("Stats {edits:%d, byteDiff:%d, titles:%s, counts:%s}", edits, byteDiff, titles, counts);
    }
  }

  /**
   *
   */
  private class WikipediaWindow implements FoldLeftFunction<Map<String, Object>, WindowedStats> {

    private KeyValueStore<String, Integer> store;

    @Override
    public void init(Config config, TaskContext context) {
      store = (KeyValueStore<String, Integer>) context.getStore(STATS_STORE_NAME);
    }

    @Override
    public WindowedStats apply(Map<String, Object> edit, WindowedStats stats) {
      log.debug("In window.apply");

      Integer totalEdits = store.get(EDIT_COUNT_KEY);
      if (totalEdits == null) {
        totalEdits = 0;
      }
      log.debug("Before window: " + stats.toString());
      log.debug("Before total: " + totalEdits);

      totalEdits++;
      store.put(EDIT_COUNT_KEY, totalEdits);
      stats.edits++;
      stats.titles.add((String) edit.get("title"));
      stats.byteDiff += (Integer) edit.get("diff-bytes");
      stats.totalEdits = totalEdits;

      Map<String, Boolean> flags = (Map<String, Boolean>) edit.get("flags");
      for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
        if (Boolean.TRUE.equals(flag.getValue())) {
          stats.counts.compute(flag.getKey(), (k, v) -> v == null ? 0 : v + 1);
        }
      }

      log.debug("After window: " + stats.toString());
      log.debug("After total: " + totalEdits);
      return stats;
    }
  }

  /**
   *
   */
  private Map<String, Integer> formatOutput(WindowPane<Void, WindowedStats> statsWindowPane) {
    log.debug("In outputfn.apply");

    WindowedStats stats = statsWindowPane.getMessage();

    Map<String, Integer> counts = new HashMap<String, Integer>();
    counts.put("edits", stats.edits);
    counts.put("bytes-added", stats.byteDiff);
    counts.put("unique-titles", stats.titles.size());
    counts.put("edits-all-time", stats.totalEdits);

    log.debug("window: " + stats.toString());
    log.debug("total: " + stats.totalEdits);
    return counts;
  }
}

