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

package samza.examples.wikipedia.task;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;

public class WikipediaElasticStreamTask implements StreamTask {

  // This is the index we intend to submit to.
  public static final String ELASTICSEARCH_INDEX = "samza";

  // This is the type that will be used under _type
  public static final String ELASTICSEARCH_TYPE = "wikiedit";

  @SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    Map<String, Object> jsonObject = (Map<String, Object>) envelope.getMessage();
    WikipediaFeedEvent event = new WikipediaFeedEvent(jsonObject);

    try {
      Map<String, Object> parsedJsonObject = parse(event.getRawEvent());

      parsedJsonObject.put("channel", event.getChannel());
      parsedJsonObject.put("source", event.getSource());
      parsedJsonObject.put("time", event.getTime());

      collector.send(new OutgoingMessageEnvelope(new SystemStream("elasticsearch", ELASTICSEARCH_INDEX + "/" + ELASTICSEARCH_TYPE), parsedJsonObject));
    } catch (Exception e) {
      System.err.println("Unable to parse line: " + event);
    }
  }

  public static Map<String, Object> parse(String line) {
    Pattern p = Pattern.compile("\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)");
    Matcher m = p.matcher(line);

    if (m.find() && m.groupCount() == 6) {
      int byteDiff = Integer.parseInt(m.group(5));
      Map<String, Object> root = new HashMap<String, Object>();

      root.put("title", m.group(1));
      root.put("user", m.group(4));
      root.put("unparsed-flags", m.group(2));
      root.put("diff-bytes", byteDiff);
      root.put("diff-url", m.group(3));
      root.put("summary", m.group(6));

      return root;
    } else {
      throw new IllegalArgumentException();
    }
  }

  public static void main(String[] args) {
    String[] lines = new String[] { "[[Wikipedia talk:Articles for creation/Lords of War]]  http://en.wikipedia.org/w/index.php?diff=562991653&oldid=562991567 * BBGLordsofWar * (+95) /* Lords of War: Elves versus Lizardmen */]", "[[David Shepard (surgeon)]] M http://en.wikipedia.org/w/index.php?diff=562993463&oldid=562989820 * Jacobsievers * (+115) /* American Revolution (1775�1783) */  Added to note regarding David Shepard's brothers" };

    for (String line : lines) {
      System.out.println(parse(line));
    }
  }
}
