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

package samza.examples.wikipedia.system.descriptors;

import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;

import samza.examples.wikipedia.system.WikipediaFeed;


public class WikipediaInputDescriptor extends InputDescriptor<WikipediaFeed.WikipediaFeedEvent, WikipediaInputDescriptor> {
  // Messages come from WikipediaConsumer so we know that they don't have a key and don't need to be deserialized.
  private static final Serde SERDE = new NoOpSerde();

  WikipediaInputDescriptor(String streamId, SystemDescriptor systemDescriptor) {
    super(streamId, SERDE, systemDescriptor, null);
  }

  public WikipediaInputDescriptor withChannel(String channel) {
    withPhysicalName(channel);
    return this;
  }
}
