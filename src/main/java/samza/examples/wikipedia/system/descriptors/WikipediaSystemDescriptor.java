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

import samza.examples.wikipedia.system.WikipediaSystemFactory;

import java.util.Map;
import org.apache.samza.system.descriptors.SystemDescriptor;

public class WikipediaSystemDescriptor extends SystemDescriptor<WikipediaSystemDescriptor> {
  private static final String SYSTEM_NAME = "wikipedia";
  private static final String FACTORY_CLASS_NAME = WikipediaSystemFactory.class.getName();
  private static final String HOST_KEY = "systems.%s.host";
  private static final String PORT_KEY = "systems.%s.port";

  private final String host;
  private final int port;

  public WikipediaSystemDescriptor(String host, int port) {
    super(SYSTEM_NAME, FACTORY_CLASS_NAME, null, null);
    this.host = host;
    this.port = port;
  }

  public WikipediaInputDescriptor getInputDescriptor(String streamId) {
    return new WikipediaInputDescriptor(streamId, this);
  }

  @Override
  public Map<String, String> toConfig() {
    Map<String, String> configs = super.toConfig();
    configs.put(String.format(HOST_KEY, getSystemName()), host);
    configs.put(String.format(PORT_KEY, getSystemName()), Integer.toString(port));
    return configs;
  }
}
