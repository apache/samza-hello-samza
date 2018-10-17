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

package samza.examples.azure;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.eventhub.descriptors.EventHubsInputDescriptor;
import org.apache.samza.system.eventhub.descriptors.EventHubsOutputDescriptor;
import org.apache.samza.system.eventhub.descriptors.EventHubsSystemDescriptor;


public class AzureApplication implements StreamApplication {
  // Stream names
  private static final String INPUT_STREAM_ID = "input-stream";
  private static final String OUTPUT_STREAM_ID = "output-stream";

  // These properties could be configured here or in azure-application-local-runner.properties
  // Keep in mind that the .properties file will be overwrite properties defined here with Descriptors
  private static final String EVENTHUBS_NAMESPACE = "YOUR-EVENT-HUBS-NAMESPACE";

  // Upstream and downstream Event Hubs entity names
  private static final String EVENTHUBS_INPUT_ENTITY = "YOUR-INPUT-ENTITY";
  private static final String EVENTHUBS_OUTPUT_ENTITY = "YOUR-OUTPUT-ENTITY";

  // Consider storing these sensitive fields in an .properties file
  private static final String EVENTHUBS_SAS_KEY_NAME = "YOUR-SAS-KEY-NAME";
  private static final String EVENTHUBS_SAS_KEY_TOKEN = "YOUR-SAS-TOKEN";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    // Define your system here
    EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs")
        .withDefaultStreamOffsetDefault(SystemStreamMetadata.OffsetType.OLDEST);

    // Choose your Serialize/Deserialize types for the Value of the EventData payload here
    StringSerde serde = new StringSerde();

    // Define the input and output descriptors with respective configs
    EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
        systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, serde)
            .withSasKeyName(EVENTHUBS_SAS_KEY_NAME)
            .withSasKey(EVENTHUBS_SAS_KEY_TOKEN);

    EventHubsOutputDescriptor<KV<String, String>> outputDescriptor =
        systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_OUTPUT_ENTITY, serde)
            .withSasKeyName(EVENTHUBS_SAS_KEY_NAME)
            .withSasKey(EVENTHUBS_SAS_KEY_TOKEN);

    // Define the input and output streams with descriptors
    MessageStream<KV<String, String>> eventhubInput = appDescriptor.getInputStream(inputDescriptor);
    OutputStream<KV<String, String>> eventhubOutput = appDescriptor.getOutputStream(outputDescriptor);

    // Define the execution flow with the high-level API
    eventhubInput
        .map((message) -> {
          System.out.println("Sending: ");
          System.out.println("Received Key: " + message.getKey());
          System.out.println("Received Message: " + message.getValue());
          return message;
        })
        .sendTo(eventhubOutput);
  }
}
