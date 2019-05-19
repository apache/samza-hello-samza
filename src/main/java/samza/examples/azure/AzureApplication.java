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
  private static final String EVENTHUBS_NAMESPACE = "my-eventhubs-namespace";

  // Upstream and downstream Event Hubs entity names
  private static final String EVENTHUBS_INPUT_ENTITY = "my-input-entity";
  private static final String EVENTHUBS_OUTPUT_ENTITY = "my-output-entity";

  // You may define your own config properties in azure-application-local-runner.properties and retrieve them
  // in the StreamApplicationDescriptor. Prefix them with 'sensitive.' to avoid logging them.
  private static final String EVENTHUBS_SAS_KEY_NAME_CONFIG = "sensitive.eventhubs.sas.key.name";
  private static final String EVENTHUBS_SAS_KEY_TOKEN_CONFIG = "sensitive.eventhubs.sas.token";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    // Define your system here
    EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs");

    // Choose your serializer/deserializer for the EventData payload
    StringSerde serde = new StringSerde();

    // Define the input and output descriptors with respective configs
    EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
        systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, serde)
            .withSasKeyName(appDescriptor.getConfig().get(EVENTHUBS_SAS_KEY_NAME_CONFIG))
            .withSasKey(appDescriptor.getConfig().get(EVENTHUBS_SAS_KEY_TOKEN_CONFIG));

    EventHubsOutputDescriptor<KV<String, String>> outputDescriptor =
        systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_OUTPUT_ENTITY, serde)
            .withSasKeyName(appDescriptor.getConfig().get(EVENTHUBS_SAS_KEY_NAME_CONFIG))
            .withSasKey(appDescriptor.getConfig().get(EVENTHUBS_SAS_KEY_TOKEN_CONFIG));

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
