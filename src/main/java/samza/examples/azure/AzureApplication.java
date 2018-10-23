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
import org.apache.samza.serializers.ByteSerde;
import org.apache.samza.system.eventhub.descriptors.EventHubsInputDescriptor;
import org.apache.samza.system.eventhub.descriptors.EventHubsOutputDescriptor;
import org.apache.samza.system.eventhub.descriptors.EventHubsSystemDescriptor;


public class AzureApplication implements StreamApplication {

  // Input stream id
  private static final String INPUT_STREAM_ID = "input-stream";

  // Outputs stream id
  private static final String OUTPUT_STREAM_ID = "output-stream";

  @Override
  public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {

    // Build input and output system descriptors
    EventHubsSystemDescriptor eventHubsSystemDescriptor = new EventHubsSystemDescriptor("");

    EventHubsInputDescriptor<KV<String, byte[]>> inputDescriptor =
        eventHubsSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, "", "", new ByteSerde());

    EventHubsOutputDescriptor<KV<String, byte[]>> outputDescriptor =
        eventHubsSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, "", "", new ByteSerde());

    // Input stream
    MessageStream<KV<String, byte[]>> eventhubInput = streamApplicationDescriptor.getInputStream(inputDescriptor);

    // Output stream
    OutputStream<KV<String, byte[]>> eventhubOutput = streamApplicationDescriptor.getOutputStream(outputDescriptor);

    // wire the logic
    eventhubInput.filter((message) -> message.getKey() != null).map((message) -> {
      System.out.println("Sending: ");
      System.out.println("Received Key: " + message.getKey());
      System.out.println("Received Message: " + new String(message.getValue()));
      return message;
    }).sendTo(eventhubOutput);
  }
}
