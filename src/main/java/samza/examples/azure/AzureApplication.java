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
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericOutputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;

public class AzureApplication implements StreamApplication {
  private static final String INPUT_STREAM_ID = "input-stream";
  private static final String OUTPUT_STREAM_ID = "output-stream";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    GenericSystemDescriptor systemDescriptor =
        new GenericSystemDescriptor("eventhubs", "org.apache.samza.system.eventhub.EventHubSystemFactory");

    KVSerde<String, byte[]> serde = KVSerde.of(new StringSerde(), new ByteSerde());

    GenericInputDescriptor<KV<String, byte[]>> inputDescriptor =
        systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, serde);

    GenericOutputDescriptor<KV<String, byte[]>> outputDescriptor =
        systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, serde);

    MessageStream<KV<String, byte[]>> eventhubInput = appDescriptor.getInputStream(inputDescriptor);
    OutputStream<KV<String, byte[]>> eventhubOutput = appDescriptor.getOutputStream(outputDescriptor);

    eventhubInput
        .filter((message) -> message.getKey() != null)
        .map((message) -> {
          System.out.println("Sending: ");
          System.out.println("Received Key: " + message.getKey());
          System.out.println("Received Message: " + new String(message.getValue()));
          return message;
        })
        .sendTo(eventhubOutput);
  }
}
