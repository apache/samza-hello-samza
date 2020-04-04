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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.descriptors.GenericOutputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samza.examples.azure.data.PageViewAvroRecord;
import samza.examples.cookbook.data.PageView;
import samza.examples.wikipedia.application.WikipediaApplication;


/**
 * In this example, we demonstrate sending blobs to Azure Blob Storage.
 * This Samza job reads from Kafka topic "page-view-azure-blob-input" and produces blobs to Azure-Container "azure-blob-container" in your Azure Storage account.
 *
 * Currently, Samza supports sending Avro files are blobs.
 * Hence the incoming messages into the Samza job have to be converted to an Avro record.
 * For this job, we use input message as {@link samza.examples.cookbook.data.PageView} and
 * covert it to an Avro record defined as {@link samza.examples.azure.data.PageViewAvroRecord}.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Replace your-azure-storage-account-name and your-azure-storage-account-key with details of your Azure Storage Account.
 *   </li>
 *   <li>
 *     Ensure that the topic "page-view-azure-blob-input" is created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic page-view-azure-blob-input --partitions 1 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-path=$PWD/deploy/samza/config/azure-blob-application.properties
 *   </li>
 *   <li>
 *     Produce some messages to the "page-view-azure-blob-input" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic page-view-azure-blob-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com"} <br/>
 *     {"userId": "user2", "country": "france", "pageId":"facebook.com"} <br/>
 *     {"userId": "user3", "country": "china", "pageId":"yahoo.com"} <br/>
 *     {"userId": "user4", "country": "italy", "pageId":"linkedin.com"} <br/>
 *     {"userId": "user5", "country": "germany", "pageId":"amazon.com"} <br/>
 *     {"userId": "user6", "country": "denmark", "pageId":"apple.com"} <br/>
 *   </li>
 *   <li>
 *    Seeing Output:
 *    <ol>
 *      <li>
 *       See blobs in your Azure portal at https://<azure-storage-account-name>.blob.core.windows.net/azure-blob-container/PageViewEventStream/<time-stamp>.avro
 *      </li>
 *      <li>
 *       system-name "azure-blob-container" in configs and code below maps to Azure-Container in Azure Storage account.
 *      </li>
 *      <li>
 *       <time-stamp> is of the format yyyy/MM/dd/HH/mm-ss-randomString.avro. Hence navigate through the virtual folders on the portal to see your blobs.
 *      </li>
 *      <li>
 *       Due to network calls, allow a few minutes for blobs to appear on the portal.
 *      </li>
 *      <li>
 *       Config "maxMessagesPerBlob=2" ensures that a blob is created per 2 input messages. Adjust input or config accordingly.
 *      </li>
 *    </ol>
 *   </li>
 * </ol>
 */
public class AzureBlobApplication implements StreamApplication {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobApplication.class);

  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");
  private static final String INPUT_PAGEVIEW_STREAM_ID = "page-view-azure-blob-input";
  private static final String OUTPUT_SYSTEM = "azure-blob-container";
  private static final String OUTPUT_STREAM = "PageViewEventStream";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    // Define a system descriptor for Kafka
    KafkaSystemDescriptor kafkaSystemDescriptor =
        new KafkaSystemDescriptor("kafka").withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
            .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
            .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<PageView> pageViewInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_PAGEVIEW_STREAM_ID, new JsonSerdeV2<>(PageView.class));

    // Define a system descriptor for Azure Blob Storage
    GenericSystemDescriptor azureBlobSystemDescriptor =
        new GenericSystemDescriptor(OUTPUT_SYSTEM, "org.apache.samza.system.azureblob.AzureBlobSystemFactory");

    GenericOutputDescriptor<PageViewAvroRecord> azureBlobOuputDescriptor =
        azureBlobSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM, new NoOpSerde<>());

    // Set Kafka as the default system for the job
    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    // Define the input and output streams with descriptors
    MessageStream<PageView> pageViewInput = appDescriptor.getInputStream(pageViewInputDescriptor);
    OutputStream<PageViewAvroRecord> pageViewAvroRecordOutputStream = appDescriptor.getOutputStream(azureBlobOuputDescriptor);

    // Define the execution flow with the high-level API
    pageViewInput
        .map((message) -> {
          LOG.info("Sending: Received PageViewEvent with pageId: " + message.pageId);
          return PageViewAvroRecord.buildPageViewRecord(message);
        })
        .sendTo(pageViewAvroRecordOutputStream);
  }
}
