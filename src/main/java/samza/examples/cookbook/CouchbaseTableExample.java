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
package samza.examples.cookbook;

import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.Context;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.remote.NoOpTableReadFunction;
import org.apache.samza.table.remote.RemoteTable;
import org.apache.samza.table.remote.couchbase.CouchbaseTableWriteFunction;
import org.apache.samza.table.retry.TableRetryPolicy;


/**
 * This is a simple word count example using a remote store.
 *
 * In this example, we use Couchbase to demonstrate how to invoke API's on a remote store other than get, put or delete
 * as defined in {@link org.apache.samza.table.remote.AsyncRemoteTable}. Input messages are collected from user through
 * a Kafka console producer, and tokenized using space. For each word, we increment a counter for this word
 * as well as a counter for all words on Couchbase. We also output the current value of both counters to Kafka console
 * consumer.
 *
 * A rate limit of 4 requests/second to Couchbase is set of the entire job, internally Samza uses an embedded
 * rate limiter, which evenly distributes the total rate limit among tasks. As we invoke 2 calls on Couchbase
 * for each word, you should see roughly 2 messages per second in the Kafka console consumer
 * window.
 *
 * A retry policy with 1 second fixed backoff time and max 3 retries is attached to the remote table.
 *
 * <p> Concepts covered: remote table, rate limiter, retry, arbitrary operation on remote store.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Create a Couchbase instance using docker; Log into the admin UI at http://localhost:8091 (Administrator/password) <br/>
 *     create a bucket called "my-bucket" <br/>
 *     Under Security tab, create a user with the same name, set 123456 as the password, and give it "Data Reader"
 *     and "Data Writer" privilege for this bucket. <br/>
 *     More information can be found at https://docs.couchbase.com/server/current/getting-started/do-a-quick-install.html
 *   </li>
 *   <li>
 *     Create Kafka topics "word-input" and "count-output" <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic word-input --partitions 2 --replication-factor 1
 *     ./deploy/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic count-output --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-path=$PWD/deploy/samza/config/couchbase-table-example.properties
 *   </li>
 *   <li>
 *     Consume messages from the output topic <br/>
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic count-output
 *   </li>
 *   <li>
 *     Produce some messages to the input topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic word-input --broker-list localhost:9092
 *
 *     After the console producer is started, type
 *     1
 *     2
 *     3
 *     4
 *     5
 *     4
 *     3
 *     2
 *     1
 *
 *     You should see messages like below from the console consumer window
 *
 *     2019-05-23 21:18:07 2019-05-23 21:18:07 word=2, count=1, total-count=1
 *     2019-05-23 21:18:07 2019-05-23 21:18:07 word=1, count=1, total-count=2
 *     2019-05-23 21:18:07 2019-05-23 21:18:07 word=4, count=1, total-count=3
 *     2019-05-23 21:18:07 2019-05-23 21:18:07 word=3, count=1, total-count=4
 *     2019-05-23 21:18:08 2019-05-23 21:18:08 word=4, count=2, total-count=5
 *     2019-05-23 21:18:08 2019-05-23 21:18:08 word=5, count=1, total-count=6
 *     2019-05-23 21:18:09 2019-05-23 21:18:09 word=2, count=2, total-count=7
 *     2019-05-23 21:18:09 2019-05-23 21:18:09 word=3, count=2, total-count=8
 *     2019-05-23 21:18:10 2019-05-23 21:18:10 word=1, count=2, total-count=9
 *
 *     You can examine the result on Couchbase Admin GUI as well.
 *
 *     Note:
 *       - If you enter "1 2 3 4 5 4 3 2 1", you should see roughly 1 QPS as
 *         the input is processed by only one task
 *
 *
 *   </li>
 * </ol>
 *
 */
public class CouchbaseTableExample implements StreamApplication {

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "word-input";
  private static final String OUTPUT_STREAM_ID = "count-output";

  private static final String CLUSTER_NODES = "couchbase://127.0.0.1";
  private static final int COUCHBASE_PORT = 11210;
  private static final String BUCKET_NAME = "my-bucket";
  private static final String BUCKET_PASSWORD = "123456";
  private static final String TOTAL_COUNT_ID = "total-count";

  @Override
  public void describe(StreamApplicationDescriptor app) {

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<String> wordInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, new StringSerde());

    KafkaOutputDescriptor<String> countOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, new StringSerde());

    MyCouchbaseTableWriteFunction writeFn = new MyCouchbaseTableWriteFunction(BUCKET_NAME, CLUSTER_NODES)
        .withBootstrapCarrierDirectPort(COUCHBASE_PORT)
        .withUsernameAndPassword(BUCKET_NAME, BUCKET_PASSWORD)
        .withTimeout(Duration.ofSeconds(5));

    TableRetryPolicy retryPolicy = new TableRetryPolicy()
        .withFixedBackoff(Duration.ofSeconds(1))
        .withStopAfterAttempts(3);

    RemoteTableDescriptor couchbaseTableDescriptor = new RemoteTableDescriptor("couchbase-table")
        .withReadFunction(new NoOpTableReadFunction())
        .withReadRateLimiterDisabled()
        .withWriteFunction(writeFn)
        .withWriteRetryPolicy(retryPolicy)
        .withWriteRateLimit(4);

    app.withDefaultSystem(kafkaSystemDescriptor);
    MessageStream<String> wordStream = app.getInputStream(wordInputDescriptor);
    OutputStream<String> countStream = app.getOutputStream(countOutputDescriptor);
    app.getTable(couchbaseTableDescriptor);

    wordStream
        .flatMap(m -> Arrays.asList(m.split(" ")))
        .filter(word -> word != null && word.length() > 0)
        .map(new MyCountFunction())
        .map(countString -> currentTime() + " " + countString)
        .sendTo(countStream);
  }

  static class MyCountFunction implements MapFunction<String, String> {

    private MyCouchbaseTableWriteFunction writeFn;

    @Override
    public void init(Context context) {
      RemoteTable table = (RemoteTable) context.getTaskContext().getTable("couchbase-table");
      writeFn = (MyCouchbaseTableWriteFunction) table.getWriteFunction();
    }

    @Override
    public String apply(String word) {
      CompletableFuture<Long> countFuture = writeFn.incCounter(word);
      CompletableFuture<Long> totalCountFuture = writeFn.incCounter(TOTAL_COUNT_ID);
      return String.format("%s word=%s, count=%d, total-count=%d",
          currentTime(), word, countFuture.join(), totalCountFuture.join());
    }
  }

  static class MyCouchbaseTableWriteFunction extends CouchbaseTableWriteFunction<JsonObject> {

    private final static int OP_COUNTER = 1;

    public MyCouchbaseTableWriteFunction(String bucketName, String... clusterNodes) {
      super(bucketName, JsonObject.class, clusterNodes);
    }

    @Override
    public <T> CompletableFuture<T> writeAsync(int opId, Object... args) {
      switch (opId) {
        case OP_COUNTER:
          Preconditions.checkArgument(2 == args.length,
              String.format("Two arguments (String and int) are expected for counter operation (opId=%d)", opId));
          String id = (String) args[0];
          int delta = (int) args[1];
          return asyncWriteHelper(
              bucket.async().counter(id, delta, 1, timeout.toMillis(), TimeUnit.MILLISECONDS),
              String.format("Failed to invoke counter with Id %s from bucket %s.", id, bucketName),
              false);
        default:
          throw new SamzaException("Unknown opId: " + opId);
      }
    }

    public CompletableFuture<Long> incCounter(String id) {
      return table.writeAsync(OP_COUNTER, id, 1);
    }

  }

  private static String currentTime() {
    return DATE_FORMAT.format(new Date());
  }

}
