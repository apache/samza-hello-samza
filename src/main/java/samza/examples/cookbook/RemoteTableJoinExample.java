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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.remote.BaseTableFunction;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.apache.samza.util.HttpUtil;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * In this example, we join a stream of stock symbols with a remote table backed by a RESTful service,
 * which delivers latest stock quotes. The join results contain stock symbol and latest price, and are
 * delivered to an output stream.
 *
 * A rate limit of 10 requests/second is set of the entire job, internally Samza uses an embedded
 * rate limiter, which evenly distributes the total rate limit among tasks.
 *
 * A caching table is used over the remote table with a read TTL of 5 seconds, therefore one would
 * receive the same quote with this time span.
 *
 * <p> Concepts covered: remote table, rate limiter, caching table, stream to table joins.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Create Kafka topics "stock-symbol-input", "stock-price-output" are created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic stock-symbol-input --partitions 2 --replication-factor 1
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic stock-price-output --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-path=$PWD/deploy/samza/config/remote-table-join-example.properties
 *   </li>
 *   <li>
 *     Consume messages from the output topic <br/>
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic stock-price-output
 *   </li>
 *   <li>
 *     Produce some messages to the input topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic stock-symbol-input --broker-list localhost:9092
 *
 *     After the console producer is started, type
 *     MSFT
 *
 *     You should see messages like below from the console consumer window
 *     {"symbol":"MSFT","close":107.64}
 *
 *     Note: you will need a free API key for symbols other than MSFT, see below for more information.
 *   </li>
 * </ol>
 *
 */
public class RemoteTableJoinExample implements StreamApplication {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  /**
   * Default API key "demo" only works for symbol "MSFT"; however you can get an
   * API key for free at https://www.alphavantage.co/, which will work for other symbols.
   */
  private static final String API_KEY = "demo";

  private static final String URL_TEMPLATE =
      "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s&apikey=" + API_KEY;

  private static final String INPUT_STREAM_ID = "stock-symbol-input";
  private static final String OUTPUT_STREAM_ID = "stock-price-output";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<String> stockSymbolInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, new StringSerde());
    KafkaOutputDescriptor<StockPrice> stockPriceOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, new JsonSerdeV2<>(StockPrice.class));
    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);
    MessageStream<String> stockSymbolStream = appDescriptor.getInputStream(stockSymbolInputDescriptor);
    OutputStream<StockPrice> stockPriceStream = appDescriptor.getOutputStream(stockPriceOutputDescriptor);

    RemoteTableDescriptor<String, Double> remoteTableDescriptor =
        new RemoteTableDescriptor("remote-table")
            .withReadRateLimit(10)
            .withReadFunction(new StockPriceReadFunction());
    CachingTableDescriptor<String, Double> cachedRemoteTableDescriptor =
        new CachingTableDescriptor<>("cached-remote-table", remoteTableDescriptor)
            .withReadTtl(Duration.ofSeconds(5));
    Table<KV<String, Double>> cachedRemoteTable = appDescriptor.getTable(cachedRemoteTableDescriptor);

    stockSymbolStream
        .map(symbol -> new KV<String, Void>(symbol, null))
        .join(cachedRemoteTable, new JoinFn())
        .sendTo(stockPriceStream);

  }

  static class JoinFn implements StreamTableJoinFunction<String, KV<String, Void>, KV<String, Double>, StockPrice> {
    @Override
    public StockPrice apply(KV<String, Void> message, KV<String, Double> record) {
      return record == null ? null : new StockPrice(message.getKey(), record.getValue());
    }
    @Override
    public String getMessageKey(KV<String, Void> message) {
      return message.getKey();
    }
    @Override
    public String getRecordKey(KV<String, Double> record) {
      return record.getKey();
    }
  }

  static class StockPriceReadFunction extends BaseTableFunction
      implements TableReadFunction<String, Double> {
    @Override
    public CompletableFuture<Double> getAsync(String symbol) {
      return CompletableFuture.supplyAsync(() -> {
        try {
          URL url = new URL(String.format(URL_TEMPLATE, symbol));
          String response = HttpUtil.read(url, 5000, new ExponentialSleepStrategy());
          JsonParser parser = new JsonFactory().createJsonParser(response);
          while (!parser.isClosed()) {
            if (JsonToken.FIELD_NAME.equals(parser.nextToken()) && "4. close".equalsIgnoreCase(parser.getCurrentName())) {
              return Double.valueOf(parser.nextTextValue());
            }
          }
          return -1d;
        } catch (Exception ex) {
          throw new SamzaException(ex);
        }
      });
    }

    @Override
    public boolean isRetriable(Throwable throwable) {
      return false;
    }
  }

  public static class StockPrice implements Serializable {

    public final String symbol;
    public final Double close;

    public StockPrice(
        @JsonProperty("symbol") String symbol,
        @JsonProperty("close") Double close) {
      this.symbol = symbol;
      this.close = close;
    }
  }

}
