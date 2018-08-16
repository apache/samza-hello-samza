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

import java.io.Serializable;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.table.Table;
import org.apache.samza.table.caching.CachingTableDescriptor;
import org.apache.samza.table.remote.RemoteTableDescriptor;
import org.apache.samza.table.remote.TableReadFunction;
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
 *     ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/stock-price-table-joiner.properties
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
public class StockPriceTableJoiner implements StreamApplication {

  /**
   * Default API key "demo" only works with symbol "MSFT"; however you can get an
   * API key for free at https://www.alphavantage.co/, which will work for other symbols.
   */
  static final private String API_KEY = "demo";

  static final private String URL_TEMPLATE =
      "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s&apikey=" + API_KEY;

  static final private String INPUT_TOPIC = "stock-symbol-input";
  static final private String OUTPUT_TOPIC = "stock-price-output";

  @Override
  public void init(StreamGraph graph, Config config) {

    Table remoteTable = graph.getTable(new RemoteTableDescriptor("remote-table")
        .withReadRateLimit(10)
        .withReadFunction(new StockPriceReadFunction()));

    Table table = graph.getTable(new CachingTableDescriptor("table")
        .withTable(remoteTable)
        .withReadTtl(Duration.ofSeconds(5)));

    OutputStream<StockPrice> joinResultStream = graph.getOutputStream(
        OUTPUT_TOPIC, new JsonSerdeV2<>(StockPrice.class));

    graph.getInputStream(INPUT_TOPIC, new StringSerde())
        .map(symbol -> new KV<String, Void>(symbol, null))
        .join(table, new JoinFn())
        .sendTo(joinResultStream);

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

  static class StockPriceReadFunction implements TableReadFunction<String, Double> {
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
  }

  static class StockPrice implements Serializable {

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
