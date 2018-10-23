package samza.examples.cookbook;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.sql.runner.SamzaSqlApplication;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;


/**
 * In this example, we demonstrate how to use SQL to create a samza job.
 *
 * <p>Concepts covered: Using sql to perform Stream processing.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Ensure that the topic "PageViewStream" is created  <br/>
 *     ./kafka-topics.sh  --zookeeper localhost:2181 --create --topic PageViewStream --partitions 1 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the ./bin/run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory <br/>
 *     --config-path=file://$PWD/deploy/samza/config/pageview-filter-sql.properties)
 *   </li>
 *   <li>
 *     Produce some messages to the "PageViewStream" topic <br/>
 *     Please follow instructions at https://github.com/srinipunuru/samzasqltools on how to produce events into PageViewStream<br/>
 *   </li>
 *   <li>
 *     Consume messages from the "pageview-filter-output" topic (e.g. bin/kafka-console-consumer.sh)
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic outputTopic <br/>
 *     --property print.key=true    </li>
 * </ol>
 *
 */

public class PageViewFilterSqlApp extends SamzaSqlApplication {

  public static final String CFG_SCHEMA_FILES = "schema.files";
  private static final String CFG_SCHEMA_VALUE_FMT = "";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    String sqlStmt = "insert into kafka.NewLinkedInEmployees select id, Name from ProfileChangeStream";
    String schemaFiles = appDescriptor.getConfig().get(CFG_SCHEMA_FILES);
    populateSchemaConfigs(schemaFiles, appDescriptor);
    appDescriptor.getConfig().put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sqlStmt);
    super.describe(appDescriptor);
  }

  private void populateSchemaConfigs(String schemaFilesValue, StreamApplicationDescriptor appDescriptor) {
    String[] schemaFiles = schemaFilesValue.split(",");
    for (String schemaFileValue : schemaFiles) {
      try {
        File schemaFile = new File(schemaFileValue);
        String schemaValue = Schema.parse(schemaFile).toString();
        appDescriptor.getConfig().put(String.format(CFG_SCHEMA_VALUE_FMT, schemaFile.getName()), schemaValue);
      } catch (IOException e) {
        throw new SamzaException("Unable to parse the schemaFile " + schemaFileValue, e);
      }
    }
  }
}
