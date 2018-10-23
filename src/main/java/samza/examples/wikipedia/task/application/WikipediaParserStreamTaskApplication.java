package samza.examples.wikipedia.task.application;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;
import samza.examples.wikipedia.task.WikipediaParserStreamTask;


public class WikipediaParserStreamTaskApplication implements TaskApplication {
  @Override
  public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {

    // Define a system descriptor for Kafka
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor("kafka");

    // Input descriptor for the wikipedia-edits topic
    KafkaInputDescriptor kafkaInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor("wikipedia-raw", new JsonSerde<>());

    // Set the input
    taskApplicationDescriptor.withInputStream(kafkaInputDescriptor);

    // Set the task factory
    taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new WikipediaParserStreamTask());
  }
}

