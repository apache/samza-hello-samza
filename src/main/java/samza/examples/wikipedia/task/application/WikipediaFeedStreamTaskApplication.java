package samza.examples.wikipedia.task.application;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;
import samza.examples.wikipedia.task.WikipediaStatsStreamTask;


public class WikipediaFeedStreamTaskApplication implements TaskApplication {

  @Override
  public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {

    // Define a system descriptor for Kafka
    GenericSystemDescriptor systemDescriptor =
        new GenericSystemDescriptor("wikipedia", "samza.examples.wikipedia.system.WikipediaSystemFactory");

    // Input descriptors specifying the topic name and serde to use
    GenericInputDescriptor inputDescriptorForWikipedia =
        systemDescriptor.getInputDescriptor("#en.wikipedia", new JsonSerde<>());

    GenericInputDescriptor inputDescriptorForWiktionary =
        systemDescriptor.getInputDescriptor("#en.wiktionary", new JsonSerde<>());

    GenericInputDescriptor inputDescriptorForWikiNews =
        systemDescriptor.getInputDescriptor("#en.wikinews", new JsonSerde<>());

    // Set the inputs
    taskApplicationDescriptor.withInputStream(inputDescriptorForWikipedia);
    taskApplicationDescriptor.withInputStream(inputDescriptorForWiktionary);
    taskApplicationDescriptor.withInputStream(inputDescriptorForWikiNews);

    // Set the task factory
    taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new WikipediaStatsStreamTask());
  }
}
