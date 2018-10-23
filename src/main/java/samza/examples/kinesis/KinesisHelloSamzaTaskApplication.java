package samza.examples.kinesis;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;


public class KinesisHelloSamzaTaskApplication implements TaskApplication {

  @Override
  public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {

    GenericSystemDescriptor systemDescriptor =
        new GenericSystemDescriptor("kinesis", "org.apache.samza.system.kinesis.KinesisSystemFactory");

    // Please replace the input stream with the stream you plan to consume from.
    GenericInputDescriptor inputDescriptor =
        systemDescriptor.getInputDescriptor("kinesis-samza-sample-stream", new JsonSerde<>());

    // set the input
    taskApplicationDescriptor.withInputStream(inputDescriptor);

    // set the task factory
    taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new KinesisHelloSamza());
  }
}

