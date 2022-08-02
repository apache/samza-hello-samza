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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.startpoint.Startpoint;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.startpoint.StartpointOldest;
import org.apache.samza.startpoint.StartpointUpcoming;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.NoOpMetricsRegistry;


public class StartpointMain {

  public static void main(String[] args) throws IOException {
    Map<String, String> configs = ImmutableMap.of(
        "job.name", "startpoint-test-4",
        "job.coordinator.system", "kafka",
        "systems.kafka.consumer.zookeeper.connect", "localhost:2181",
        "systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory",
        "systems.kafka.producer.bootstrap.servers", "localhost:9092"
    );

    TaskName taskName = new TaskName("Task-0");

    SystemStreamPartition p0 = new SystemStreamPartition("kafka", "startpoint-test-input-4", new Partition(0));
    SystemStreamPartition p1 = new SystemStreamPartition("kafka", "startpoint-test-input-4", new Partition(1));

    CoordinatorStreamStore store =
        new CoordinatorStreamStore(new MapConfig(configs), new NoOpMetricsRegistry());
    store.init();
    StartpointManager startpointManager = new StartpointManager(store);
    startpointManager.start();

//    startpointManager.writeStartpoint(p0, new StartpointUpcoming());
//    startpointManager.writeStartpoint(p1, new StartpointUpcoming());

    System.out.println(startpointManager.readStartpoint(p0));
    System.out.println(startpointManager.readStartpoint(p1));

//    System.out.println(startpointManager.getFanOutForTask(new TaskName("Partition 0")));
//    System.out.println(startpointManager.getFanOutForTask(new TaskName("Partition 1")));
    System.out.println(startpointManager.getFanOutForTask(taskName));

//    startpointManager.fanOut(ImmutableMap.of(taskName, ImmutableSet.of(p0, p1)));

  }
}
