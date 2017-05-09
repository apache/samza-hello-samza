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

package samza.examples.runners;

import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.ApplicationRunnerMain;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.Util;


/**
 * A stand alone runner for running {@link samza.examples.wikipedia.application.WikipediaApplication} locally.
 * It waits for the job to finish; The job can also be ended by killing this runner.
 */
public class StandAloneRunner {

  private static final String STREAM_APPLICATION_CLASS_CONFIG = "app.class";

  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config orgConfig = cmdLine.loadConfig(options);
    Config config = Util.rewriteConfig(orgConfig);

    if (config.containsKey(STREAM_APPLICATION_CLASS_CONFIG)) {
      LocalApplicationRunner runner = new LocalApplicationRunner(config);
      StreamApplication app =
          (StreamApplication) Class.forName(config.get(STREAM_APPLICATION_CLASS_CONFIG)).newInstance();

      runner.run(app);
      runner.waitForFinish();
    } else {
      throw new RuntimeException("Missing app.class configuration. Either provide the configuration or use" + ApplicationRunnerMain.class.getName());
    }
  }
}
