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
package samza.examples.cookbook.data;

import java.util.Map;
import org.codehaus.jackson.annotate.JsonProperty;


public class Profile {

  public final String userId;
  public final String company;

  /**
   * Constructs a user profile.
   *
   * @param userId the user Id
   * @param company company to which the user belong to
   */
  public Profile(
      @JsonProperty("userId") String userId,
      @JsonProperty("company") String company) {
    this.userId = userId;
    this.company = company;
  }

  public Profile(Map<String, Object> jsonObject) {
    this((String) jsonObject.get("userId"), (String) jsonObject.get("company"));
  }
}
