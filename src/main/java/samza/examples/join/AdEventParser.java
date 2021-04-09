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

package samza.examples.join;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class AdEventParser {

  /**
   * Parse raw ad event that should have "key=value" pairs separated with space character into a map.
   * Example argument:
   *  {@code impression-id=1 type=click advertiser-id=1 ip=111.111.111.* agent=Chrome timestamp=2017-01-01T00:00:00.000"}
   * Return value is a map that contains given key-value pairs
   *
   * @param rawAdEvent raw ad event String that should have key=value pairs separated with one space character
   * @return event map
   * @throws ParseException
   */
  public static synchronized Map<String, String> parseAdEvent(String rawAdEvent) throws ParseException{
    Map<String, String> adEvent = new HashMap<>();
    String[] fields = rawAdEvent.split(" ");
    for(String field : fields){
      String[] keyValuePair = field.split("=");
      if(keyValuePair.length == 2)
        adEvent.put(keyValuePair[0], keyValuePair[1]);
      else
        throw new ParseException("Error while parsing. Messages should have only 'key=value' pairs separated by one space characters with no space and '=' characters in keys and values", -1);
    }
    return adEvent;
  }
}
