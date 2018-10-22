/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.request.json;

import java.io.IOException;
import org.noggit.JSONParser;

public class JSONUtil {


  public static boolean advanceToMapKey(JSONParser parser, String key, boolean deepSearch) throws IOException {
    for (;;) {
      int event = parser.nextEvent();
      switch (event) {
        case JSONParser.STRING:
          if (key != null && parser.wasKey()) {
            String val = parser.getString();
            if (key.equals(val)) {
              return true;
            }
          }
          break;
        case JSONParser.OBJECT_END:
          return false;
        case JSONParser.OBJECT_START:
          if (deepSearch) {
            boolean found = advanceToMapKey(parser, key, true);
            if (found) {
              return true;
            }
          } else {
            advanceToMapKey(parser, null, false);
          }
          break;
        case JSONParser.ARRAY_START:
          skipArray(parser, key, deepSearch);
          break;
      }
    }
  }

  public static void skipArray(JSONParser parser, String key, boolean deepSearch) throws IOException {
    for (;;) {
      int event = parser.nextEvent();
      switch (event) {
        case JSONParser.OBJECT_START:
          advanceToMapKey(parser, key, deepSearch);
          break;
        case JSONParser.ARRAY_START:
          skipArray(parser, key, deepSearch);
          break;
        case JSONParser.ARRAY_END:
          return;
      }
    }
  }

  public static void expect(JSONParser parser, int parserEventType) throws IOException {
    int event = parser.nextEvent();
    if (event != parserEventType) {
      throw new IOException("JSON Parser: expected " + JSONParser.getEventString(parserEventType) + " but got " + JSONParser.getEventString(event) );
    }
  }

}
