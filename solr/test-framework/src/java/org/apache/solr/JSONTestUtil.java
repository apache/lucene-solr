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
package org.apache.solr;

import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import java.io.IOException;
import java.util.*;


public class JSONTestUtil {

  /**
   * Default delta used in numeric equality comparisons for floats and doubles.
   */
  public final static double DEFAULT_DELTA = 1e-5;
  public static volatile boolean failRepeatedKeys = false;

  /**
   * comparison using default delta
   * @see #DEFAULT_DELTA
   * @see #match(String,String,double)
   */
  public static String match(String input, String pathAndExpected) throws Exception {
    return match(input, pathAndExpected, DEFAULT_DELTA);
  }

  /**
   * comparison using default delta
   * @see #DEFAULT_DELTA
   * @see #match(String,String,String,double)
   */
  public static String match(String path, String input, String expected) throws Exception {
    return match(path, input, expected, DEFAULT_DELTA);
  }

  /**
   * comparison using default delta
   * @see #DEFAULT_DELTA
   * @see #matchObj(String,Object,Object,double)
   */
  public static String matchObj(String path, Object input, Object expected) throws Exception {
    return matchObj(path,input,expected, DEFAULT_DELTA);
  }

  /**
   * @param input JSON Structure to parse and test against
   * @param pathAndExpected JSON path expression + '==' + expected value
   * @param delta tollerance allowed in comparing float/double values
   */
  public static String match(String input, String pathAndExpected, double delta) throws Exception {
    int pos = pathAndExpected.indexOf("==");
    String path = pos>=0 ? pathAndExpected.substring(0,pos) : null;
    String expected = pos>=0 ? pathAndExpected.substring(pos+2) : pathAndExpected;
    return match(path, input, expected, delta);
  }

  /**
   * @param input Object structure to parse and test against
   * @param pathAndExpected JSON path expression + '==' + expected value
   * @param delta tollerance allowed in comparing float/double values
   */
  public static String matchObj(Object input, String pathAndExpected, double delta) throws Exception {
    int pos = pathAndExpected.indexOf("==");
    String path = pos>=0 ? pathAndExpected.substring(0,pos) : null;
    String expected = pos>=0 ? pathAndExpected.substring(pos+2) : pathAndExpected;
    Object expectObj = failRepeatedKeys ? new NoDupsObjectBuilder(new JSONParser(expected)).getVal() : ObjectBuilder.fromJSON(expected);
    assert path != null;
    return matchObj(path, input, expectObj, delta);
  }

  /**
   * @param path JSON path expression
   * @param input JSON Structure to parse and test against
   * @param expected expected value of path
   * @param delta tollerance allowed in comparing float/double values
   */
  public static String match(String path, String input, String expected, double delta) throws Exception {
    Object inputObj = failRepeatedKeys ? new NoDupsObjectBuilder(new JSONParser(input)).getVal() : ObjectBuilder.fromJSON(input);
    Object expectObj = failRepeatedKeys ? new NoDupsObjectBuilder(new JSONParser(expected)).getVal() : ObjectBuilder.fromJSON(expected);
    return matchObj(path, inputObj, expectObj, delta);
  }

  static class NoDupsObjectBuilder extends ObjectBuilder {
    public NoDupsObjectBuilder(JSONParser parser) throws IOException {
      super(parser);
    }

    @Override
    public void addKeyVal(Object map, Object key, Object val) {
      Object prev = ((Map<Object, Object>) map).put(key, val);
      if (prev != null) {
        throw new RuntimeException("REPEATED JSON OBJECT KEY: key=" + key + " prevValue=" + prev + " thisValue" + val);
      }
    }
  }

  /**
   * @param path JSON path expression
   * @param input JSON Structure
   * @param expected expected JSON Object
   * @param delta tollerance allowed in comparing float/double values
   */
  public static String matchObj(String path, Object input, Object expected, double delta) {
    CollectionTester tester = new CollectionTester(input,delta);
    boolean reversed = path.startsWith("!");
    String positivePath = reversed ? path.substring(1) : path;
    if (!tester.seek(positivePath) ^ reversed) {
      return "Path not found: " + path;
    }
    if (expected != null && (!tester.match(expected) ^ reversed)) {
      return tester.err + " @ " + tester.getPath();
    }
    return null;
  }
}


