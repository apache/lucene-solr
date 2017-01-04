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
package org.apache.solr.ltr;

public class FeatureLoggerTestUtils {

  public static String toFeatureVector(String ... keysAndValues) {
    return toFeatureVector(
        CSVFeatureLogger.DEFAULT_KEY_VALUE_SEPARATOR,
        CSVFeatureLogger.DEFAULT_FEATURE_SEPARATOR,
        keysAndValues);
  }

  public static String toFeatureVector(char keyValueSeparator, char featureSeparator,
      String ... keysAndValues) {
    StringBuilder sb = new StringBuilder(keysAndValues.length/2 * 3);
    for (int ii = 0; ii+1 < keysAndValues.length; ii += 2) {
        sb.append(keysAndValues[ii])
        .append(keyValueSeparator)
        .append(keysAndValues[ii+1])
        .append(featureSeparator);
    }

    final String features = (sb.length() > 0 ?
        sb.substring(0, sb.length() - 1) : "");

    return features;
  }

}
