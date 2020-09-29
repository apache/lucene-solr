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
package org.apache.solr.cloud.autoscaling;

import java.util.HashMap;
import java.util.Map;

/**
 * This class represents errors found when validating trigger configuration.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class TriggerValidationException extends Exception {
  private final Map<String, String> details = new HashMap<>();
  private final String name;

  /**
   * Create an exception.
   * @param name name of the trigger / action / listener that caused the exception
   * @param details details of invalid configuration - key is a property name,
   *                value is an error message.
   */
  public TriggerValidationException(String name, Map<String, String> details) {
    super();
    this.name = name;
    if (details != null) {
      this.details.putAll(details);
    }
  }

  /**
   * Create an exception.
   * @param name name of the trigger / action / listener that caused the exception
   * @param keyValues zero or even number of arguments representing symbolic key
   *                  (eg. property name) and the corresponding validation error message.
   */
  public TriggerValidationException(String name, String... keyValues) {
    super();
    this.name = name;
    if (keyValues == null || keyValues.length == 0) {
      return;
    }
    if (keyValues.length % 2 != 0) {
      throw new IllegalArgumentException("number of arguments representing key & value pairs must be even");
    }
    for (int i = 0; i < keyValues.length; i += 2) {
      details.put(keyValues[i], keyValues[i + 1]);
    }
  }

  public Map<String, String> getDetails() {
    return details;
  }

  @Override
  public String toString() {
    return "TriggerValidationException{" +
        "name=" + name +
        ", details='" + details + '\'' +
        '}';
  }
}
