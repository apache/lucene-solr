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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class TriggerUtils {
  // validation helper methods

  public static void requiredProperties(Set<String> required, Set<String> valid, String... propertyNames) {
    required.addAll(Arrays.asList(propertyNames));
    valid.addAll(Arrays.asList(propertyNames));
  }

  public static void validProperties(Set<String> valid, String... propertyNames) {
    valid.addAll(Arrays.asList(propertyNames));
  }

  public static void checkProperties(Map<String, Object> properties, Map<String, String> results, Set<String> required, Set<String> valid) {
    checkValidPropertyNames(properties, results, valid);
    checkRequiredPropertyNames(properties, results, required);
  }

  public static void checkValidPropertyNames(Map<String, Object> properties, Map<String, String> results, Set<String> valid) {
    Set<String> currentNames = new HashSet<>(properties.keySet());
    currentNames.removeAll(valid);
    if (!currentNames.isEmpty()) {
      for (String name : currentNames) {
        results.put(name, "unknown property");
      }
    }
  }

  public static void checkRequiredPropertyNames(Map<String, Object> properties, Map<String, String> results, Set<String> required) {
    Set<String> requiredNames = new HashSet<>(required);
    requiredNames.removeAll(properties.keySet());
    if (!requiredNames.isEmpty()) {
      for (String name : requiredNames) {
        results.put(name, "missing required property");
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void checkProperty(Map<String, Object> properties, Map<String, String> results, String name, boolean required, Class... acceptClasses) {
    Object value = properties.get(name);
    if (value == null) {
      if (required) {
        results.put(name, "missing required value");
      } else {
        return;
      }
    }
    if (acceptClasses == null || acceptClasses.length == 0) {
      return;
    }
    boolean accepted = false;
    for (Class clz : acceptClasses) {
      if (clz.isAssignableFrom(value.getClass())) {
        accepted = true;
        break;
      }
    }
    if (!accepted) {
      results.put(name, "value is not an expected type");
    }
  }
}
