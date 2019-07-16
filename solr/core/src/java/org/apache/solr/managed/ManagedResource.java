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
package org.apache.solr.managed;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A managed resource.
 */
public interface ManagedResource {
  Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Unique name of this resource.
   */
  String getResourceName();

  /**
   * Returns types of management plugins supported by this resource. This must always
   * return a non-null collection with at least one entry.
   */
  Collection<String> getManagedResourceTypes();

  /**
   * Set values of managed limits.
   * @param limits map of limit names and values
   */
  default void setManagedLimits(Map<String, Object> limits) {
    if (limits == null) {
      return;
    }
    limits.forEach((key, value) -> {
      try {
        setManagedLimit(key, value);
      } catch (Exception e) {
        log.warn("Exception setting managed limit on {}: key={}, value={}, exception={}",
            getResourceName(), key, value, e);
      }
    });
  }

  /**
   * Set value of a managed limit.
   * @param key limit name
   * @param value limit value
   */
  void setManagedLimit(String key, Object value) throws Exception;

  /**
   * Returns current values of managed limits.
   * @return map where keys are controlled tags and values are current limits
   */
  Map<String, Object> getManagedLimits();

  /**
   * Returns monitored values that are used for calculating optimal settings of managed limits.
   * @param tags monitored tags
   * @return map of tags to current values.
   */
  Map<String, Object> getMonitoredValues(Collection<String> tags) throws Exception;
}
