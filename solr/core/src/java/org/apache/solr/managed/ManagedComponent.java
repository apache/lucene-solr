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
 * A managed component.
 */
public interface ManagedComponent {
  Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Unique name of this component. By convention id-s form a dot-separated hierarchy that
   * follows the naming of metric registries and metric names.
   */
  ManagedComponentId getManagedComponentId();

  /**
   * Returns types of management plugins supported by this component. This must always
   * return a non-null collection with at least one entry.
   */
  Collection<String> getManagedResourceTypes();

  /**
   * Set values of managed resource limits.
   * @param limits map of limit names and values
   */
  default void setResourceLimits(Map<String, Object> limits) {
    if (limits == null) {
      return;
    }
    limits.forEach((key, value) -> {
      try {
        setResourceLimit(key, value);
      } catch (Exception e) {
        log.warn("Exception setting resource limit on {}: key={}, value={}, exception={}",
            getManagedComponentId(), key, value, e);
      }
    });
  }

  /**
   * Set value of a managed resource limit.
   * @param key limit name
   * @param value limit value
   */
  void setResourceLimit(String key, Object value) throws Exception;

  /**
   * Returns current values of managed resource limits.
   * @return map where keys are controlled parameters and values are current values of limits
   */
  Map<String, Object> getResourceLimits();

  /**
   * Returns monitored values that are used for calculating optimal settings of managed resource limits.
   * @param params selected monitored parameters, empty collection to return all monitored values
   * @return map of parameter names to current values.
   */
  Map<String, Object> getMonitoredValues(Collection<String> params) throws Exception;

  /**
   * Component context used for managing additional component state.
   * @return component's context
   */
  ManagedContext getManagedContext();
}
