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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A plugin that implements an algorithm for managing a pool of resources of a given type.
 */
public interface ResourceManagerPlugin<T extends ManagedComponent> {

  /** Plugin symbolic type. */
  String getType();

  void init(Map<String, Object> params);

  /**
   * Name of monitored parameters that {@link ManagedComponent}-s managed by this plugin
   * are expected to support.
   */
  Collection<String> getMonitoredParams();
  /**
   * Name of controlled parameters that {@link ManagedComponent}-s managed by this plugin
   * are expected to support.
   */
  Collection<String> getControlledParams();

  /**
   * Return current values of monitored parameters. Note: the resulting map may contain also
   * other implementation-specific parameter values.
   * @param component monitored component
   */
  Map<String, Object> getMonitoredValues(T component) throws Exception;

  default void setResourceLimits(T component, Map<String, Object> limits) throws Exception {
    if (limits == null || limits.isEmpty()) {
      return;
    }
    for (Map.Entry<String, Object> entry : limits.entrySet()) {
      setResourceLimit(component, entry.getKey(), entry.getValue());
    }
  }

  void setResourceLimit(T component, String limitName, Object value) throws Exception;

  Map<String, Object> getResourceLimits(T component) throws Exception;

  /**
   * Manage resources in a pool. This method is called periodically by {@link ResourceManager},
   * according to a schedule defined by the pool.
   * @param pool pool instance.
   */
  void manage(ResourceManagerPool pool) throws Exception;

  /**
   * Aggregated current monitored values.
   * <p>Default implementation of this method simply sums up all non-negative numeric values across
   * components and ignores any non-numeric values.</p>
   */
  default Map<String, Object> aggregateTotalValues(Map<String, Map<String, Object>> perComponentValues) {
    // calculate the totals
    Map<String, Object> newTotalValues = new HashMap<>();
    perComponentValues.values().forEach(map -> map.forEach((k, v) -> {
      // only calculate totals for numbers
      if (!(v instanceof Number)) {
        return;
      }
      Double val = ((Number)v).doubleValue();
      // -1 and MAX_VALUE are our special guard values
      if (val < 0 || val.longValue() == Long.MAX_VALUE || val.longValue() == Integer.MAX_VALUE) {
        return;
      }
      newTotalValues.merge(k, val, (v1, v2) -> ((Number)v1).doubleValue() + ((Number)v2).doubleValue());
    }));
    return newTotalValues;
  }
}
