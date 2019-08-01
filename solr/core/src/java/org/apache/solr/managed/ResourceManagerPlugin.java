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

}
