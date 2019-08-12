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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages a pool of resources of the same type, which use the same
 * {@link ResourceManagerPlugin} for managing their resource limits.
 */
public class DefaultResourceManagerPool implements ResourceManagerPool {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, ManagedComponent> components = new ConcurrentHashMap<>();
  private Map<String, Object> poolLimits;
  private final String type;
  private final Class<? extends ManagedComponent> componentClass;
  private final String name;
  private final ResourceManagerPlugin resourceManagerPlugin;
  private final Map<String, Object> args;
  private final ManagedContext poolContext = new ManagedContext();
  private Map<String, Float> totalValues = null;
  private final ReentrantLock updateLock = new ReentrantLock();
  int scheduleDelaySeconds;
  ScheduledFuture<?> scheduledFuture;

  /**
   * Create a pool of resources to manage.
   * @param name unique name of the pool
   * @param type one of the supported pool types (see {@link ResourceManagerPluginFactory})
   * @param factory factory of {@link ResourceManagerPlugin}-s of the specified type
   * @param poolLimits pool limits (keys are controlled tags)
   * @param args parameters for the {@link ResourceManagerPlugin}
   * @throws Exception when initialization of the management plugin fails.
   */
  public DefaultResourceManagerPool(String name, String type, ResourceManagerPluginFactory factory, Map<String, Object> poolLimits, Map<String, Object> args) throws Exception {
    this.name = name;
    this.type = type;
    this.resourceManagerPlugin = factory.create(type, args);
    this.componentClass = factory.getComponentClassByType(type);
    this.poolLimits = new TreeMap<>(poolLimits);
    this.args = new HashMap<>(args);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public Map<String, Object> getParams() {
    return args;
  }

  @Override
  public ResourceManagerPlugin getResourceManagerPlugin() {
    return resourceManagerPlugin;
  }

  @Override
  public void registerComponent(ManagedComponent managedComponent) {
    if (!componentClass.isAssignableFrom(managedComponent.getClass())) {
      log.debug("Pool type '" + type + "' is not supported by the component " + managedComponent.getManagedComponentId());
      return;
    }
    ManagedComponent existing = components.putIfAbsent(managedComponent.getManagedComponentId().toString(), managedComponent);
    if (existing != null) {
      throw new IllegalArgumentException("Component '" + managedComponent.getManagedComponentId() + "' already exists in pool '" + name + "' !");
    }
  }

  @Override
  public boolean unregisterComponent(String name) {
    return components.remove(name) != null;
  }

  @Override
  public boolean isRegistered(String componentId) {
    return components.containsKey(componentId);
  }

  @Override
  public Map<String, ManagedComponent> getComponents() {
    return Collections.unmodifiableMap(components);
  }

  @Override
  public Map<String, Map<String, Object>> getCurrentValues() throws InterruptedException {
    updateLock.lockInterruptibly();
    try {
      // collect the current values
      Map<String, Map<String, Object>> currentValues = new HashMap<>();
      for (ManagedComponent managedComponent : components.values()) {
        try {
          currentValues.put(managedComponent.getManagedComponentId().toString(), resourceManagerPlugin.getMonitoredValues(managedComponent));
        } catch (Exception e) {
          log.warn("Error getting managed values from " + managedComponent.getManagedComponentId(), e);
        }
      }
      // calculate the totals
      Map<String, Float> newTotalValues = new HashMap<>();
      currentValues.values().forEach(map -> map.forEach((k, v) -> {
        // only calculate totals for numbers
        if (!(v instanceof Number)) {
          return;
        }
        Float val = ((Number)v).floatValue();
        // -1 and MAX_VALUE are our special guard values
        if (val < 0 || val.longValue() == Long.MAX_VALUE || val.longValue() == Integer.MAX_VALUE) {
          return;
        }
        Float total = newTotalValues.get(k);
        if (total == null) {
          newTotalValues.put(k, val);
        } else {
          newTotalValues.put(k, total + val);
        }
      }));
      totalValues = newTotalValues;
      return Collections.unmodifiableMap(currentValues);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public Map<String, Number> getTotalValues() throws InterruptedException {
    updateLock.lockInterruptibly();
    try {
      return Collections.unmodifiableMap(totalValues);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public Map<String, Object> getPoolLimits() {
    return poolLimits;
  }

  @Override
  public void setPoolLimits(Map<String, Object> poolLimits) {
    this.poolLimits = new HashMap(poolLimits);
  }

  @Override
  public ManagedContext getPoolContext() {
    return poolContext;
  }

  @Override
  public void run() {
    try {
      resourceManagerPlugin.manage(this);
    } catch (Exception e) {
      log.warn("Error running management plugin " + getName(), e);
    }
  }

  @Override
  public void close() throws IOException {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
      scheduledFuture = null;
    }
    components.clear();
    poolContext.clear();
  }
}
