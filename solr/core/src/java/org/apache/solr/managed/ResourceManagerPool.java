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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class pools that manage a specific component class.
 */
public abstract class ResourceManagerPool<T extends ManagedComponent> implements SolrInfoBean, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** How frequently this pool is scheduled - resource manager calls the {@link #manage()} method this often.
   * If this parameter is set to less than 1 the scheduling of this pool is disabled.
   */
  public static final String SCHEDULE_DELAY_SECONDS_PARAM = "scheduleDelaySeconds";
  /** Default schedule delay. */
  public static final int DEFAULT_SCHEDULE_DELAY_SECONDS = 10;

  protected final String name;
  protected final String type;
  protected final Map<String, Object> poolLimits = new ConcurrentHashMap<>();
  protected final Map<String, T> components = new ConcurrentHashMap<>();
  protected final Map<String, Map<String, Object>> initialComponentLimits = new ConcurrentHashMap<>();
  protected final ResourceManager resourceManager;
  protected final Class<? extends ManagedComponent> componentClass;
  protected final Map<String, Object> poolParams = new ConcurrentHashMap<>();
  protected final ResourcePoolContext poolContext = new ResourcePoolContext();
  protected final Set<ChangeListener> listeners = new CopyOnWriteArraySet<>();
  protected int scheduleDelaySeconds;
  protected ScheduledFuture<?> scheduledFuture;
  protected SolrMetricsContext solrMetricsContext;
  protected Timer manageTimer;
  protected Meter manageErrors;
  protected final Map<ChangeListener.Reason, AtomicLong> changeCounts = new ConcurrentHashMap<>();

  public ResourceManagerPool(String name, String type, ResourceManager resourceManager,
                                Map<String, Object> poolLimits, Map<String, Object> poolParams) {
    Objects.nonNull(name);
    Objects.nonNull(type);
    Objects.nonNull(resourceManager);

    this.name = name;
    this.type = type;
    this.resourceManager = resourceManager;
    this.componentClass = resourceManager.getResourceManagerPoolFactory().getComponentClassByType(type);
    Objects.nonNull(componentClass);

    setPoolLimits(poolLimits);
    setPoolParams(poolParams);
  }

  /** Unique pool name. */
  public String getName() {
    return name;
  }

  /** Pool type. */
  public String getType() {
    return type;
  }

  public Category getCategory() {
    return Category.RESOURCE;
  }

  public String getDescription() {
    return getName() + "/" + getType() + " (" + getClass().getSimpleName() + ")";
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String childScope) {
    solrMetricsContext = parentContext.getChildContext(this, childScope);
    manageTimer = solrMetricsContext.timer("manageTimes", getCategory().toString(), "pool", getType());
    manageErrors = solrMetricsContext.meter("manageErrors", getCategory().toString(), "pool", getType());
    MetricsMap changeMap = new MetricsMap((detailed, map) -> changeCounts.forEach((k, v) -> map.put(k.toString(), v.get())));
    solrMetricsContext.gauge(changeMap, true, "changes", getCategory().toString(), "pool", getType());
    solrMetricsContext.gauge(new MetricsMap((detailed, map) -> map.putAll(poolLimits)), true, "poolLimits", getCategory().toString(), "pool", getType());
    solrMetricsContext.gauge(new MetricsMap((detailed, map) -> {
      try {
        Map<String, Object> totalValues = aggregateTotalValues(getCurrentValues());
        map.putAll(totalValues);
      } catch (Exception e) {
        log.warn("Exception retrieving current values in pool {} / {}: {}", getName(), getType(), e);
      }
    }), true, "totalValues", getCategory().toString(), "pool", getType());
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  /**
   * Return instance of the resource manager where this pool is managed.
   */
  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  /** Add component to this pool. Note: component class must be a subclass of the type that this pool can manage. */
  public void registerComponent(T managedComponent) {
    if (!componentClass.isAssignableFrom(managedComponent.getClass())) {
      log.warn("Pool type '" + type + "' is not supported by the component " + managedComponent.getManagedComponentId());
      return;
    }
    ManagedComponent existing = components.putIfAbsent(managedComponent.getManagedComponentId().toString(), managedComponent);
    if (existing != null) {
      throw new IllegalArgumentException("Component '" + managedComponent.getManagedComponentId() + "' already exists in pool '" + name + "' !");
    }
    try {
      initialComponentLimits.put(managedComponent.getManagedComponentId().toString(), getResourceLimits(managedComponent));
    } catch (Exception e) {
      log.warn("Could not retrieve initial component limits for {} / {}: {}", managedComponent.getManagedComponentId(), getName(), e);
    }
  }

  /** Remove named component from this pool. */
  public boolean unregisterComponent(String componentId) {
    initialComponentLimits.remove(componentId);
    return components.remove(componentId) != null;
  }

  public Map<String, Object> getInitialResourceLimits(String componentId) {
    return initialComponentLimits.getOrDefault(componentId, Collections.emptyMap());
  }

  /**
   * Check whether a named component is registered in this pool.
   * @param componentId component id
   * @return true if the component with this name is registered, false otherwise.
   */
  public boolean isRegistered(String componentId) {
    return components.containsKey(componentId);
  }

  /** Get components managed by this pool. */
  public Map<String, T> getComponents() {
    return Collections.unmodifiableMap(components);
  }

  public void addChangeListener(ChangeListener listener) {
    listeners.add(listener);
  }

  public void removeChangeListener(ChangeListener listener) {
    listeners.remove(listener);
  }


  /**
   * Get the current monitored values from all resources. Result is a map with resource names as keys,
   * and param/value maps as values.
   */
  public Map<String, Map<String, Object>> getCurrentValues() throws InterruptedException {
    // collect the current values
    Map<String, Map<String, Object>> currentValues = new HashMap<>();
    for (T managedComponent : components.values()) {
      try {
        currentValues.put(managedComponent.getManagedComponentId().toString(), getMonitoredValues(managedComponent));
      } catch (Exception e) {
        log.warn("Error getting managed values from " + managedComponent.getManagedComponentId(), e);
      }
    }
    return Collections.unmodifiableMap(currentValues);
  }

  /**
   * Return values of monitored parameters of this component.
   * @param component component instance.
   * @return current monitored values.
   */
  public abstract Map<String, Object> getMonitoredValues(T component) throws Exception;

  /**
   * Set resource limits for a component managed by this pool.
   * @param component component instance
   * @param limits map of limit name to the new limit value
   * @param reason reason for the change
   */
  public void setResourceLimits(T component, Map<String, Object> limits, ChangeListener.Reason reason) throws Exception {
    if (limits == null || limits.isEmpty()) {
      return;
    }
    for (Map.Entry<String, Object> entry : limits.entrySet()) {
      setResourceLimit(component, entry.getKey(), entry.getValue(), reason);
    }
  }

  /**
   * Set resource limit for a component managed by this pool.
   * @param component component instance.
   * @param limitName limit name.
   * @param value new limit value.
   * @param reason reason for the change.
   * @return the actual limit value accepted by the component.
   */
  public Object setResourceLimit(T component, String limitName, Object value, ChangeListener.Reason reason) throws Exception {
    try {
      Object newActualLimit = doSetResourceLimit(component, limitName, value);
      changeCounts.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
      for (ChangeListener listener : listeners) {
        listener.changedLimit(getName(), component, limitName, value, newActualLimit, reason);
      }
      return newActualLimit;
    } catch (Throwable t) {
      for (ChangeListener listener : listeners) {
        listener.onError(getName(), component, limitName, value, reason, t);
      }
      throw t;
    }
  }

  protected abstract Object doSetResourceLimit(T component, String limitName, Object value) throws Exception;

  /**
   * Get current resource limits for a component.
   * @param component component managed by this pool.
   * @return map of limit names to limit values.
   */
  public abstract Map<String, Object> getResourceLimits(T component) throws Exception;

  /**
   * Calculate aggregated monitored values.
   * <p>Default implementation of this method simply sums up all non-negative numeric values across
   * components and ignores any negative or non-numeric values. Values equal to {@link Integer#MAX_VALUE} or
   * {@link Long#MAX_VALUE} are also ignored.</p>
   * @param perComponentValues per-component monitored values, as returned by {@link #getCurrentValues()}.
   * @return aggregated monitored values.
   */
  public Map<String, Object> aggregateTotalValues(Map<String, Map<String, Object>> perComponentValues) {
    // calculate the totals
    Map<String, Object> newTotalValues = new HashMap<>();
    perComponentValues.values().forEach(map -> map.forEach((k, v) -> {
      // only calculate totals for numbers
      if (!(v instanceof Number)) {
        return;
      }
      Number val = (Number)v;
      // -1 and MAX_VALUE are our special guard values
      if (val.longValue() < 0 || val.longValue() == Long.MAX_VALUE || val.intValue() == Integer.MAX_VALUE) {
        return;
      }
      newTotalValues.merge(k, val, (v1, v2) -> ((Number)v1).doubleValue() + ((Number)v2).doubleValue());
    }));
    return newTotalValues;
  }

  /** Get current pool limits.
   * @return map of limit names to (total) limit values for this pool.
   */
  public Map<String, Object> getPoolLimits() {
    return Collections.unmodifiableMap(poolLimits);
  }

  /**
   * Set new pool limits. By convention only the values present in this map will be modified,
   * all other limits will remain unchanged. In order to remove a limit use null value.
   * Pool limits are defined using controlled limit names.
   * @param poolLimits map of limit names to new (total) limit values for this pool.
   */
  public void setPoolLimits(Map<String, Object> poolLimits) {
    if (poolLimits != null) {
      poolLimits.forEach((k, v) -> {
        if (v == null) {
          this.poolLimits.remove(k);
        } else {
          this.poolLimits.put(k, v);
        }
      });
    }
  }

  /** Get current pool parameters. */
  public Map<String, Object> getPoolParams() {
    return Collections.unmodifiableMap(poolParams);
  }

  /**
   * Set new pool parameters. By convention only the values present in this map will be modified,
   * all other params will remain unchanged. In order to remove a param use null value.
   * <p>Note: when invoking this method directly be sure to check if the pool's schedule parameters have
   * changed and re-schedule the pool if needed.</p>
   */
  public void setPoolParams(Map<String, Object> poolParams) {
    if (poolParams != null) {
      poolParams.forEach((k, v) -> {
        if (v == null) {
          this.poolParams.remove(k);
        } else {
          this.poolParams.put(k, v);
        }
      });
      this.scheduleDelaySeconds = Integer.parseInt(String.valueOf(this.poolParams.getOrDefault(SCHEDULE_DELAY_SECONDS_PARAM, DEFAULT_SCHEDULE_DELAY_SECONDS)));
    }
  }

  /**
   * Pool context used for managing additional pool state.
   */
  public ResourcePoolContext getResourcePoolContext() {
    return poolContext;
  }

  /**
   * Manage resources used by components in this pool. This method is periodically called according to
   * {@link #SCHEDULE_DELAY_SECONDS_PARAM}.
   */
  public void manage() {
    Timer.Context ctx = manageTimer.time();
    try {
      doManage();
    } catch (Exception e) {
      log.warn("Exception caught managing pool " + getName(), e);
      manageErrors.mark();
    } finally {
      long time = ctx.stop();
      long timeSec = TimeUnit.NANOSECONDS.toSeconds(time);
      if (timeSec > scheduleDelaySeconds) {
        log.warn("Execution of pool " + getName() + "/" + getType() + " took " + timeSec +
            " seconds, which is longer than the schedule delay of " + scheduleDelaySeconds + " seconds!");
      }
    }
  }

  /**
   * Implementation-specific resource management logic for this type of components.
   * @throws Exception on fatal errors.
   */
  protected abstract void doManage() throws Exception;

  @Override
  public void close() throws IOException {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
      scheduledFuture = null;
    }
    components.clear();
    poolContext.clear();
    listeners.clear();
  }
}
