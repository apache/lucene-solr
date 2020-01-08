package org.apache.solr.managed;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.codahale.metrics.Timer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class ResourceManagerPool<T extends ManagedComponent> implements SolrInfoBean, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final String name;
  protected final String type;
  protected Map<String, Object> poolLimits;
  protected final Map<String, T> components = new ConcurrentHashMap<>();
  protected final ResourceManager resourceManager;
  protected final Class<? extends ManagedComponent> componentClass;
  private final Map<String, Object> poolParams;
  protected final ResourcePoolContext poolContext = new ResourcePoolContext();
  protected final Set<ChangeListener> listeners = new CopyOnWriteArraySet<>();
  protected final ReentrantLock manageLock = new ReentrantLock();
  protected int scheduleDelaySeconds;
  protected ScheduledFuture<?> scheduledFuture;
  protected SolrMetricsContext solrMetricsContext;
  protected Timer manageTimer;
  protected Map<ChangeListener.Reason, AtomicLong> changeCounts = new ConcurrentHashMap<>();

  public ResourceManagerPool(String name, String type, ResourceManager resourceManager,
                                Map<String, Object> poolLimits, Map<String, Object> poolParams) {
    this.name = name;
    this.type = type;
    this.resourceManager = resourceManager;
    this.componentClass = resourceManager.getResourceManagerPoolFactory().getComponentClassByType(type);
    this.poolLimits = new HashMap<>(poolLimits);
    this.poolParams = new HashMap<>(poolParams);
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
    MetricsMap changeMap = new MetricsMap((detailed, map) -> {
      changeCounts.forEach((k, v) -> map.put(k.toString(), v.get()));
    });
    solrMetricsContext.gauge(changeMap, true, "changes", getCategory().toString(), "pool", getType());
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  /** Add component to this pool. */
  public void registerComponent(T managedComponent) {
    if (!componentClass.isAssignableFrom(managedComponent.getClass())) {
      log.debug("Pool type '" + type + "' is not supported by the component " + managedComponent.getManagedComponentId());
      return;
    }
    ManagedComponent existing = components.putIfAbsent(managedComponent.getManagedComponentId().toString(), managedComponent);
    if (existing != null) {
      throw new IllegalArgumentException("Component '" + managedComponent.getManagedComponentId() + "' already exists in pool '" + name + "' !");
    }
  }

  /** Remove named component from this pool. */
  public boolean unregisterComponent(String componentId) {
    return components.remove(componentId) != null;
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

  public abstract Map<String, Object> getMonitoredValues(T component) throws Exception;

  public void setResourceLimits(T component, Map<String, Object> limits, ChangeListener.Reason reason) throws Exception {
    if (limits == null || limits.isEmpty()) {
      return;
    }
    for (Map.Entry<String, Object> entry : limits.entrySet()) {
      setResourceLimit(component, entry.getKey(), entry.getValue(), reason);
    }
  }

  public Object setResourceLimit(T component, String limitName, Object value, ChangeListener.Reason reason) throws Exception {
    Object newActualLimit = doSetResourceLimit(component, limitName, value);
    changeCounts.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
    for (ChangeListener listener : listeners) {
      listener.changedLimit(getName(), component, limitName, value, newActualLimit, reason);
    }
    return newActualLimit;
  }

  protected abstract Object doSetResourceLimit(T component, String limitName, Object value) throws Exception;

  public abstract Map<String, Object> getResourceLimits(T component) throws Exception;

  /**
   * Calculate aggregated monitored values.
   * <p>Default implementation of this method simply sums up all non-negative numeric values across
   * components and ignores any non-numeric values.</p>
   */
  public Map<String, Object> aggregateTotalValues(Map<String, Map<String, Object>> perComponentValues) {
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

  /** Get current pool limits. */
  public Map<String, Object> getPoolLimits() {
    return Collections.unmodifiableMap(poolLimits);
  }

  /**
   * Pool limits are defined using controlled tags.
   */
  public void setPoolLimits(Map<String, Object> poolLimits) {
    this.poolLimits = new HashMap(poolLimits);
  }

  /** Get parameters specified during creation. */
  public Map<String, Object> getParams() {
    return Collections.unmodifiableMap(poolParams);
  }

  /**
   * Pool context used for managing additional pool state.
   */
  public ResourcePoolContext getResourcePoolContext() {
    return poolContext;
  }

  public void manage() {
    manageLock.lock();
    Timer.Context ctx = manageTimer.time();
    try {
      doManage();
    } catch (Exception e) {
      log.warn("Exception caught managing pool " + getName(), e);
    } finally {
      long time = ctx.stop();
      long timeSec = TimeUnit.NANOSECONDS.toSeconds(time);
      if (timeSec > scheduleDelaySeconds) {
        log.warn("Execution of pool " + getName() + "/" + getType() + " took " + timeSec +
            " seconds, which is longer than the schedule delay of " + scheduleDelaySeconds + " seconds!");
      }
      manageLock.unlock();
    }
  }

  protected abstract void doManage() throws Exception;

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
