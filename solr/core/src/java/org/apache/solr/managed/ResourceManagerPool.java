package org.apache.solr.managed;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
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
 * A pool of resources of the same type, which use the same {@link ResourceManagerPlugin} for managing their
 * resource use.
 */
public class ResourceManagerPool implements Runnable, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, ManagedResource> resources = new ConcurrentHashMap<>();
  private Map<String, Float> poolLimits;
  private final String type;
  private final String name;
  private final ResourceManagerPlugin resourceManagerPlugin;
  private final Map<String, Object> params;
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
   * @param params parameters for the {@link ResourceManagerPlugin}
   * @throws Exception
   */
  public ResourceManagerPool(String name, String type, ResourceManagerPluginFactory factory, Map<String, Float> poolLimits, Map<String, Object> params) throws Exception {
    this.name = name;
    this.type = type;
    this.resourceManagerPlugin = factory.create(type, params);
    this.poolLimits = new TreeMap<>(poolLimits);
    this.params = new HashMap<>(params);
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public void addResource(ManagedResource managedResource) {
    Collection<String> types = managedResource.getManagedResourceTypes();
    if (!types.contains(type)) {
      log.debug("Pool type '" + type + "' is not supported by the resource " + managedResource.getResourceName());
      return;
    }
    ManagedResource existing = resources.putIfAbsent(managedResource.getResourceName(), managedResource);
    if (existing != null) {
      throw new IllegalArgumentException("Resource '" + managedResource.getResourceName() + "' already exists in pool '" + name + "' !");
    }
  }

  public Map<String, ManagedResource> getResources() {
    return Collections.unmodifiableMap(resources);
  }

  /**
   * Get the current values from all resources. Result is a map with resource names as keys,
   * and tag/value maps as values.
   */
  public Map<String, Map<String, Float>> getCurrentValues() throws InterruptedException {
    updateLock.lockInterruptibly();
    try {
      // collect current values
      Map<String, Map<String, Float>> currentValues = new HashMap<>();
      for (ManagedResource resource : resources.values()) {
        try {
          currentValues.put(resource.getResourceName(), resource.getManagedValues(resourceManagerPlugin.getMonitoredTags()));
        } catch (Exception e) {
          log.warn("Error getting managed values from " + resource.getResourceName(), e);
        }
      }
      // calculate totals
      Map<String, Float> newTotalValues = new HashMap<>();
      currentValues.values().forEach(map -> map.forEach((k, v) -> {
        Float total = newTotalValues.get(k);
        if (total == null) {
          newTotalValues.put(k, v);
        } else {
          newTotalValues.put(k, total + v);
        }
      }));
      totalValues = newTotalValues;
      return Collections.unmodifiableMap(currentValues);
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * This returns cumulative values of all resources. NOTE:
   * you must call {@link #getCurrentValues()} first!
   */
  public Map<String, Float> getTotalValues() throws InterruptedException {
    updateLock.lockInterruptibly();
    try {
      return Collections.unmodifiableMap(totalValues);
    } finally {
      updateLock.unlock();
    }
  }

  public Map<String, Float> getPoolLimits() {
    return poolLimits;
  }

  /**
   * Pool limits are defined using controlled tags.
   */
  public void setPoolLimits(Map<String, Float> poolLimits) {
    this.poolLimits = new HashMap(poolLimits);
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
  }
}
