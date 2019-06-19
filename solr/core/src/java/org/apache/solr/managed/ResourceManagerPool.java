package org.apache.solr.managed;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 *
 */
public class ResourceManagerPool implements Runnable, Closeable {
  private final Map<String, ManagedResource> resources = new ConcurrentHashMap<>();
  private Map<String, Float> limits;
  private final String type;
  private final ResourceManagerPlugin resourceManagerPlugin;
  private final Map<String, Object> params;
  private Map<String, Map<String, Float>> currentValues = null;
  private Map<String, Float> totalValues = null;
  int scheduleDelaySeconds;
  ScheduledFuture<?> scheduledFuture;

  public ResourceManagerPool(ResourceManagerPluginFactory factory, String type, Map<String, Float> limits, Map<String, Object> params) throws Exception {
    this.type = type;
    this.resourceManagerPlugin = factory.create(type, params);
    this.limits = new HashMap<>(limits);
    this.params = new HashMap<>(params);
  }

  public synchronized void addResource(ManagedResource managedResource) {
    if (resources.containsKey(managedResource.getName())) {
      throw new IllegalArgumentException("Pool already has resource '" + managedResource.getName() + "'.");
    }
    Collection<String> types = managedResource.getManagedResourceTypes();
    if (!types.contains(type)) {
      throw new IllegalArgumentException("Pool type '" + type + "' is not supported by the resource " + managedResource.getName());
    }
    resources.put(managedResource.getName(), managedResource);
  }

  public Map<String, ManagedResource> getResources() {
    return Collections.unmodifiableMap(resources);
  }

  public Map<String, Map<String, Float>> getCurrentValues() {
    // collect current values
    currentValues = new HashMap<>();
    for (ManagedResource resource : resources.values()) {
      currentValues.put(resource.getName(), resource.getManagedValues(resourceManagerPlugin.getMonitoredTags()));
    }
    // calculate totals
    totalValues = new HashMap<>();
    currentValues.values().forEach(map -> map.forEach((k, v) -> {
      Float total = totalValues.get(k);
      if (total == null) {
        totalValues.put(k, v);
      } else {
        totalValues.put(k, total + v);
      }
    }));
    return Collections.unmodifiableMap(currentValues);
  }

  /**
   * This returns cumulative values of all resources. NOTE:
   * you must call {@link #getCurrentValues()} first!
   */
  public Map<String, Float> getTotalValues() {
    return Collections.unmodifiableMap(totalValues);
  }

  public Map<String, Float> getLimits() {
    return limits;
  }

  public void setLimits(Map<String, Float> limits) {
    this.limits = new HashMap(limits);
  }

  @Override
  public void run() {
    resourceManagerPlugin.manage(this);
  }

  @Override
  public void close() throws IOException {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
      scheduledFuture = null;
    }
  }
}
