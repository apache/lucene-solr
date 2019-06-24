package org.apache.solr.managed;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ResourceManagerPool implements Runnable, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, ManagedResource> resources = new ConcurrentHashMap<>();
  private Map<String, Float> limits;
  private final String type;
  private final String name;
  private final ResourceManagerPlugin resourceManagerPlugin;
  private final Map<String, Object> params;
  private Map<String, Float> totalValues = null;
  int scheduleDelaySeconds;
  ScheduledFuture<?> scheduledFuture;

  public ResourceManagerPool(String name, String type, ResourceManagerPluginFactory factory, Map<String, Float> limits, Map<String, Object> params) throws Exception {
    this.name = name;
    this.type = type;
    this.resourceManagerPlugin = factory.create(type, params);
    this.limits = new HashMap<>(limits);
    this.params = new HashMap<>(params);
  }

  public String getName() {
    return name;
  }

  public void addResource(ManagedResource managedResource) {
    Collection<String> types = managedResource.getManagedResourceTypes();
    if (!types.contains(type)) {
      log.debug("Pool type '" + type + "' is not supported by the resource " + managedResource.getName());
      return;
    }
    ManagedResource existing = resources.putIfAbsent(managedResource.getName(), managedResource);
    if (existing != null) {
      throw new IllegalArgumentException("Resource '" + managedResource.getName() + "' already exists in pool '" + name + "' !");
    }
  }

  public Map<String, ManagedResource> getResources() {
    return Collections.unmodifiableMap(resources);
  }

  public Map<String, Map<String, Float>> getCurrentValues() {
    // collect current values
    Map<String, Map<String, Float>> currentValues = new HashMap<>();
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
