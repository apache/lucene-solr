package org.apache.solr.managed;

import java.io.Closeable;
import java.util.Map;

/**
 *
 */
public interface ResourceManagerPool extends Runnable, Closeable {

  /** Unique pool name. */
  String getName();

  /** Pool type. */
  String getType();

  ResourceManagerPlugin getResourceManagerPlugin();

  /** Add component to this pool. */
  void registerComponent(ManagedComponent managedComponent);

  /** Remove named component from this pool. */
  boolean unregisterComponent(String componentId);

  /**
   * Check whether a named component is registered in this pool.
   * @param componentId component id
   * @return true if the component with this name is registered, false otherwise.
   */
  boolean isRegistered(String componentId);

  /** Get components managed by this pool. */
  Map<String, ManagedComponent> getComponents();

  /**
   * Get the current monitored values from all resources. Result is a map with resource names as keys,
   * and param/value maps as values.
   */
  Map<String, Map<String, Object>> getCurrentValues() throws InterruptedException;

  /**
   * This returns cumulative monitored values of all components.
   * <p>NOTE: you MUST call {@link #getCurrentValues()} first!</p>
   */
  Map<String, Object> getTotalValues() throws InterruptedException;

  /** Get current pool limits. */
  Map<String, Object> getPoolLimits();

  /** Get parameters specified during creation. */
  Map<String, Object> getParams();

  /**
   * Pool limits are defined using controlled tags.
   */
  void setPoolLimits(Map<String, Object> poolLimits);

  /**
   * Pool context used for managing additional pool state.
   */
  ManagedContext getPoolContext();
}
