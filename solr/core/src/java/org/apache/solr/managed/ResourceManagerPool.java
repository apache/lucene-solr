package org.apache.solr.managed;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public interface ResourceManagerPool extends Runnable, Closeable {

  /** Unique pool name. */
  String getName();

  /** Pool type. */
  String getType();

  /** Add resource to this pool. */
  void addResource(ManagedResource managedResource);

  /** Remove named resource from this pool. */
  boolean removeResource(String name);

  /** Get resources managed by this pool. */
  Map<String, ManagedResource> getResources();

  /**
   * Get the current monitored values from all resources. Result is a map with resource names as keys,
   * and tag/value maps as values.
   */
  Map<String, Map<String, Object>> getCurrentValues() throws InterruptedException;

  /**
   * This returns cumulative monitored values of all resources.
   * <p>NOTE: you must call {@link #getCurrentValues()} first!</p>
   */
  Map<String, Float> getTotalValues() throws InterruptedException;

  /** Get current pool limits. */
  Map<String, Object> getPoolLimits();

  /** Get parameters specified during creation. */
  Map<String, Object> getArgs();

  /**
   * Pool limits are defined using controlled tags.
   */
  void setPoolLimits(Map<String, Object> poolLimits);
}
