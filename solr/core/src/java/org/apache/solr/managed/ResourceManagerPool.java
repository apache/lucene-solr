package org.apache.solr.managed;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public interface ResourceManagerPool extends Runnable, Closeable {

  public class Context extends ConcurrentHashMap<String, Object> {

  }

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
   * and param/value maps as values.
   */
  Map<String, Map<String, Object>> getCurrentValues() throws InterruptedException;

  /**
   * This returns cumulative monitored values of all resources.
   * <p>NOTE: you must call {@link #getCurrentValues()} first!</p>
   */
  Map<String, Number> getTotalValues() throws InterruptedException;

  /** Get current pool limits. */
  Map<String, Object> getPoolLimits();

  /** Get parameters specified during creation. */
  Map<String, Object> getParams();

  /**
   * Pool limits are defined using controlled tags.
   */
  void setPoolLimits(Map<String, Object> poolLimits);

  /**
   * Pool context used for managing pool state.
   */
  Context getPoolContext();

  /**
   * Resource context used for managing resource state. This context is always present for
   * a resource registered in this pool, and it is unique to this pool.
   * @param name resource name
   */
  Context getResourceContext(String name);
}
