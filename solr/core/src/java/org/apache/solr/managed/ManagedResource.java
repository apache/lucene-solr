package org.apache.solr.managed;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public interface ManagedResource {
  Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Unique name of this resource.
   */
  String getResourceName();

  /**
   * Returns types of management schemes supported by this resource. This always
   * returns a non-null collection with at least one entry.
   */
  Collection<String> getManagedResourceTypes();

  /**
   * Set current managed limits.
   * @param limits map of limit names and values
   */
  default void setManagedLimits(Map<String, Float> limits) {
    if (limits == null) {
      return;
    }
    limits.forEach((key, value) -> {
      try {
        setManagedLimit(key, value);
      } catch (Exception e) {
        log.warn("Exception setting managed limit on {}: key={}, value={}, exception={}",
            getResourceName(), key, value, e);
      }
    });
  }

  /**
   * Set a managed limit.
   * @param key limit name
   * @param value limit value
   */
  void setManagedLimit(String key, float value) throws Exception;

  /**
   * Returns current managed limits.
   */
  Map<String, Float> getManagedLimits();

  /**
   * Returns monitored values that are used for calculating optimal setting of managed limits.
   * @param tags value names
   * @return map of names to current values.
   */
  Map<String, Float> getManagedValues(Collection<String> tags) throws Exception;
}
