package org.apache.solr.managed.types;

import org.apache.lucene.util.Accountable;
import org.apache.solr.managed.ManagedComponent;

/**
 *
 */
public interface ManagedCacheComponent extends ManagedComponent, Accountable {
  /** Set maximum cache size limit (number of items). */
  void setMaxSize(int size);
  /** Set maximum cache size limit (in MB of RAM). */
  void setMaxRamMB(int maxRamMB);
  /** Get the configured maximum size limit (number of items). */
  int getMaxSize();
  /** Get the configured maximym size limit (in MB of RAM). */
  int getMaxRamMB();
  /** Get the current number of items in cache. */
  int getSize();
  /** Get the ratio of hits to lookups. */
  float getHitRatio();
}
