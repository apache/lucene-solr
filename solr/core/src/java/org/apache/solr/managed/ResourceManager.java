package org.apache.solr.managed;

import java.util.Collection;
import java.util.Map;

import org.apache.solr.common.SolrCloseable;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.util.plugin.PluginInfoInitialized;

/**
 *
 */
public interface ResourceManager extends SolrCloseable, PluginInfoInitialized {

  void setEnabled(Boolean enabled);

  PluginInfo getPluginInfo();

  void createPool(String name, String type, Map<String, Float> limits, Map<String, Object> params) throws Exception;

  void modifyPoolLimits(String name, Map<String, Float> limits) throws Exception;

  void removePool(String name) throws Exception;

  default void addResources(String pool, Collection<ManagedResource> managedResource) {
    ensureNotClosed();
    for (ManagedResource resource : managedResource) {
      addResource(pool, resource);
    }
  }

  void addResource(String pool, ManagedResource managedResource);

  default void ensureNotClosed() {
    if (isClosed()) {
      throw new IllegalStateException("Already closed.");
    }
  }
}
