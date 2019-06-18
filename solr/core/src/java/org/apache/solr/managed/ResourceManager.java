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

  void createPool(String name, Limits limits, Map<String, Object> params) throws Exception;

  void modifyPoolLimits(String name, Limits limits) throws Exception;

  void removePool(String name) throws Exception;

  default void addResources(String pool, Collection<ResourceManaged> resourceManaged) {
    ensureNotClosed();
    for (ResourceManaged resource : resourceManaged) {
      addResource(pool, resource);
    }
  }

  void addResource(String pool, ResourceManaged resourceManaged);

  default void ensureNotClosed() {
    if (isClosed()) {
      throw new IllegalStateException("Already closed.");
    }
  }
}
