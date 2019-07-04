package org.apache.solr.managed;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for resource management. It represents a flat model where there are named
 * resource pools of a given type, each pool with its own defined limits. Resources can be added
 * to a pool for management of a specific aspect of that resource using {@link ResourceManagerPlugin}.
 */
public abstract class ResourceManager implements SolrCloseable, PluginInfoInitialized {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected PluginInfo pluginInfo;
  protected boolean isClosed = false;
  protected boolean enabled = true;

  @Override
  public void init(PluginInfo info) {
    if (info != null) {
      this.pluginInfo = info.copy();
      if (pluginInfo.initArgs != null) {
        SolrPluginUtils.invokeSetters(this, this.pluginInfo.initArgs);
      }
    }
    if (!enabled) {
      log.debug("Resource manager " + getClass().getSimpleName() + " disabled.");
      return;
    }
    try {
      doInit();
    } catch (Exception e) {
      log.warn("Exception initializing resource manager " + getClass().getSimpleName() + ", disabling!");
      IOUtils.closeQuietly(this);
    }
  }

  /**
   * Enable resource management, defaults to true. {@link #init(PluginInfo)} checks
   * this flag before calling {@link #doInit()}.
   * @param enabled - whether or not resource management is to be enabled
   */
  public void setEnabled(Boolean enabled) {
    if (enabled != null) {
      this.enabled = enabled;
    }
  }

  public PluginInfo getPluginInfo() {
    return pluginInfo;
  }

  protected abstract void doInit() throws Exception;

  public abstract void createPool(String name, String type, Map<String, Float> poolLimits, Map<String, Object> params) throws Exception;

  public abstract void modifyPoolLimits(String name, Map<String, Float> poolLimits) throws Exception;

  public abstract void removePool(String name) throws Exception;

  public void addResources(String pool, Collection<ManagedResource> managedResource) throws Exception {
    ensureNotClosed();
    for (ManagedResource resource : managedResource) {
      addResource(pool, resource);
    }
  }

  public abstract void addResource(String pool, ManagedResource managedResource) throws Exception;

  protected void ensureNotClosed() {
    if (isClosed()) {
      throw new IllegalStateException("Already closed.");
    }
  }

  @Override
  public synchronized boolean isClosed() {
    return isClosed;
  }
}
