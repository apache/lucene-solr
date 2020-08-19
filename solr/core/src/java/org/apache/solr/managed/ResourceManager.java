/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.managed;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.client.solrj.request.beans.ResourcePoolConfig;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.schema.FieldType;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for resource management. It uses a flat model where there are named
 * resource pools of a given type, each pool with its own defined resource limits. Components can be added
 * to a pool for the management of a specific aspect of that component.
 */
public abstract class ResourceManager implements PluginInfoInitialized, SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String POOL_LIMITS_PARAM = "poolLimits";
  public static final String POOL_PARAMS_PARAM = "poolParams";

  protected PluginInfo pluginInfo;
  protected SolrMetricsContext metricsContext;
  protected ReentrantLock updateLock = new ReentrantLock();
  protected AtomicBoolean isClosed = new AtomicBoolean();
  protected boolean enabled = true;
  protected AtomicBoolean initialized = new AtomicBoolean();

  /**
   * Create and initialize a resource manager. This also creates default pools.
   * @param loader SolrResourceLoader instance
   * @param timeSource time source instance
   * @param pluginInfo resource manager plugin info
   */
  public static ResourceManager load(SolrResourceLoader loader, SolrMetricManager metricManager, TimeSource timeSource,
                                     PluginInfo pluginInfo) {
    Class<? extends ResourceManager> resourceManagerClass = DefaultResourceManager.class;
    Map<String, Object> pluginMap = new HashMap<>();
    pluginInfo.toMap(pluginMap);
    if (pluginMap.containsKey(FieldType.CLASS_NAME)) {
      resourceManagerClass = loader.findClass((String)pluginMap.get(FieldType.CLASS_NAME), ResourceManager.class);
    }

    ResourceManager resourceManager = loader.newInstance(
        resourceManagerClass.getName(),
        resourceManagerClass,
        null,
        new Class[]{SolrResourceLoader.class, TimeSource.class},
        new Object[]{loader, timeSource});
    SolrPluginUtils.invokeSetters(resourceManager, pluginInfo.initArgs);
    resourceManager.init(pluginInfo);
    resourceManager.initializeMetrics(new SolrMetricsContext(metricManager, "node", SolrMetricProducer.getUniqueMetricTag(loader, null)), null);
    resourceManager.setPoolConfigs(resourceManager.getDefaultPoolConfigs());
    return resourceManager;
  }

  /**
   * Get the configuration of default pools. These pools are protected from removal, only their parameters and
   * limits can be modified.
   */
  public abstract Map<String, ResourcePoolConfig> getDefaultPoolConfigs();

  /**
   * Set new pool configurations. This removes pools that no longer exist in the new configuration (except for
   * the default pools listed in {@link #getDefaultPoolConfigs()}), and then
   * creates new pools, or updates limits and parameters of existing pools.
   * @param poolConfigs new pool configurations - a map where keys are pool names and values are maps containing pool
   *                    configurations.
   */
  public void setPoolConfigs(Map<String, ResourcePoolConfig> poolConfigs) {
    ensureActive();
    if (poolConfigs != null) {
      updateLock.lock();
      try {
        // remove pools no longer present in the config - EXCEPT the default pools
        listPools().stream()
            .filter(p -> !getDefaultPoolConfigs().containsKey(p))
            .filter(p -> !poolConfigs.containsKey(p))
            .forEach(p -> {
              try {
                removePool(p);
              } catch (Exception e) {
                log.warn("Exception removing pool {}, ignoring: {}", p, e);
              }
            });
        // add or modify existing pools
        for (String poolName : poolConfigs.keySet()) {
          ResourcePoolConfig config = poolConfigs.get(poolName);
          if (config == null) {
            log.warn("Pool '{}}' configuration missing, skipping: {}", poolName, poolConfigs);
            continue;
          }
          if (config.type == null || config.type.isBlank()) {
            log.warn("Pool '" + poolName + "' type is missing, skipping: " + config);
            continue;
          }
          ResourceManagerPool pool = getPool(poolName);
          if (pool != null) {
            // pool already exists, modify if needed
            if (!pool.getPoolLimits().equals(config.poolLimits) || !pool.getPoolParams().equals(config.poolParams)) {
              pool.setPoolLimits(config.poolLimits);
              pool.setPoolParams(config.poolParams);
              log.debug("Updated limits and params of pool {}", poolName);
            }
          } else {
            // create new pool
            try {
              createPool(poolName, config.type, config.poolLimits, config.poolParams);
            } catch (Exception e) {
              log.warn("Failed to create resource manager pool '" + poolName + "'", e);
            }
          }
        }
      } finally {
        updateLock.unlock();
      }
    }
  }

  @Override
  public void init(PluginInfo info) {
    if (info != null) {
      this.pluginInfo = info.copy();
    }
    if (!enabled) {
      log.debug("Resource manager " + getClass().getSimpleName() + " disabled.");
      return;
    }
    try {
      doInit();
      initialized.set(true);
    } catch (Exception e) {
      log.warn("Exception initializing resource manager " + getClass().getSimpleName() + ", disabling!");
      try {
        close();
      } catch (Exception e1) {
        log.debug("Exception closing resource manager " + getClass().getSimpleName(), e1);
      }
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

  public boolean isEnabled() {
    return enabled;
  }

  public PluginInfo getPluginInfo() {
    return pluginInfo;
  }

  /**
   * This method is called after plugin info and setters are invoked, but before metrics are initialized.
   * @throws Exception on fatal errors during initialization
   */
  protected abstract void doInit() throws Exception;

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String childScope) {
    metricsContext = parentContext.getChildContext(this, "manager");
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return metricsContext;
  }

  /**
   * Return a factory for creating specialized pools.
   */
  public abstract ResourceManagerPoolFactory getResourceManagerPoolFactory();

  /**
   * Create a named resource management pool.
   * @param name pool name (must not be empty)
   * @param type pool type (one of the supported {@link ResourceManagerPool} types)
   * @param poolLimits pool limits (must not be null)
   * @param poolParams other parameters (must not be null).
   * @return newly created and scheduled resource pool
   */
  public abstract ResourceManagerPool<? extends ManagedComponent> createPool(String name, String type, Map<String, Object> poolLimits, Map<String, Object> poolParams) throws Exception;

  /**
   * List all currently existing pool names.
   */
  public abstract Collection<String> listPools();

  /** Return a named pool or null if no such pool exists. */
  public abstract ResourceManagerPool getPool(String name);

  /** Returns true if a pool with this name exists, false otherwise. */
  public boolean hasPool(String name) {
    return getPool(name) != null;
  }

  /**
   * Modify pool limits of an existing pool.
   * @param name existing pool name
   * @param poolLimits new pool limits. By convention only the values present in this map will be modified,
   *                   all other limits will remain unchanged. In order to remove a limit use null value.
   */
  public abstract void setPoolLimits(String name, Map<String, Object> poolLimits) throws Exception;

  /**
   * Modify parameters of an existing pool.
   * @param name existing pool name
   * @param poolParams new parameter values.By convention only the values present in this map will be modified,
   *                   all other params will remain unchanged. In order to remove a param use null value.
   * @throws Exception when an invalid value or unsupported parameter is requested, or the parameter
   * value cannot be changed after creation.
   */
  public abstract void setPoolParams(String name, Map<String, Object> poolParams) throws Exception;

  /**
   * Remove pool. This also stops the management of resources registered with that pool.
   * @param name existing pool name.
   */
  public abstract void removePool(String name) throws Exception;

  /**
   * Add a managed component to a pool.
   * @param pool existing pool name.
   * @param managedComponent managed component. The component must support the management type
   *                        used in the selected pool. The component must not be already managed by
   *                        another pool of the same type.
   */
  public abstract void registerComponent(String pool, ManagedComponent managedComponent);

  /**
   * Remove a managed component from a pool.
   * @param pool existing pool name.
   * @param componentId component id to remove.
   * @return true if a component was actually registered and has been removed.
   */
  public boolean unregisterComponent(String pool, ManagedComponentId componentId) {
    return unregisterComponent(pool, componentId.toString());
  }

  /**
   * Unregister component from all pools.
   * @param componentId component id.
   * @return true if a component was actually registered and removed from at least one pool.
   */
  public boolean unregisterComponent(String componentId) {
    boolean removed = false;
    for (String pool : listPools()) {
      removed = removed || unregisterComponent(pool, componentId);
    }
    return removed;
  }

  /**
   * Remove a managed component from a pool.
   * @param pool existing pool name.
   * @param componentId component id to remove.
   * @return true if a component was actually registered and has been removed.
   */
  public abstract boolean unregisterComponent(String pool, String componentId);

  protected void ensureActive() {
    if (!enabled) {
      throw new IllegalStateException("Resource manager is disabled.");
    }
    if (isClosed.get()) {
      throw new IllegalStateException("Resource manager is already closed.");
    }
    if (!initialized.get()) {
      throw new IllegalStateException("Resource manager not initialized.");
    }
  }

  @Override
  public void close() throws Exception {
    isClosed.set(true);
    SolrMetricProducer.super.close();
  }
}
