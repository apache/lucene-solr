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
 * Base class for a resource managemer. It uses a flat model where there are named
 * resource pools of a given type, each pool with its own defined resource limits. Resources can be added
 * to a pool for the management of a specific aspect of that resource using {@link ResourceManagerPlugin}.
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

  /**
   * Create a named resource management pool.
   * @param name pool name
   * @param type pool type (one of the supported {@link ResourceManagerPlugin} types)
   * @param poolLimits pool limits
   * @param params other parameters. These are also used for creating a {@link ResourceManagerPlugin}
   */
  public abstract void createPool(String name, String type, Map<String, Object> poolLimits, Map<String, Object> params) throws Exception;

  /**
   * Modify pool limits.
   * @param name existing pool name
   * @param poolLimits new pool limits
   */
  public abstract void modifyPoolLimits(String name, Map<String, Object> poolLimits) throws Exception;

  /**
   * Remove pool. This also stops the management of resources registered with that pool.
   * @param name existing pool name
   */
  public abstract void removePool(String name) throws Exception;

  /**
   * Add managed resources to a pool.
   * @param pool existing pool name.
   * @param managedResources resources to add
   */
  public void addResources(String pool, Collection<ManagedResource> managedResources) throws Exception {
    ensureNotClosed();
    for (ManagedResource resource : managedResources) {
      addResource(pool, resource);
    }
  }

  /**
   * Add a managed resource to a pool.
   * @param pool existing pool name.
   * @param managedResource managed resource. The resource must support the management type
   *                        (in its {@link ManagedResource#getManagedResourceTypes()}) used
   *                        in the selected pool. The resource must not be already managed by
   *                        another pool of the same type.
   */
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
