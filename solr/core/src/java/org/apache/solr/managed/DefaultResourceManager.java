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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.managed.types.CacheManagerPool;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.search.SolrCache;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link ResourceManager}.
 * <p>Resource pools managed by this implementation are run periodically, each according to
 * its schedule defined by {@link #SCHEDULE_DELAY_SECONDS_PARAM} parameter during the pool creation.</p>
 */
public class DefaultResourceManager extends ResourceManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public static final String SCHEDULE_DELAY_SECONDS_PARAM = "scheduleDelaySeconds";
  public static final String MAX_NUM_POOLS_PARAM = "maxNumPools";

  public static final int DEFAULT_MAX_POOLS = 200;
  public static final int DEFAULT_SCHEDULE_DELAY_SECONDS = 10;

  public static final String DOCUMENT_CACHE_POOL = "searcherDocumentCache";
  public static final String FILTER_CACHE_POOL = "searcherFilterCache";
  public static final String FIELD_VALUE_CACHE_POOL = "searcherFieldValueCache";
  public static final String QUERY_RESULT_CACHE_POOL = "searcherQueryResultCache";
  public static final String USER_SEARCHER_CACHE_POOL = "searcherUserCache";

  public static final Map<String, Map<String, Object>> DEFAULT_NODE_POOLS = new HashMap<>();

  static {
    Map<String, Object> params = new HashMap<>();
    params.put(CommonParams.TYPE, CacheManagerPool.TYPE);
    // unlimited RAM
    params.put(SolrCache.MAX_RAM_MB_PARAM, -1L);
    // unlimited size
    params.put(SolrCache.MAX_SIZE_PARAM, -1L);
    DEFAULT_NODE_POOLS.put(DOCUMENT_CACHE_POOL, new HashMap<>(params));
    DEFAULT_NODE_POOLS.put(FILTER_CACHE_POOL, new HashMap<>(params));
    DEFAULT_NODE_POOLS.put(FIELD_VALUE_CACHE_POOL, new HashMap<>(params));
    DEFAULT_NODE_POOLS.put(QUERY_RESULT_CACHE_POOL, new HashMap<>(params));
    DEFAULT_NODE_POOLS.put(USER_SEARCHER_CACHE_POOL, new HashMap<>(params));
  }


  protected int maxNumPools = DEFAULT_MAX_POOLS;

  protected Map<String, ResourceManagerPool> resourcePools = new ConcurrentHashMap<>();


  private TimeSource timeSource;

  /**
   * Thread pool for scheduling the pool runs.
   */
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  protected ResourceManagerPoolFactory resourceManagerPoolFactory;
  protected SolrResourceLoader loader;
  protected SolrMetricManager metricManager;


  public DefaultResourceManager(SolrResourceLoader loader, SolrMetricManager metricManager, TimeSource timeSource) {
    this.loader = loader;
    this.timeSource = timeSource;
    this.metricManager = metricManager;
  }

  protected void doInit() throws Exception {
    scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(DEFAULT_NODE_POOLS.size(),
        new DefaultSolrThreadFactory(getClass().getSimpleName()));
    scheduledThreadPoolExecutor.setMaximumPoolSize(maxNumPools);
    scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
    scheduledThreadPoolExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    // TODO: make configurable based on plugin info
    resourceManagerPoolFactory = new DefaultResourceManagerPoolFactory(loader,
        pluginInfo != null ?
            (Map<String, Object>)pluginInfo.initArgs.toMap(new HashMap<>()).getOrDefault("plugins", Collections.emptyMap()) :
            Collections.emptyMap());
    log.info("Resource manager initialized.");
  }

  // setter invoked by PluginUtils.invokeSetters
  public void setMaxNumPools(Integer maxNumPools) {
    if (maxNumPools != null) {
      this.maxNumPools = maxNumPools;
    } else {
      this.maxNumPools = DEFAULT_MAX_POOLS;
    }
  }

  @Override
  public PluginInfo getPluginInfo() {
    return pluginInfo;
  }

  @Override
  public ResourceManagerPoolFactory getResourceManagerPoolFactory() {
    return resourceManagerPoolFactory;
  }

  @Override
  public ResourceManagerPool createPool(String name, String type, Map<String, Object> poolLimits, Map<String, Object> args) throws Exception {
    ensureActive();
    if (resourcePools.containsKey(name)) {
      throw new IllegalArgumentException("Pool '" + name + "' already exists.");
    }
    if (resourcePools.size() >= maxNumPools) {
      throw new IllegalArgumentException("Maximum number of pools (" + maxNumPools + ") reached.");
    }
    ResourceManagerPool newPool = resourceManagerPoolFactory.create(name, type, this, poolLimits, args);
    newPool.scheduleDelaySeconds = Integer.parseInt(String.valueOf(args.getOrDefault(SCHEDULE_DELAY_SECONDS_PARAM, DEFAULT_SCHEDULE_DELAY_SECONDS)));
    ResourceManagerPool oldPool = resourcePools.putIfAbsent(name, newPool);
    if (oldPool != null) {
      // someone else already created it
      IOUtils.closeQuietly(newPool);
      throw new IllegalArgumentException("Pool '" + name + "' already exists.");
    }
    newPool.initializeMetrics(metricsContext, name);
    if (timeSource != null) {
      newPool.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(() -> {
            log.info("- running pool " + newPool.getName() + " / " + newPool.getType());
            newPool.manage();
          }, 0,
          timeSource.convertDelay(TimeUnit.SECONDS, newPool.scheduleDelaySeconds, TimeUnit.MILLISECONDS),
          TimeUnit.MILLISECONDS);
    }
    log.info("- created pool " + newPool.getName() + " / " + newPool.getType());
    return newPool;
  }

  @Override
  public Collection<String> listPools() {
    return Collections.unmodifiableSet(resourcePools.keySet());
  }

  @Override
  public ResourceManagerPool getPool(String name) {
    return resourcePools.get(name);
  }

  @Override
  public void setPoolLimits(String name, Map<String, Object> poolLimits) throws Exception {
    ensureActive();
    ResourceManagerPool pool = resourcePools.get(name);
    if (pool == null) {
      throw new IllegalArgumentException("Pool '" + name + "' doesn't exist.");
    }
    pool.setPoolLimits(poolLimits);
    log.info("- modified pool limits " + pool.getName() + " / " + pool.getType() + ": " + poolLimits);

  }

  @Override
  public void removePool(String name) throws Exception {
    ensureActive();
    ResourceManagerPool pool = resourcePools.remove(name);
    if (pool == null) {
      log.warn("Pool '" + name + "' doesn't exist, ignoring remove request.");
      return;
    }
    IOUtils.closeQuietly(pool);
    log.info("- removed pool " + pool.getName() + " / " + pool.getType());
  }

  @Override
  public void registerComponent(String name, ManagedComponent managedComponent) {
    ensureActive();
    ResourceManagerPool pool = resourcePools.get(name);
    if (pool == null) {
      log.warn("Pool '" + name + "' doesn't exist, ignoring register of " + managedComponent.getManagedComponentId());
      return;
    }
    String poolType = pool.getType();
    resourcePools.forEach((poolName, otherPool) -> {
      if (otherPool == pool) {
        return;
      }
      if (otherPool.isRegistered(managedComponent.getManagedComponentId().toString()) &&
          otherPool.getType().equals(poolType)) {
        throw new IllegalArgumentException("Resource " + managedComponent.getManagedComponentId() +
            " is already managed in another pool (" +
            otherPool.getName() + ") of the same type " + poolType);
      }
    });
    pool.registerComponent(managedComponent);
  }

  @Override
  public boolean unregisterComponent(String poolName, String resourceId) {
    ensureActive();
    ResourceManagerPool pool = resourcePools.get(poolName);
    if (pool == null) {
      log.warn("Pool '" + poolName + "' doesn't exist, ignoring unregister of " + resourceId);
      return false;
    }
    return pool.unregisterComponent(resourceId);
  }

  @Override
  public void close() throws Exception {
    super.close();
    synchronized (this) {
      log.debug("Closing all pools.");
      for (ResourceManagerPool pool : resourcePools.values()) {
        IOUtils.closeQuietly(pool);
      }
      resourcePools.clear();
    }
    log.debug("Shutting down scheduled thread pool executor now");
    scheduledThreadPoolExecutor.shutdownNow();
    log.debug("Awaiting termination of scheduled thread pool executor");
    ExecutorUtil.awaitTermination(scheduledThreadPoolExecutor);
    log.debug("Closed.");
  }

}
