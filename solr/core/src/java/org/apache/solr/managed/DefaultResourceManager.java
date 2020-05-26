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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.request.beans.ResourcePoolConfig;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.managed.types.CacheManagerPool;
import org.apache.solr.search.SolrCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link ResourceManager}.
 * <p>Resource pools managed by this implementation are managed periodically, each according to
 * its schedule defined by {@link ResourceManagerPool#SCHEDULE_DELAY_SECONDS_PARAM} parameter during the pool creation.</p>
 */
public class DefaultResourceManager extends ResourceManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public static final String MAX_NUM_POOLS_PARAM = "maxNumPools";

  /**
   * Maximum number of pools.
   */
  public static final int DEFAULT_MAX_POOLS = 500;

  /** {@link org.apache.solr.search.SolrIndexSearcher}'s document cache pool. */
  public static final String DOCUMENT_CACHE_POOL = "searcherDocumentCache";
  /** {@link org.apache.solr.search.SolrIndexSearcher}'s filter cache pool. */
  public static final String FILTER_CACHE_POOL = "searcherFilterCache";
  /** {@link org.apache.solr.search.SolrIndexSearcher}'s field value (for uninverted fields) cache pool. */
  public static final String FIELD_VALUE_CACHE_POOL = "searcherFieldValueCache";
  /** {@link org.apache.solr.search.SolrIndexSearcher}'s query result cache pool. */
  public static final String QUERY_RESULT_CACHE_POOL = "searcherQueryResultCache";
  /** {@link org.apache.solr.search.SolrIndexSearcher}'s generic user cache pool. */
  public static final String USER_SEARCHER_CACHE_POOL = "searcherUserCache";

  private static final Map<String, ResourcePoolConfig> DEFAULT_NODE_POOLS;

  static {
    Map<String, Object> defaultLimits = new HashMap<>();
    // unlimited cache RAM
    defaultLimits.put(SolrCache.MAX_RAM_MB_PARAM, -1L);
    // unlimited cache size
    defaultLimits.put(SolrCache.MAX_SIZE_PARAM, -1L);
    Map<String, ResourcePoolConfig> defaultPools = new HashMap<>();
    for (String poolName : Arrays.asList(
        DOCUMENT_CACHE_POOL,
        FILTER_CACHE_POOL,
        FIELD_VALUE_CACHE_POOL,
        QUERY_RESULT_CACHE_POOL,
        USER_SEARCHER_CACHE_POOL)) {
      ResourcePoolConfig config = new ResourcePoolConfig();
      config.name = poolName;
      config.type = CacheManagerPool.TYPE;
      config.poolLimits.putAll(defaultLimits);
      defaultPools.put(poolName, config);
    }
    DEFAULT_NODE_POOLS = Collections.unmodifiableMap(defaultPools);
  }


  private int maxNumPools = DEFAULT_MAX_POOLS;
  private final Map<String, ResourceManagerPool> resourcePools = new ConcurrentHashMap<>();
  private final TimeSource timeSource;
  private final SolrResourceLoader loader;

  /**
   * Thread pool for scheduling the pool runs.
   */
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
  private ResourceManagerPoolFactory resourceManagerPoolFactory;


  public DefaultResourceManager(SolrResourceLoader loader, TimeSource timeSource) {
    Objects.nonNull(loader);
    this.loader = loader;
    // timeSource may be null in tests where we want to disable automatic scheduling
    this.timeSource = timeSource;
  }

  @Override
  public Map<String, ResourcePoolConfig> getDefaultPoolConfigs() {
    return DEFAULT_NODE_POOLS;
  }

  protected void doInit() throws Exception {
    if (scheduledThreadPoolExecutor == null) {
      scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(DEFAULT_NODE_POOLS.size(),
          new SolrNamedThreadFactory(getClass().getSimpleName()));
      scheduledThreadPoolExecutor.setMaximumPoolSize(maxNumPools);
      scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
      scheduledThreadPoolExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }
    if (resourceManagerPoolFactory == null) {
      resourceManagerPoolFactory = new DefaultResourceManagerPoolFactory(loader,
          pluginInfo != null ?
              (Map<String, Object>)pluginInfo.initArgs.toMap(new HashMap<>()).getOrDefault("plugins", Collections.emptyMap()) :
              Collections.emptyMap());
    }
    log.debug("Default resource manager initialized for loader {}.", loader);
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
  public ResourceManagerPool createPool(String name, String type, Map<String, Object> poolLimits, Map<String, Object> poolParams) throws Exception {
    ensureActive();
    if (resourcePools.containsKey(name)) {
      throw new IllegalArgumentException("Pool '" + name + "' already exists.");
    }
    if (resourcePools.size() >= maxNumPools) {
      throw new IllegalArgumentException("Maximum number of pools (" + maxNumPools + ") reached.");
    }
    ResourceManagerPool newPool = resourceManagerPoolFactory.create(name, type, this, poolLimits, poolParams);
    ResourceManagerPool oldPool = resourcePools.putIfAbsent(name, newPool);
    if (oldPool != null) {
      // someone else already created it
      IOUtils.closeQuietly(newPool);
      throw new IllegalArgumentException("Pool '" + name + "' already exists.");
    }
    newPool.initializeMetrics(metricsContext, name);
    if (timeSource != null) {
      if (newPool.scheduleDelaySeconds > 0) {
        newPool.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(() -> {
              log.debug("- running pool {} / {}", newPool.getName(), newPool.getType());
              newPool.manage();
            }, 0,
            timeSource.convertDelay(TimeUnit.SECONDS, newPool.scheduleDelaySeconds, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS);
      } else {
        log.info(" - disabling automatic scheduling for pool {} / {}", newPool.getName(), newPool.getType());
      }
    }
    log.debug("- created pool " + newPool.getName() + " / " + newPool.getType());
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
    log.debug("- modified pool limits {} / {}: {}", pool.getName(), pool.getType(), poolLimits);

  }

  @Override
  public void setPoolParams(String name, Map<String, Object> poolParams) throws Exception {
    ensureActive();
    ResourceManagerPool pool = resourcePools.get(name);
    if (pool == null) {
      throw new IllegalArgumentException("Pool '" + name + "' doesn't exist.");
    }
    // schedule delay may have changed, re-schedule if needed
    int oldDelay = pool.scheduleDelaySeconds;
    pool.setPoolParams(poolParams);
    int newDelay = pool.scheduleDelaySeconds;
    if (newDelay != oldDelay && timeSource != null) {
      // re-schedule
      if (pool.scheduledFuture != null) {
        pool.scheduledFuture.cancel(false);
      }
      if (newDelay > 0) {
        log.debug("- modified pool params {} / {}, re-scheduling due to a new schedule delay: {}",
            pool.getName(), pool.getType(), poolParams);
        pool.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(() -> {
              log.debug("- running pool {} / {}", pool.getName(), pool.getType());
              pool.manage();
            }, 0,
            timeSource.convertDelay(TimeUnit.SECONDS, newDelay, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS);
      } else {
        log.info(" - disabling automatic scheduling for pool {} / {}", pool.getName(), pool.getType());
      }
    }
  }

  @Override
  public void removePool(String name) throws Exception {
    ensureActive();
    ResourceManagerPool pool = resourcePools.remove(name);
    if (pool == null) {
      log.debug("Pool '" + name + "' doesn't exist, ignoring remove request.");
      return;
    }
    IOUtils.closeQuietly(pool);
    log.debug("- removed pool " + pool.getName() + " / " + pool.getType());
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
    updateLock.lock();
    try {
      log.debug("Closing all pools.");
      for (ResourceManagerPool pool : resourcePools.values()) {
        IOUtils.closeQuietly(pool);
      }
      resourcePools.clear();
      log.debug("Shutting down scheduled thread pool executor now");
      scheduledThreadPoolExecutor.shutdownNow();
      log.debug("Awaiting termination of scheduled thread pool executor");
      ExecutorUtil.awaitTermination(scheduledThreadPoolExecutor);
      log.debug("Closed.");
    } finally {
      updateLock.unlock();
    }
  }

}
