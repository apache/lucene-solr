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

package org.apache.solr.core;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache of the most frequently accessed transient cores. Keeps track of all the registered
 * transient cores descriptors, including the cores in the cache as well as all the others.
 */
public class TransientSolrCoreCacheDefault extends TransientSolrCoreCache {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final CoreContainer coreContainer;

  /**
   * "Lazily loaded" cores cache with limited size. When the max size is reached, the least
   * accessed core is evicted to make room for a new core.
   */
  protected final Cache<String, SolrCore> transientCores;

  /**
   * Unlimited map of all the descriptors for all the registered transient cores, including the
   * cores in the {@link #transientCores} as well as all the others.
   */
  protected final Map<String, CoreDescriptor> transientDescriptors;

  /**
   * @param container The enclosing CoreContainer.
   */
  public TransientSolrCoreCacheDefault(final CoreContainer container) {
    this.coreContainer = container;
    int cacheMaxSize = getConfiguredCacheMaxSize(coreContainer);

    // Now don't allow ridiculous allocations here, if the size is > 1,000, we'll just deal with
    // adding cores as they're opened. This blows up with the marker value of -1.
    int initialCapacity = Math.min(cacheMaxSize, 1024);
    log.info("Allocating transient core cache for max {} cores with initial capacity of {}", cacheMaxSize, initialCapacity);
    Caffeine<String, SolrCore> transientCoresCacheBuilder =
        Caffeine.newBuilder()
            .initialCapacity(initialCapacity)
            // Use the current thread to queue evicted cores for closing. This ensures the
            // cache max size is respected (with a different thread the max size would be
            // respected asynchronously only eventually).
            .executor(Runnable::run)
            .removalListener(
                (coreName, core, cause) -> {
                  if (core != null && cause.wasEvicted()) {
                    if (log.isInfoEnabled()) {
                      log.info("Closing transient core [{}] evicted from the cache", core.getName());
                    }
                    coreContainer.queueCoreToClose(core);
                  }
                });
    if (cacheMaxSize != Integer.MAX_VALUE) {
      transientCoresCacheBuilder.maximumSize(cacheMaxSize);
    }
    transientCores = transientCoresCacheBuilder.build();

    transientDescriptors = new LinkedHashMap<>(initialCapacity);
  }

  private int getConfiguredCacheMaxSize(CoreContainer container) {
    int configuredCacheMaxSize = NodeConfig.NodeConfigBuilder.DEFAULT_TRANSIENT_CACHE_SIZE;
    NodeConfig cfg = container.getNodeConfig();
    if (cfg.getTransientCachePluginInfo() == null) {
      // Still handle just having transientCacheSize defined in the body of solr.xml
      // not in a transient handler clause.
      configuredCacheMaxSize = cfg.getTransientCacheSize();
    } else {
      NamedList<?> args = cfg.getTransientCachePluginInfo().initArgs;
      Object obj = args.get("transientCacheSize");
      if (obj != null) {
        configuredCacheMaxSize = (int) obj;
      }
    }
    if (configuredCacheMaxSize < 0) { // Trap old flag
      configuredCacheMaxSize = Integer.MAX_VALUE;
    }
    return configuredCacheMaxSize;
  }
  
  @Override
  public Collection<SolrCore> prepareForShutdown() {
    // Return a copy of the values.
    List<SolrCore> ret = new ArrayList<>(transientCores.asMap().values());
    transientCores.invalidateAll();
    transientCores.cleanUp();
    return ret;
  }

  @Override
  public CoreContainer getContainer() { return coreContainer; }

  @Override
  public SolrCore addCore(String name, SolrCore core) {
    return transientCores.asMap().put(name, core);
  }

  @Override
  public Set<String> getAllCoreNames() {
    return Collections.unmodifiableSet(transientDescriptors.keySet());
  }
  
  @Override
  public Set<String> getLoadedCoreNames() {
    return Collections.unmodifiableSet(transientCores.asMap().keySet());
  }

  @Override
  public SolrCore removeCore(String name) {
    return transientCores.asMap().remove(name);
  }

  @Override
  public SolrCore getCore(String name) {
    return name == null ? null : transientCores.getIfPresent(name);
  }

  @Override
  public boolean containsCore(String name) {
    return name != null && transientCores.asMap().containsKey(name);
  }

  @Override
  public void addTransientDescriptor(String rawName, CoreDescriptor cd) {
    transientDescriptors.put(rawName, cd);
  }

  @Override
  public CoreDescriptor getTransientDescriptor(String name) {
    return transientDescriptors.get(name);
  }

  @Override
  public Collection<CoreDescriptor> getTransientDescriptors() {
    return Collections.unmodifiableCollection(transientDescriptors.values());
  }

  @Override
  public CoreDescriptor removeTransientDescriptor(String name) {
    return transientDescriptors.remove(name);
  }

  @Override
  public int getStatus(String coreName) { return 0; } //no_op for default handler.

  @Override
  public void setStatus(String coreName, int status) {} //no_op for default handler.
}
