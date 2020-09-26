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

package org.apache.solr.search;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.pkg.PackagePluginHolder;

public class DelegatingCache<K, V> implements SolrCache<K, V> {

  private static final SolrConfig.SolrPluginInfo CACHE_PLUGIN_INFO = SolrConfig.classVsSolrPluginInfo.get(SolrCache.class.getName());
  private volatile SolrCache<K, V> delegate;
  final CacheConfig cacheConfig;

  private SolrMetricsContext solrMetricsContext;
  private String scope;

  public DelegatingCache(CacheConfig cacheConfig, PluginInfo.ClassName cName, SolrCore.Provider coreProvider) {
    this.cacheConfig = cacheConfig;
    PackagePluginHolder<SolrCache<K, V>> cacheHolder = coreProvider
        .withCore(core -> new PackagePluginHolder<>(new PluginInfo(CACHE_PLUGIN_INFO.tag, cacheConfig.toMap(new HashMap<>())), core, CACHE_PLUGIN_INFO) {

          @Override
          @SuppressWarnings("unchecked")
          protected Object initNewInstance(PackageLoader.Package.Version newest, SolrCore core) {
            SolrCache<K,V> newCache = cacheConfig.getSolrCache(() -> newest.getLoader().findClass(cName.className, SolrCache.class));
            if (solrMetricsContext != null && scope != null) {
              newCache.initializeMetrics(solrMetricsContext, scope);
            }
            SolrCache<K, V> oldCache = delegate;
            newCache.setState(oldCache.getState());
            delegate = newCache;
            oldCache.clear();
            return newCache;
          }
        });
    if(cacheHolder == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "No core available");
    }

  }


  @Override
  @SuppressWarnings("rawtypes")
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    return null;
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public V put(K key, V value) {
    return delegate.put(key, value);
  }

  @Override
  public V get(K key) {
    return delegate.get(key);
  }

  @Override
  public V remove(K key) {
    return delegate.remove(key);
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return delegate.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public void clear() {
    delegate.clear();

  }

  @Override
  public void setState(State state) {
    delegate.setState(state);
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K, V> old) {
    if (old instanceof DelegatingCache) {
      delegate.warm(searcher, ((DelegatingCache<K, V>) old).delegate);
    } else {
      delegate.warm(searcher, old);
    }

  }

  @Override
  public int getMaxSize() {
    return delegate.getMaxSize();
  }

  @Override
  public void setMaxSize(int maxSize) {
    delegate.setMaxSize(maxSize);
  }

  @Override
  public int getMaxRamMB() {
    return delegate.getMaxRamMB();
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    delegate.setMaxRamMB(maxRamMB);
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public Category getCategory() {
    return delegate.getCategory();
  }



  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    delegate.initializeMetrics(parentContext, scope);
    this.solrMetricsContext = parentContext;
    this.scope = scope;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return delegate.getSolrMetricsContext();
  }
}
