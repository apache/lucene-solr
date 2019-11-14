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

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.function.Function;

import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrCacheHolder<K, V> implements SolrCache<K,V> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  private final CacheConfig factory;
  protected volatile SolrCache<K, V> delegate;

  public SolrCacheHolder(SolrCache<K, V> delegate, CacheConfig factory) {
    this.delegate = delegate;
    this.factory = factory;
  }

  public int size() {
    return delegate.size();
  }

  public V put(K key, V value) {
    return delegate.put(key, value);
  }

  @Override
  public V remove(K key) {
    return delegate.remove(key);
  }

  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return delegate.computeIfAbsent(key, mappingFunction);
  }

  public V get(K key) {
    return delegate.get(key);
  }

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
    if (old instanceof SolrCacheHolder) old = ((SolrCacheHolder) old).get();
    delegate.warm(searcher, old);

  }

  public SolrCache<K, V> get() {
    return delegate;
  }

  public void close() throws Exception {
    delegate.close();
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

  public void warm(SolrIndexSearcher searcher, SolrCacheHolder src) {
    delegate.warm(searcher, src.get());
  }

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    return null;
  }

  @Override
  public String name() {
    return delegate.name();
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
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return delegate.getSolrMetricsContext();
  }

}
