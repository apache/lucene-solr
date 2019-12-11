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

import org.apache.solr.core.SolrInfoBean;

import java.util.Map;
import java.util.function.Function;


/**
 * Primary API for dealing with Solr's internal caches.
 */
public interface SolrCache<K,V> extends SolrInfoBean {

  String HIT_RATIO_PARAM = "hitratio";
  String HITS_PARAM = "hits";
  String INSERTS_PARAM = "inserts";
  String EVICTIONS_PARAM = "evictions";
  String LOOKUPS_PARAM = "lookups";
  String SIZE_PARAM = "size";
  String MAX_SIZE_PARAM = "maxSize";
  String RAM_BYTES_USED_PARAM = "ramBytesUsed";
  String MAX_RAM_MB_PARAM = "maxRamMB";
  String MAX_IDLE_TIME_PARAM = "maxIdleTime";
  String INITIAL_SIZE_PARAM = "initialSize";
  String CLEANUP_THREAD_PARAM = "cleanupThread";
  String SHOW_ITEMS_PARAM = "showItems";

  /**
   * The initialization routine. Instance specific arguments are passed in
   * the <code>args</code> map.
   * <p>
   * The persistence object will exist across different lifetimes of similar caches.
   * For example, all filter caches will share the same persistence object, sometimes
   * at the same time (it must be thread-safe).  If null is passed, then the cache
   * implementation should create and return a new persistence object.  If not null,
   * the passed in object should be returned again.
   * <p>
   * Since it will exist across the lifetime of many caches, care should be taken to
   * not reference any particular cache instance and prevent it from being
   * garbage collected (no using inner classes unless they are static).
   * <p>
   * The persistence object is designed to be used as a way for statistics
   * to accumulate across all instances of the same type of cache, however the
   * object may be of any type desired by the cache implementation.
   * <p>
   * The {@link CacheRegenerator} is what the cache uses during auto-warming to
   * regenerate an item in the new cache from an entry in the old cache.
   *
   */
  public Object init(Map args, Object persistence, CacheRegenerator regenerator);
  // I don't think we need a factory for faster creation given that these
  // will be associated with slow-to-create SolrIndexSearchers.
  // change to NamedList when other plugins do?

  /**
   * Name the Cache can be referenced with by SolrRequestHandlers.
   *
   * This method must return the identifier that the Cache instance 
   * expects SolrRequestHandlers to use when requesting access to it 
   * from the SolrIndexSearcher.  It is <strong>strongly</strong> 
   * recommended that this method return the value of the "name" 
   * parameter from the init args.
   *
   * :TODO: verify this.
   */
  public String name();


  // Should SolrCache just extend the java.util.Map interface?
  // Following the conventions of the java.util.Map interface in any case.

  /** :TODO: copy from Map */
  public int size();

  /** :TODO: copy from Map */
  public V put(K key, V value);

  /** :TODO: copy from Map */
  public V get(K key);

  public V remove(K key);

  /**
   * Get an existing element or atomically compute it if missing.
   * @param key key
   * @param mappingFunction function to compute the element. If the function returns a null
   *                        result the cache mapping will not be created. NOTE: this function
   *                        must NOT attempt to modify any mappings in the cache.
   * @return existing or newly computed value, null if there was no existing value and
   * it was not possible to compute a new value (in which case the new mapping won't be created).
   */
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction);

  /** :TODO: copy from Map */
  public void clear();

  /** 
   * Enumeration of possible States for cache instances.
   * :TODO: only state that seems to ever be set is LIVE ?
  */
  public enum State { 
    /** :TODO */
    CREATED, 
    /** :TODO */
    STATICWARMING, 
    /** :TODO */
    AUTOWARMING, 
    /** :TODO */
    LIVE 
  }

  /**
   * Set different cache states.
   * The state a cache is in can have an effect on how statistics are kept.
   * The cache user (SolrIndexSearcher) will take care of switching
   * cache states.
   */
  public void setState(State state);

  /**
   * Returns the last State set on this instance
   *
   * @see #setState
   */
  public State getState();

  /**
   * Warm this cache associated with <code>searcher</code> using the <code>old</code>
   * cache object.  <code>this</code> and <code>old</code> will have the same concrete type.
   */
  void warm(SolrIndexSearcher searcher, SolrCache<K,V> old);
  // Q: an alternative to passing the searcher here would be to pass it in
  // init and have the cache implementation save it.


  /** Frees any non-memory resources */
  default void close() throws Exception {
    SolrInfoBean.super.close();
  }

  /** Returns maximum size limit (number of items) if set and supported, -1 otherwise. */
  int getMaxSize();

  /** Set maximum size limit (number of items), or -1 for unlimited. Note: this has effect
   * only on implementations that support it, it's a no-op otherwise
   */
  void setMaxSize(int maxSize);

  /** Returns maximum size limit (in MB) if set and supported, -1 otherwise. */
  int getMaxRamMB();

  /** Set maximum size limit (in MB), or -1 for unlimited. Note: this has effect
   * only on implementations that support it, it's a no-op otherwise.
   */
  void setMaxRamMB(int maxRamMB);
}
