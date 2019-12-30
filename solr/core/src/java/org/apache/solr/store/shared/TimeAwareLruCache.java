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
package org.apache.solr.store.shared;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;

/**
 * Abstract class representing an in-memory time-aware LRU cache 
 * 
 * Time-based LRU cache should evict entries that haven't been accessed
 * recently and frequently as the size of the cache approaches the specified
 * limit or if an entry expires before its accessed again
 */
public abstract class TimeAwareLruCache<K, V> {

  /**
   * Retrieves an entry from the cache if it exists, otherwise invokes the
   * callable to compute the value and adds it to the cache
   */
  public abstract V computeIfAbsent(K key, Function<? super K, ? extends V> func) throws Exception;
  
  /**
   * Retrieves an entry from the cache if it exists, returns null otherwise 
   */
  public abstract V get(K key);
  
  /**
   * Adds or removes an entry from the cache 
   */
  public abstract void put(K key, V value);
  
  /**
   * Removes an entry from the cache. Returns true if the entry existed and was removed
   */
  public abstract boolean invalidate(K key);
  
  /**
   * Forces the cache to remove any expired entries, or entries to be removed as deemed
   * by the underlying eviction policies 
   */
  public abstract void cleanUp();
  
  /**
   * An object that performs some function when an entry is removed from the cache. This 
   * should be used in cache implementations that support removal listeners
   */
  @FunctionalInterface
  public interface CacheRemovalTrigger<K, V> {
    /**
     * @param key of the value being removed
     * @param value of the key being removed
     * @param desc string description of the removal cause
     */
    void call(K key, V value, String desc);
  }
  
  /**
   * Caffeine-based implementation of a {@link TimeAwareLruCache}. By default, eviction policy
   * work is performed asynchronously. Therefore entries are evicted after some read or write 
   * operations. Calling cleanup will perform any pending maintenance operations required by 
   * the cache
   * https://github.com/ben-manes/caffeine/wiki/Cleanup
   */
  public static class EvictingCache<K, V> extends TimeAwareLruCache<K, V> {

    private Cache<K, V> cache;
    
    public EvictingCache(int maxSize, long expirationTimeSeconds) {
      this(maxSize, expirationTimeSeconds, null);
    }
    
    @VisibleForTesting
    protected EvictingCache(int maxSize, long expirationTimeSeconds, CacheRemovalTrigger<K, V> removalTrigger) {
      Caffeine<Object,Object> builder = Caffeine.newBuilder();
      builder.maximumSize(maxSize).expireAfterAccess(expirationTimeSeconds, TimeUnit.SECONDS);
      if (removalTrigger != null) {
        builder.removalListener(new RemovalListener<K, V>() {
          @Override
          public void onRemoval(K key, V value, RemovalCause cause) {
            removalTrigger.call(key, value, cause.name());
          }
        });
      }
      cache = builder.build();
    }  
    
    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> func) throws Exception {
      return cache.get(key, func);
    }
    
    @Override
    public V get(K key) {
      return cache.getIfPresent(key);
    }
    
    @Override
    public void put(K key, V value) {
      cache.put(key, value);
    }
    
    @Override
    public void cleanUp() {
      cache.cleanUp();
    }

    @Override
    public boolean invalidate(K key) {
      if (cache.getIfPresent(key) != null) {
        cache.invalidate(key);
        return true;
      }
      return false;
    }
  }
}
