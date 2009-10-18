package org.apache.lucene.util.cache;

/**
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Simple cache implementation that uses a HashMap to store (key, value) pairs.
 * This cache is not synchronized, use {@link Cache#synchronizedCache(Cache)}
 * if needed.
 */
public class SimpleMapCache<K,V> extends Cache<K,V> {
  protected Map<K,V> map;
  
  public SimpleMapCache() {
    this(new HashMap<K,V>());
  }

  public SimpleMapCache(Map<K,V> map) {
    this.map = map;
  }
  
  @Override
  public V get(Object key) {
    return map.get(key);
  }

  @Override
  public void put(K key, V value) {
    map.put(key, value);
  }

  @Override
  public void close() {
    // NOOP
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }
  
  /**
   * Returns a Set containing all keys in this cache.
   */
  public Set<K> keySet() {
    return map.keySet();
  }
  
  @Override
  Cache<K,V> getSynchronizedCache() {
    return new SynchronizedSimpleMapCache<K,V>(this);
  }
  
  private static class SynchronizedSimpleMapCache<K,V> extends SimpleMapCache<K,V> {
    private Object mutex;
    private SimpleMapCache<K,V> cache;
    
    SynchronizedSimpleMapCache(SimpleMapCache<K,V> cache) {
        this.cache = cache;
        this.mutex = this;
    }
    
    @Override
    public void put(K key, V value) {
        synchronized(mutex) {cache.put(key, value);}
    }
    
    @Override
    public V get(Object key) {
        synchronized(mutex) {return cache.get(key);}
    }
    
    @Override
    public boolean containsKey(Object key) {
        synchronized(mutex) {return cache.containsKey(key);}
    }
    
    @Override
    public void close() {
        synchronized(mutex) {cache.close();}
    }
    
    @Override
    public Set<K> keySet() {
      synchronized(mutex) {return cache.keySet();}
    }
    
    @Override
    Cache<K,V> getSynchronizedCache() {
      return this;
    }
  }
}
