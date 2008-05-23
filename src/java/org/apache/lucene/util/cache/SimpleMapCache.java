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
public class SimpleMapCache extends Cache {
  Map map;
  
  public SimpleMapCache() {
    this(new HashMap());
  }

  public SimpleMapCache(Map map) {
    this.map = map;
  }
  
  public Object get(Object key) {
    return map.get(key);
  }

  public void put(Object key, Object value) {
    map.put(key, value);
  }

  public void close() {
    // NOOP
  }

  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }
  
  /**
   * Returns a Set containing all keys in this cache.
   */
  public Set keySet() {
    return map.keySet();
  }
  
  Cache getSynchronizedCache() {
    return new SynchronizedSimpleMapCache(this);
  }
  
  private static class SynchronizedSimpleMapCache extends SimpleMapCache {
    Object mutex;
    SimpleMapCache cache;
    
    SynchronizedSimpleMapCache(SimpleMapCache cache) {
        this.cache = cache;
        this.mutex = this;
    }
    
    public void put(Object key, Object value) {
        synchronized(mutex) {cache.put(key, value);}
    }
    
    public Object get(Object key) {
        synchronized(mutex) {return cache.get(key);}
    }
    
    public boolean containsKey(Object key) {
        synchronized(mutex) {return cache.containsKey(key);}
    }
    
    public void close() {
        synchronized(mutex) {cache.close();}
    }
    
    public Set keySet() {
      synchronized(mutex) {return cache.keySet();}
    }
    
    Cache getSynchronizedCache() {
      return this;
    }
  }
}
