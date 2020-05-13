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

package org.apache.solr.common.util;


import java.util.Map;
import java.util.function.Function;

// A cache backed by a map
public class MapBackedCache<K, V> implements Cache<K, V> {

  protected final Map<K, V> map;

  public MapBackedCache(Map<K, V> map) {
    this.map = map;
  }

  public Map<K, V> asMap() {
    return map;
  }

  @Override
  public V put(K key, V val) {
    return map.put(key, val);
  }

  @Override
  public V get(K key) {
    return map.get(key);
  }

  @Override
  public V remove(K key) {
    return map.remove(key);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return map.computeIfAbsent(key, mappingFunction);
  }
}
