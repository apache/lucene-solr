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
package org.apache.solr.util;

import com.google.common.collect.ForwardingMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Wraps another map, keeping track of each key that was seen via {@link #get(Object)} or {@link #remove(Object)}.
 */
@SuppressWarnings("unchecked")
public class MapListener<K, V> extends ForwardingMap<K, V> {
  private final Map<K, V> target;
  private final Set<K> seenKeys;

  public MapListener(Map<K, V> target) {
    this.target = target;
    seenKeys = new HashSet<>(target.size());
  }

  public Set<K> getSeenKeys() {
    return seenKeys;
  }

  @Override
  public V get(Object key) {
    seenKeys.add((K) key);
    return super.get(key);
  }

  @Override
  public V remove(Object key) {
    seenKeys.add((K) key);
    return super.remove(key);
  }

  @Override
  protected Map<K, V> delegate() {
    return target;
  }
}
