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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.solr.common.SolrCloseable;

/**
 * Simple object cache with a type-safe accessor.
 */
public class ObjectCache extends MapBackedCache<String, Object> implements SolrCloseable {

  private volatile boolean isClosed;

  public ObjectCache() {
    super(new ConcurrentHashMap<>());
  }

  private void ensureNotClosed() {
    if (isClosed) {
      throw new RuntimeException("This ObjectCache is already closed.");
    }
  }

  @Override
  public Object put(String key, Object val) {
    ensureNotClosed();
    return super.put(key, val);
  }

  @Override
  public Object get(String key) {
    ensureNotClosed();
    return super.get(key);
  }

  @Override
  public Object remove(String key) {
    ensureNotClosed();
    return super.remove(key);
  }

  @Override
  public void clear() {
    ensureNotClosed();
    super.clear();
  }

  public <T> T get(String key, Class<T> clazz) {
    Object o = get(key);
    if (o == null) {
      return null;
    } else {
      return clazz.cast(o);
    }
  }

  public <T> T computeIfAbsent(String key, Class<T> clazz, Function<String, ? extends T> mappingFunction) {
    ensureNotClosed();
    Object o = super.computeIfAbsent(key, mappingFunction);
    return clazz.cast(o);
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
    map.clear();
  }
}