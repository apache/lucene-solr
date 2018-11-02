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
package org.apache.solr.handler.dataimport;

import java.util.Iterator;
import java.util.Map;

/**
 * <p>
 * A cache that allows a DIH entity's data to persist locally prior being joined
 * to other data and/or indexed.
 * </p>
 * 
 * @lucene.experimental
 */
public interface DIHCache extends Iterable<Map<String,Object>> {
  
  /**
   * <p>
   * Opens the cache using the specified properties. The {@link Context}
   * includes any parameters needed by the cache impl. This must be called
   * before any read/write operations are permitted.
   */
  void open(Context context);
  
  /**
   * <p>
   * Releases resources used by this cache, if possible. The cache is flushed
   * but not destroyed.
   * </p>
   */
  void close();
  
  /**
   * <p>
   * Persists any pending data to the cache
   * </p>
   */
  void flush();
  
  /**
   * <p>
   * Closes the cache, if open. Then removes all data, possibly removing the
   * cache entirely from persistent storage.
   * </p>
   */
  public void destroy();
  
  /**
   * <p>
   * Adds a document. If a document already exists with the same key, both
   * documents will exist in the cache, as the cache allows duplicate keys. To
   * update a key's documents, first call delete(Object key).
   * </p>
   */
  void add(Map<String, Object> rec);
  
  /**
   * <p>
   * Returns an iterator, allowing callers to iterate through the entire cache
   * in key, then insertion, order.
   * </p>
   */
  @Override
  Iterator<Map<String,Object>> iterator();
  
  /**
   * <p>
   * Returns an iterator, allowing callers to iterate through all documents that
   * match the given key in insertion order.
   * </p>
   */
  Iterator<Map<String,Object>> iterator(Object key);
  
  /**
   * <p>
   * Delete all documents associated with the given key
   * </p>
   */
  void delete(Object key);
  
  /**
   * <p>
   * Delete all data from the cache,leaving the empty cache intact.
   * </p>
   */
  void deleteAll();
  
}
