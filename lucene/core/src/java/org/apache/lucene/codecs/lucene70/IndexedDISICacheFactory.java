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
package org.apache.lucene.codecs.lucene70;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Creates and stores caches for {@link IndexedDISI} and {@link Lucene70DocValuesProducer}.
 * The caches are stored in maps, where the key is made up from offset and length of a slice
 * in an underlying segment. Each segment uses their own IndexedDISICacheFactory.
 *
 * See {@link IndexedDISICache} for details on the caching.
 */
public class IndexedDISICacheFactory implements Accountable {

  /**
   * If the slice with the DISI-data is less than this number of bytes, don't create a cache.
   * This is a very low number as the DISI-structure very efficiently represents EMPTY and ALL blocks.
   */
  private static int MIN_LENGTH_FOR_CACHING = 50; // Set this very low: Could be 9 EMPTY followed by a SPARSE

  // jump-table and rank for DISI blocks
  private final Map<Long, IndexedDISICache> disiPool = new HashMap<>();

  /**
   * Create a cached {@link IndexedDISI} instance.
   * @param data   persistent data containing the DISI-structure.
   * @param cost   cost as defined for IndexedDISI.
   * @param name   identifier for the DISI-structure for debug purposes.
   * @return a cached IndexedDISI or a plain IndexedDISI, if caching is not applicable.
   * @throws IOException if the DISI-structure could not be accessed.
   */
  IndexedDISI createCachedIndexedDISI(IndexInput data, long key, int cost, String name) throws IOException {
    IndexedDISICache cache = getCache(data, key, name);
    return new IndexedDISI(data, cost, cache, name);
  }

  /**
   * Create a cached {@link IndexedDISI} instance.
   * @param data   persistent data containing the DISI-structure.
   * @param offset same as the offset that will also be used for creating an {@link IndexedDISI}.
   * @param length same af the length that will also be used for creating an {@link IndexedDISI}.
   * @param cost   cost as defined for IndexedDISI.
   * @param name   identifier for the DISI-structure for debug purposes.
   * @return a cached IndexedDISI or a plain IndexedDISI, if caching is not applicable.
   * @throws IOException if the DISI-structure could not be accessed.
   */
  IndexedDISI createCachedIndexedDISI(IndexInput data, long offset, long length, long cost, String name)
      throws IOException {
    IndexedDISICache cache = getCache(data, offset, length, name);
    return new IndexedDISI(data, offset, length, cost, cache, name);
  }

  /**
   * Creates a cache (jump table) for {@link IndexedDISI}.
   * If the cache has previously been created, the old cache is returned.
   * @param data   the slice to create a cache for.
   * @param offset same as the offset that will also be used for creating an {@link IndexedDISI}.
   * @param length same af the length that will also be used for creating an {@link IndexedDISI}.
   * @param name human readable designation, typically a field name. Used for debug, log and inspection.
   * @return a cache for the given slice+offset+length or null if not suitable for caching.
   */
  public IndexedDISICache getCache(IndexInput data, long offset, long length, String name) throws IOException {
    if (length < MIN_LENGTH_FOR_CACHING) {
      return null;
    }

    long key = offset + length;
    IndexedDISICache cache = disiPool.get(key);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache for performance reason
      cache = new IndexedDISICache(data.slice("docs", offset, length), name);
      disiPool.put(key, cache);
    }
    return cache;
  }

  /**
   * Creates a cache (jump table) for {@link IndexedDISI}.
   * If the cache has previously been created, the old cache is returned.
   * @param slice the input slice.
   * @param key identifier for the cache, unique within the segment that originated the slice.
   *            Recommendation is offset+length for the slice, relative to the data mapping the segment.
   *            Warning: Do not use slice.getFilePointer and slice.length as they are not guaranteed
   *            to be unique within the segment (slice.getFilePointer is 0 when a sub-slice is created).
   * @param name human readable designation, typically a field name. Used for debug, log and inspection.
   * @return a cache for the given slice+offset+length or null if not suitable for caching.
   */
  public IndexedDISICache getCache(IndexInput slice, long key, String name) throws IOException {
    final long length = slice.length();
    if (length < MIN_LENGTH_FOR_CACHING) {
      return null;
    }

    IndexedDISICache cache = disiPool.get(key);
    if (cache == null) {
      // TODO: Avoid overlapping builds of the same cache
      // Both BLOCK & DENSE caches are created as they might be requested later for the field,
      // regardless of whether they are requested now
      cache = new IndexedDISICache(slice, name);
      disiPool.put(key, cache);
    }
    return cache;
  }

  // Statistics
  public long getDISIBlocksWithOffsetsCount() {
    return disiPool.values().stream().filter(IndexedDISICache::hasOffsets).count();
  }
  public long getDISIBlocksWithRankCount() {
    return disiPool.values().stream().filter(IndexedDISICache::hasRank).count();
  }

  @Override
  public long ramBytesUsed() {
    long mem = RamUsageEstimator.shallowSizeOf(this) +
        RamUsageEstimator.shallowSizeOf(disiPool);
    for (Map.Entry<Long, IndexedDISICache> cacheEntry: disiPool.entrySet()) {
      mem += RamUsageEstimator.shallowSizeOf(cacheEntry);
      mem += RamUsageEstimator.sizeOf(cacheEntry.getKey());
      mem += cacheEntry.getValue().ramBytesUsed();
    }
    return mem;
  }

  /**
   * Releases all caches.
   */
  void releaseAll() {
    disiPool.clear();
  }
}
