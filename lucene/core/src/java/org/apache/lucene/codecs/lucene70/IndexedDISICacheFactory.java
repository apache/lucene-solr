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
  // jump-table for numerics with variable bits per value (dates, longs...)
  private final Map<String, VaryingBPVJumpTable> vBPVPool = new HashMap<>();

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
   * Creates a cache (jump table) for variable bits per value numerics and returns it.
   * If the cache has previously been created, the old cache is returned.
   * @param name the name for the cache, typically the field name. Used as key for later retrieval.
   * @param slice the long values with varying bits per value.
   * @param valuesLength the length in bytes of the slice.
   * @return a jump table for the longs in the given slice or null if the structure is not suitable for caching.
   */
  VaryingBPVJumpTable getVBPVJumpTable(String name, RandomAccessInput slice, long valuesLength) throws IOException {
    VaryingBPVJumpTable jumpTable = vBPVPool.get(name);
    if (jumpTable == null) {
      // TODO: Avoid overlapping builds of the same jump table for performance reasons
      jumpTable = new VaryingBPVJumpTable(slice, name, valuesLength);
      vBPVPool.put(name, jumpTable);
    }
    return jumpTable;
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
  public long getVaryingBPVCount() {
    return vBPVPool.size();
  }

  @Override
  public long ramBytesUsed() {
    long mem = RamUsageEstimator.shallowSizeOf(this) +
        RamUsageEstimator.shallowSizeOf(disiPool) +
        RamUsageEstimator.shallowSizeOf(vBPVPool);
    for (Map.Entry<Long, IndexedDISICache> cacheEntry: disiPool.entrySet()) {
      mem += RamUsageEstimator.shallowSizeOf(cacheEntry);
      mem += RamUsageEstimator.sizeOf(cacheEntry.getKey());
      mem += cacheEntry.getValue().ramBytesUsed();
    }
    for (Map.Entry<String, VaryingBPVJumpTable> cacheEntry: vBPVPool.entrySet()) {
      String key = cacheEntry.getKey();
      mem += RamUsageEstimator.shallowSizeOf(cacheEntry);
      mem += RamUsageEstimator.shallowSizeOf(key)+key.length()*2;
      mem += cacheEntry.getValue().ramBytesUsed();
    }
    return mem;
  }

  /**
   * Releases all caches.
   */
  void releaseAll() {
    disiPool.clear();
    vBPVPool.clear();
  }

  /**
   * Jump table used by Lucene70DocValuesProducer.VaryingBPVReader to avoid iterating all blocks from
   * current to wanted index for numerics with variable bits per value. The jump table holds offsets for all blocks.
   */
  public static class VaryingBPVJumpTable implements Accountable {
    // TODO: It is way overkill to use longs here for practically all indexes. Maybe a PackedInts representation?
    long[] offsets = new long[10];
    final String creationStats;

    VaryingBPVJumpTable(RandomAccessInput slice, String name, long valuesLength) throws IOException {
      final long startTime = System.nanoTime();

      int block = -1;
      long offset;
      long blockEndOffset = 0;

      int bitsPerValue;
      do {
        offset = blockEndOffset;

        offsets = ArrayUtil.grow(offsets, block+2); // No-op if large enough
        offsets[block+1] = offset;

        bitsPerValue = slice.readByte(offset++);
        offset += Long.BYTES; // Skip over delta as we do not resolve the values themselves at this point
        if (bitsPerValue == 0) {
          blockEndOffset = offset;
        } else {
          final int length = slice.readInt(offset);
          offset += Integer.BYTES;
          blockEndOffset = offset + length;
        }
        block++;
      } while (blockEndOffset < valuesLength-Byte.BYTES);
      offsets = ArrayUtil.copyOfSubArray(offsets, 0, block+1);
      creationStats = String.format(Locale.ENGLISH,
          "name=%s, blocks=%d, time=%dms",
          name, offsets.length, (System.nanoTime()-startTime)/1000000);
    }

    /**
     * @param block the logical block in the vBPV structure ( valueindex/16384 ).
     * @return the index slice offset for the vBPV block (1 block = 16384 values) or -1 if not available.
     */
    long getBlockOffset(long block) {
      // Technically a limitation in caching vs. VaryingBPVReader to limit to 2b blocks of 16K values
      return offsets[(int) block];
    }

    @Override
    public long ramBytesUsed() {
      return RamUsageEstimator.shallowSizeOf(this) +
          (offsets == null ? 0 : RamUsageEstimator.sizeOf(offsets)) + // offsets
          RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + creationStats.length()*2;  // creationStats
    }
  }
}
