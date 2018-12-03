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
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.codecs.lucene70.IndexedDISI.MAX_ARRAY_LENGTH;

/**
 * Caching of IndexedDISI with two strategies:
 *
 * A lookup table for block blockCache and index.
 *
 * The lookup table is an array of {@code long}s with an entry for each block. It allows for
 * direct jumping to the block, as opposed to iteration from the current position and forward
 * one block at a time.
 *
 * Each long entry consists of 2 logical parts:
 *
 * The first 31 bits holds the index (number of set bits in the blocks) up to just before the
 * wanted block. The next 33 bits holds the offset into the underlying slice.
 * As there is a maximum of 2^16 blocks, it follows that the maximum size of any block must
 * not exceed 2^17 bits to avoid overflow. This is currently the case, with the largest
 * block being DENSE and using 2^16 + 32 bits, and is likely to continue to hold as using
 * more than double the amount of bits is unlikely to be an efficient representation.
 * The cache overhead is numDocs/1024 bytes.
 *
 * Note: There are 4 types of blocks: ALL, DENSE, SPARSE and non-existing (0 set bits).
 * In the case of non-existing blocks, the entry in the lookup table has index equal to the
 * previous entry and offset equal to the next non-empty block.
 *
 * The performance overhead for creating a cache instance is equivalent to visiting every 65536th
 * doc value for the given field, i.e. it scales lineary to field size.
 */
public class IndexedDISICache implements Accountable {
  private static final int BLOCK = 65536;   // The number of docIDs that a single block represents
  static final int BLOCK_BITS = 16;
  private static final long BLOCK_INDEX_SHIFT = 33; // Number of bits to shift a lookup entry to get the index
  private static final long BLOCK_INDEX_MASK = ~0L << BLOCK_INDEX_SHIFT; // The index bits in a lookup entry
  private static final long BLOCK_LOOKUP_MASK = ~BLOCK_INDEX_MASK; // The offset bits in a lookup entry

  private long[] blockCache = null; // One every 65536 docs, contains index & slice position
  private String creationStats = "";
  private final String name; // Identifier for debug, log & inspection

  // Flags for not-yet-defined-values used during building
  private static final long BLOCK_EMPTY_INDEX = ~0L << BLOCK_INDEX_SHIFT;
  private static final long BLOCK_EMPTY_LOOKUP = BLOCK_LOOKUP_MASK;
  private static final long BLOCK_EMPTY = BLOCK_EMPTY_INDEX | BLOCK_EMPTY_LOOKUP;

  /**
   * Builds the stated caches for the given IndexInput.
   *
   * @param in positioned at the start of the logical underlying bitmap.
   */
  IndexedDISICache(IndexInput in, String name) throws IOException {
    blockCache = new long[16];    // Will be extended when needed
    Arrays.fill(blockCache, BLOCK_EMPTY);
    this.name = name;
    updateCaches(in);
  }

  private IndexedDISICache() {
    this.blockCache = null;
    this.name = "";
  }

  // Used to represent no caching.
  public static final IndexedDISICache EMPTY = new IndexedDISICache();

  /**
   * If available, returns a position within the underlying {@link IndexInput} for the start of the block
   * containing the wanted bit (the target) or the next non-EMPTY block, if the block representing the bit is empty.
   * @param targetBlock the index for the block to resolve (docID / 65536).
   * @return the offset for the block for target or -1 if it cannot be resolved.
   */
  long getFilePointerForBlock(int targetBlock) {
    long offset = blockCache == null || blockCache.length <= targetBlock ?
        -1 : blockCache[targetBlock] & BLOCK_LOOKUP_MASK;
    return offset == BLOCK_EMPTY_LOOKUP ? -1 : offset;
  }

  /**
   * If available, returns the index; number of set bits before the wanted block.
   * @param targetBlock the block to resolve (docID / 65536).
   * @return the index for the block or -1 if it cannot be resolved.
   */
  int getIndexForBlock(int targetBlock) {
    if (blockCache == null || blockCache.length <= targetBlock) {
      return -1;
    }
    return (blockCache[targetBlock] & BLOCK_INDEX_MASK) == BLOCK_EMPTY_INDEX ?
        -1 : (int)(blockCache[targetBlock] >>> BLOCK_INDEX_SHIFT);
  }

  public boolean hasOffsets() {
    return blockCache != null;
  }

  private void updateCaches(IndexInput slice) throws IOException {
    final long startOffset = slice.getFilePointer();

    final long startTime = System.nanoTime();
    AtomicInteger statBlockALL = new AtomicInteger(0);
    AtomicInteger statBlockDENSE = new AtomicInteger(0);
    AtomicInteger statBlockSPARSE = new AtomicInteger(0);

    // Fill phase
    int largestBlock = fillCache(slice, statBlockALL, statBlockDENSE, statBlockSPARSE);
    freezeCaches(largestBlock);

    slice.seek(startOffset); // Leave it as we found it
    creationStats = String.format(Locale.ENGLISH,
        "name=%s, blocks=%d (ALL=%d, DENSE=%d, SPARSE=%d, EMPTY=%d), time=%dms, block=%d bytes",
        name,
        largestBlock+1, statBlockALL.get(), statBlockDENSE.get(), statBlockSPARSE.get(),
        (largestBlock+1-statBlockALL.get()-statBlockDENSE.get()-statBlockSPARSE.get()),
        (System.nanoTime()-startTime)/1000000,
        blockCache == null ? 0 : blockCache.length*Long.BYTES);
  }

  private int fillCache(
      IndexInput slice, AtomicInteger statBlockALL, AtomicInteger statBlockDENSE, AtomicInteger statBlockSPARSE)
      throws IOException {
    int largestBlock = -1;
    long index = 0;
    int rankIndex = -1;
    while (slice.getFilePointer() < slice.length()) {
      final long startFilePointer = slice.getFilePointer();

      final int blockIndex = Short.toUnsignedInt(slice.readShort());
      final int numValues = 1 + Short.toUnsignedInt(slice.readShort());

      assert blockIndex > largestBlock;
      if (blockIndex == DocIdSetIterator.NO_MORE_DOCS >>> 16) { // End reached
        assert Short.toUnsignedInt(slice.readShort()) == (DocIdSetIterator.NO_MORE_DOCS & 0xFFFF);
        break;
      }
      largestBlock = blockIndex;

      blockCache = ArrayUtil.grow(blockCache, blockIndex+1); // No-op if large enough
      blockCache[blockIndex] = (index << BLOCK_INDEX_SHIFT) | startFilePointer;
      index += numValues;

      if (numValues <= MAX_ARRAY_LENGTH) { // SPARSE
        statBlockSPARSE.incrementAndGet();
        slice.seek(slice.getFilePointer() + (numValues << 1));
        continue;
      }
      if (numValues == 65536) { // ALL
        statBlockALL.incrementAndGet();
        // Already at next block offset
        continue;
      }

      // The block is DENSE
      statBlockDENSE.incrementAndGet();
      long nextBlockOffset = slice.getFilePointer() + (1 << 13);
      slice.seek(nextBlockOffset);
    }

    return largestBlock;
  }

  private void freezeCaches(int largestBlock) {
    if (largestBlock == -1) { // No set bit: Disable the caches
      blockCache = null;
      return;
    }

    // Reduce size to minimum
    if (blockCache.length-1 > largestBlock) {
      long[] newBC = new long[Math.max(largestBlock - 1, 1)];
      System.arraycopy(blockCache, 0, newBC, 0, newBC.length);
      blockCache = newBC;
    }

    // Set non-defined blockCache entries (caused by blocks with 0 set bits) to the subsequently defined one
    long latest = BLOCK_EMPTY;
    for (int i = blockCache.length-1; i >= 0 ; i--) {
      long current = blockCache[i];
      if (current == BLOCK_EMPTY) {
        blockCache[i] = latest;
      } else {
        latest = current;
      }
    }
  }

  /**
   * @return Human readable details from the creation of the cache instance.
   */
  public String getCreationStats() {
    return creationStats;
  }

  /**
   * @return Human-readable name for the cache instance.
   */
  public String getName() {
    return name;
  }

  @Override
  public long ramBytesUsed() {
    return (blockCache == null ? 0 : RamUsageEstimator.sizeOf(blockCache)) +
        RamUsageEstimator.NUM_BYTES_OBJECT_REF*3 +
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + creationStats.length()*2;
  }
}