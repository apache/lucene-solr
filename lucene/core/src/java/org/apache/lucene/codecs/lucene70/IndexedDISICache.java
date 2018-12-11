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
 * A lookup table for block blockCache and index, and a rank structure for DENSE block lookups.
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
 *
 * The rank structure for DENSE blocks is an array of unsigned {@code short}s with an entry
 * for each sub-block of 512 bits out of the 65536 bits in the outer block.
 *
 * Each rank-entry states the number of set bits within the block up to the bit before the
 * bit positioned at the start of the sub-block.
 * Note that that the rank entry of the first sub-block is always 0 and that the last entry can
 * at most be 65536-512 = 65024 and thus will always fit into an unsigned short.
 *
 * See https://en.wikipedia.org/wiki/Succinct_data_structure for details on rank structures.
 * The alternative to using the rank structure is iteration and summing of set bits for all
 * entries in the DENSE sub-block up until the wanted bit, with a worst-case of 1024 entries.
 * The rank cache overhead for a single DENSE block is 128 shorts (128*16 = 2048 bits) or
 * 1/32th.
 *
 * The ranks for the DENSE blocks are stored in a structure shared for the whole array of
 * blocks, DENSE or not. To avoid overhead that structure is itself sparse. See
 * {@link LongCompressor} for details on DENSE structure sparseness.
 *
 *
 * The performance overhead for creating a cache instance is equivalent to accessing all
 * DocValues values for the given field, i.e. it scales lineary to field size. On modern
 * hardware it is in the ballpark of 1ms for 5M values on modern hardware. Caveat lector:
 * At the point of writing, performance points are only available for 2 real-world setups.
 */
public class IndexedDISICache implements Accountable {
  private static final int BLOCK = 65536;   // The number of docIDs that a single block represents
  static final int BLOCK_BITS = 16;
  private static final long BLOCK_INDEX_SHIFT = 33; // Number of bits to shift a lookup entry to get the index
  private static final long BLOCK_INDEX_MASK = ~0L << BLOCK_INDEX_SHIFT; // The index bits in a lookup entry
  private static final long BLOCK_LOOKUP_MASK = ~BLOCK_INDEX_MASK; // The offset bits in a lookup entry

  private static final int RANK_BLOCK = 512; // The number of docIDs/bits in each rank-sub-block within a DENSE block
  static final int RANK_BLOCK_LONGS = 512/Long.SIZE; // The number of longs making up a rank-block (8)
  private static final int RANK_BLOCK_BITS = 9;
  private static final int RANKS_PER_BLOCK = BLOCK/RANK_BLOCK;

  private PackedInts.Reader rank;   // One every 512 docs, sparsely represented as not all blocks are DENSE
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
    this.rank = null;
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

  /**
   * Given a target (docID), this method returns the first docID in the entry containing the target.
   * @param target the docID for which an index is wanted.
   * @return the docID where the rank is known. This will be lte target.
   */
  // TODO: This method requires a lot of knowledge of the intrinsics of the cache. Usage should be simplified
  int denseRankPosition(int target) {
       return target >> RANK_BLOCK_BITS << RANK_BLOCK_BITS;
  }

  public boolean hasOffsets() {
    return blockCache != null;
  }

  boolean hasRank() {
    return rank != null;
  }
  
  /**
   * Get the rank (index) for all set bits up to just before the given rankPosition in the block.
   * The caller is responsible for deriving the count of bits up to the docID target from the rankPosition.
   * The caller is also responsible for keeping track of set bits up to the current block.
   * Important: This only accepts rankPositions that aligns to {@link #RANK_BLOCK} boundaries.
   * Note 1: Use {@link #denseRankPosition(int)} to obtain a calid rankPosition for a wanted docID.
   * Note 2: The caller should seek to the rankPosition in the underlying slice to keep everything in sync.
   * @param rankPosition a docID target that aligns to {@link #RANK_BLOCK}.
   * @return the rank (index / set bits count) up to just before the given rankPosition.
   *         If rank is disabled, -1 is returned.
   */
  // TODO: This method requires a lot of knowledge of the intrinsics of the cache. Usage should be simplified
  int getRankInBlock(int rankPosition) {
    if (rank == null) {
      return -1;
    }
    assert rankPosition == denseRankPosition(rankPosition);
    int rankIndex = rankPosition >> RANK_BLOCK_BITS;
    return rankIndex >= rank.size() ? -1 : (int) rank.get(rankIndex);
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
        "name=%s, blocks=%d (ALL=%d, DENSE=%d, SPARSE=%d, EMPTY=%d), time=%dms, block=%d bytes, rank=%d bytes",
        name,
        largestBlock+1, statBlockALL.get(), statBlockDENSE.get(), statBlockSPARSE.get(),
        (largestBlock+1-statBlockALL.get()-statBlockDENSE.get()-statBlockSPARSE.get()),
        (System.nanoTime()-startTime)/1000000,
        blockCache == null ? 0 : blockCache.length*Long.BYTES,
        rank == null ? 0 : rank.ramBytesUsed());
  }

  private int fillCache(
      IndexInput slice, AtomicInteger statBlockALL, AtomicInteger statBlockDENSE, AtomicInteger statBlockSPARSE)
      throws IOException {
    char[] buildRank = new char[256];
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
      int setBits = 0;
      int rankOrigo = blockIndex << 16 >> 9; // Double shift for clarity: The compiler will simplify it
      for (int rankDelta = 0 ; rankDelta < RANKS_PER_BLOCK ; rankDelta++) { // 128 rank-entries in a block
        rankIndex = rankOrigo + rankDelta;
        buildRank = ArrayUtil.grow(buildRank, rankIndex+1);
        buildRank[rankIndex] = (char)setBits;
        for (int i = 0 ; i < 512/64 ; i++) { // 8 longs for each rank-entry
          setBits += Long.bitCount(slice.readLong());
        }
      }
      assert slice.getFilePointer() == nextBlockOffset;
    }
    // Compress the buildRank as it is potentially very sparse
    if (rankIndex < 0) {
      rank = null;
    } else {
      PackedInts.Mutable ranks = PackedInts.getMutable(rankIndex, 16, PackedInts.DEFAULT); // Char = 16 bit
      for (int i = 0 ; i < rankIndex ; i++) {
        ranks.set(i, buildRank[i]);
      }
      rank = LongCompressor.compress(ranks);
    }

    return largestBlock;
  }

  private void freezeCaches(int largestBlock) {
    if (largestBlock == -1) { // No set bit: Disable the caches
      blockCache = null;
      rank = null;
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
        (rank == null ? 0 : rank.ramBytesUsed()) +
        RamUsageEstimator.NUM_BYTES_OBJECT_REF*3 +
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + creationStats.length()*2;
  }
}