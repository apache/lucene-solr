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

import java.io.DataInput;
import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;

/**
 * Disk-based implementation of a {@link DocIdSetIterator} which can return
 * the index of the current document, i.e. the ordinal of the current document
 * among the list of documents that this iterator can return. This is useful
 * to implement sparse doc values by only having to encode values for documents
 * that actually have a value.
 * <p>Implementation-wise, this {@link DocIdSetIterator} is inspired of
 * {@link RoaringDocIdSet roaring bitmaps} and encodes ranges of {@code 65536}
 * documents independently and picks between 3 encodings depending on the
 * density of the range:<ul>
 *   <li>{@code ALL} if the range contains 65536 documents exactly,
 *   <li>{@code DENSE} if the range contains 4096 documents or more; in that
 *       case documents are stored in a bit set,
 *   <li>{@code SPARSE} otherwise, and the lower 16 bits of the doc IDs are
 *       stored in a {@link DataInput#readShort() short}.
 * </ul>
 * <p>Only ranges that contain at least one value are encoded.
 * <p>This implementation uses 6 bytes per document in the worst-case, which happens
 * in the case that all ranges contain exactly one document.
 * @lucene.internal
 */
final class IndexedDISI extends DocIdSetIterator {

  static final int MAX_ARRAY_LENGTH = (1 << 12) - 1;
  static final String NO_NAME = "n/a";

  public final String name;

  private static void flush(int block, FixedBitSet buffer, int cardinality, IndexOutput out) throws IOException {
    assert block >= 0 && block < 65536;
    out.writeShort((short) block);
    assert cardinality > 0 && cardinality <= 65536;
    out.writeShort((short) (cardinality - 1));
    if (cardinality > MAX_ARRAY_LENGTH) {
      if (cardinality != 65536) { // all docs are set
        for (long word : buffer.getBits()) {
          out.writeLong(word);
        }
      }
    } else {
      BitSetIterator it = new BitSetIterator(buffer, cardinality);
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        out.writeShort((short) doc);
      }
    }
  }

  static void writeBitSet(DocIdSetIterator it, IndexOutput out) throws IOException {
    int i = 0;
    final FixedBitSet buffer = new FixedBitSet(1<<16);
    int prevBlock = -1;
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      final int block = doc >>> 16;
      if (prevBlock != -1 && block != prevBlock) {
        flush(prevBlock, buffer, i, out);
        buffer.clear(0, buffer.length());
        prevBlock = block;
        i = 0;
      }
      buffer.set(doc & 0xFFFF);
      i++;
      prevBlock = block;
    }
    if (i > 0) {
      flush(prevBlock, buffer, i, out);
      buffer.clear(0, buffer.length());
    }
    // NO_MORE_DOCS is stored explicitly
    buffer.set(DocIdSetIterator.NO_MORE_DOCS & 0xFFFF);
    flush(DocIdSetIterator.NO_MORE_DOCS >>> 16, buffer, 1, out);
  }

  /** The slice that stores the {@link DocIdSetIterator}. */
  private final IndexInput slice;
  private final long cost;
  private final IndexedDISICache cache;

  IndexedDISI(IndexInput in, long offset, long length, long cost) throws IOException {
    this(in, offset, length, cost, NO_NAME);
  }

  IndexedDISI(IndexInput in, long offset, long length, long cost, String name) throws IOException {
    this(in, offset, length, cost, null, name);
  }

  IndexedDISI(IndexInput in, long offset, long length, long cost, IndexedDISICache cache) throws IOException {
    this(in, offset, length, cost, cache, NO_NAME);
  }

  IndexedDISI(IndexInput in, long offset, long length, long cost, IndexedDISICache cache, String name) throws IOException {
    this(in.slice("docs", offset, length), cost, cache, name);
  }

  IndexedDISI(IndexInput slice, long cost) throws IOException {
    this(slice, cost, NO_NAME);
  }
  // This constructor allows to pass the slice directly in case it helps reuse
  // see eg. Lucene70 norms producer's merge instance
  IndexedDISI(IndexInput slice, long cost, String name) throws IOException {
    this(slice, cost, null, name);
//    IndexedDISICacheFactory.debug(
//        "Non-cached direct slice IndexedDISI with length " + slice.length() + ": " + slice.toString());
  }

  IndexedDISI(IndexInput slice, long cost, IndexedDISICache cache) throws IOException {
    this(slice, cost, cache, NO_NAME);
  }
  // This constructor allows to pass the slice directly in case it helps reuse
  // see eg. Lucene70 norms producer's merge instance
  IndexedDISI(IndexInput slice, long cost, IndexedDISICache cache, String name) {
    this.name = name;
    this.slice = slice;
    this.cost = cost;
    this.cache = cache == null ? IndexedDISICache.EMPTY : cache;
  }

  private int block = -1;
  private long blockStart; // Used with the DENSE cache
  private long blockEnd;
  private int nextBlockIndex = -1;
  Method method;

  private int doc = -1;
  private int index = -1;

  // SPARSE variables
  boolean exists;

  // DENSE variables
  private long word;
  private int wordIndex = -1;
  // number of one bits encountered so far, including those of `word`
  private int numberOfOnes;
  // Used with rank for jumps inside of DENSE
  private int denseOrigoIndex;

  // ALL variables
  private int gap;

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    final int targetBlock = target & 0xFFFF0000;
    // Note: The cache makes it easy to add support for random access. This has not been done as the API forbids it
    if (block < targetBlock) {
      advanceBlock(targetBlock);
    }
    if (block == targetBlock) {
      if (method.advanceWithinBlock(this, target)) {
        return doc;
      }
      readBlockHeader();
    }
    boolean found = method.advanceWithinBlock(this, block);
    assert found;
    return doc;
  }

  public boolean advanceExact(int target) throws IOException {
    final int targetBlock = target & 0xFFFF0000;
    if (block < targetBlock) {
      advanceBlock(targetBlock);
    }
    boolean found = block == targetBlock && method.advanceExactWithinBlock(this, target);
    this.doc = target;
    return found;
  }

  private void advanceBlock(int targetBlock) throws IOException {
    if (targetBlock >= block+2) { // 1 block skip is (slightly) faster to do without block jump table
      long offset = cache.getFilePointerForBlock(targetBlock >> IndexedDISICache.BLOCK_BITS);
      if (offset != -1 && offset > slice.getFilePointer()) {
        int origo = cache.getIndexForBlock(targetBlock >> IndexedDISICache.BLOCK_BITS);
        if (origo != -1) {
          this.nextBlockIndex = origo - 1; // -1 to compensate for the always-added 1 in readBlockHeader
          slice.seek(offset);
          readBlockHeader();
          return;
        }
      }
    }

    // Fallback to non-cached
    do {
      slice.seek(blockEnd);
      readBlockHeader();
    } while (block < targetBlock);
  }

  private void readBlockHeader() throws IOException {
    blockStart = slice.getFilePointer();
    block = Short.toUnsignedInt(slice.readShort()) << 16;
    assert block >= 0;
    final int numValues = 1 + Short.toUnsignedInt(slice.readShort());
    index = nextBlockIndex;
    nextBlockIndex = index + numValues;
    if (numValues <= MAX_ARRAY_LENGTH) {
      method = Method.SPARSE;
      blockEnd = slice.getFilePointer() + (numValues << 1);
    } else if (numValues == 65536) {
      method = Method.ALL;
      blockEnd = slice.getFilePointer();
      gap = block - index - 1;
    } else {
      method = Method.DENSE;
      blockEnd = slice.getFilePointer() + (1 << 13);
      wordIndex = -1;
      numberOfOnes = index + 1;
      denseOrigoIndex = numberOfOnes;
    }
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc + 1);
  }

  public int index() {
    return index;
  }

  @Override
  public long cost() {
    return cost;
  }

  enum Method {
    SPARSE {
      @Override
      boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        // TODO: binary search
        for (; disi.index < disi.nextBlockIndex;) {
          int doc = Short.toUnsignedInt(disi.slice.readShort());
          disi.index++;
          if (doc >= targetInBlock) {
            disi.doc = disi.block | doc;
            disi.exists = true;
            return true;
          }
        }
        return false;
      }
      @Override
      boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        // TODO: binary search
        if (target == disi.doc) {
          return disi.exists;
        }
        for (; disi.index < disi.nextBlockIndex;) {
          int doc = Short.toUnsignedInt(disi.slice.readShort());
          disi.index++;
          if (doc >= targetInBlock) {
            if (doc != targetInBlock) {
              disi.index--;
              disi.slice.seek(disi.slice.getFilePointer() - Short.BYTES);
              break;
            }
            disi.exists = true;
            return true;
          }
        }
        disi.exists = false;
        return false;
      }
    },
    DENSE {
      @Override
      boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        final int targetWordIndex = targetInBlock >>> 6;

        for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
          disi.word = disi.slice.readLong();
          disi.numberOfOnes += Long.bitCount(disi.word);
        }
        disi.wordIndex = targetWordIndex;

        long leftBits = disi.word >>> target;
        if (leftBits != 0L) {
          disi.doc = target + Long.numberOfTrailingZeros(leftBits);
          disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
          return true;
        }

        // There were no set bits at the wanted position. Move forward until one is reached
        while (++disi.wordIndex < 1024) {
          // This could use the rank cache to skip empty spaces >= 512 bits, but it seems unrealistic
          // that such blocks would be DENSE
          disi.word = disi.slice.readLong();
          if (disi.word != 0) {
            disi.index = disi.numberOfOnes;
            disi.numberOfOnes += Long.bitCount(disi.word);
            disi.doc = disi.block | (disi.wordIndex << 6) | Long.numberOfTrailingZeros(disi.word);
            return true;
          }
        }
        // No set bits in the block at or after the wanted position.
        return false;
      }

      @Override
      boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
        final int targetInBlock = target & 0xFFFF;
        final int targetWordIndex = targetInBlock >>> 6;

        for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
          disi.word = disi.slice.readLong();
          disi.numberOfOnes += Long.bitCount(disi.word);
        }
        disi.wordIndex = targetWordIndex;

        long leftBits = disi.word >>> target;
        disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
        return (leftBits & 1L) != 0;
      }


    },
    ALL {
      @Override
      boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
        disi.doc = target;
        disi.index = target - disi.gap;
        return true;
      }
      @Override
      boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
        disi.index = target - disi.gap;
        return true;
      }
    };

    /** Advance to the first doc from the block that is equal to or greater than {@code target}.
     *  Return true if there is such a doc and false otherwise. */
    abstract boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException;

    /** Advance the iterator exactly to the position corresponding to the given {@code target}
     * and return whether this document exists. */
    abstract boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException;
  }

}
