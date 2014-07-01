package org.apache.lucene.util;

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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * {@link DocIdSet} implementation based on pfor-delta encoding.
 * <p>This implementation is inspired from LinkedIn's Kamikaze
 * (http://data.linkedin.com/opensource/kamikaze) and Daniel Lemire's JavaFastPFOR
 * (https://github.com/lemire/JavaFastPFOR).</p>
 * <p>On the contrary to the original PFOR paper, exceptions are encoded with
 * FOR instead of Simple16.</p>
 */
public final class PForDeltaDocIdSet extends DocIdSet implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PForDeltaDocIdSet.class);

  static final int BLOCK_SIZE = 128;
  static final int MAX_EXCEPTIONS = 24; // no more than 24 exceptions per block
  static final PackedInts.Decoder[] DECODERS = new PackedInts.Decoder[32];
  static final int[] ITERATIONS = new int[32];
  static final int[] BYTE_BLOCK_COUNTS = new int[32];
  static final int MAX_BYTE_BLOCK_COUNT;
  static final PackedLongValues SINGLE_ZERO = PackedLongValues.packedBuilder(PackedInts.COMPACT).add(0L).build();
  static final PForDeltaDocIdSet EMPTY = new PForDeltaDocIdSet(null, 0, Integer.MAX_VALUE, SINGLE_ZERO, SINGLE_ZERO);
  static final int LAST_BLOCK = 1 << 5; // flag to indicate the last block
  static final int HAS_EXCEPTIONS = 1 << 6;
  static final int UNARY = 1 << 7;
  static {
    int maxByteBLockCount = 0;
    for (int i = 1; i < ITERATIONS.length; ++i) {
      DECODERS[i] = PackedInts.getDecoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, i);
      assert BLOCK_SIZE % DECODERS[i].byteValueCount() == 0;
      ITERATIONS[i] = BLOCK_SIZE / DECODERS[i].byteValueCount();
      BYTE_BLOCK_COUNTS[i] = ITERATIONS[i] * DECODERS[i].byteBlockCount();
      maxByteBLockCount = Math.max(maxByteBLockCount, DECODERS[i].byteBlockCount());
    }
    MAX_BYTE_BLOCK_COUNT = maxByteBLockCount;
  }

  /** A builder for {@link PForDeltaDocIdSet}. */
  public static class Builder {

    final GrowableByteArrayDataOutput data;
    final int[] buffer = new int[BLOCK_SIZE];
    final int[] exceptionIndices = new int[BLOCK_SIZE];
    final int[] exceptions = new int[BLOCK_SIZE];
    int bufferSize;
    int previousDoc;
    int cardinality;
    int indexInterval;
    int numBlocks;

    // temporary variables used when compressing blocks
    final int[] freqs = new int[32];
    int bitsPerValue;
    int numExceptions;
    int bitsPerException;

    /** Sole constructor. */
    public Builder() {
      data = new GrowableByteArrayDataOutput(128);
      bufferSize = 0;
      previousDoc = -1;
      indexInterval = 2;
      cardinality = 0;
      numBlocks = 0;
    }

    /** Set the index interval. Every <code>indexInterval</code>-th block will
     * be stored in the index. Set to {@link Integer#MAX_VALUE} to disable indexing. */
    public Builder setIndexInterval(int indexInterval) {
      if (indexInterval < 1) {
        throw new IllegalArgumentException("indexInterval must be >= 1");
      }
      this.indexInterval = indexInterval;
      return this;
    }

    /** Add a document to this builder. Documents must be added in order. */
    public Builder add(int doc) {
      if (doc <= previousDoc) {
        throw new IllegalArgumentException("Doc IDs must be provided in order, but previousDoc=" + previousDoc + " and doc=" + doc);
      }
      buffer[bufferSize++] = doc - previousDoc - 1;
      if (bufferSize == BLOCK_SIZE) {
        encodeBlock();
        bufferSize = 0;
      }
      previousDoc = doc;
      ++cardinality;
      return this;
    }

    /** Convenience method to add the content of a {@link DocIdSetIterator} to this builder. */
    public Builder add(DocIdSetIterator it) throws IOException {
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        add(doc);
      }
      return this;
    }

    void computeFreqs() {
      Arrays.fill(freqs, 0);
      for (int i = 0; i < bufferSize; ++i) {
        ++freqs[32 - Integer.numberOfLeadingZeros(buffer[i])];
      }
    }

    int pforBlockSize(int bitsPerValue, int numExceptions, int bitsPerException) {
      final PackedInts.Format format = PackedInts.Format.PACKED;
      long blockSize = 1 // header: number of bits per value
          + format.byteCount(PackedInts.VERSION_CURRENT, BLOCK_SIZE, bitsPerValue);
      if (numExceptions > 0) {
        blockSize += 2 // 2 additional bytes in case of exceptions: numExceptions and bitsPerException
            + numExceptions // indices of the exceptions
            + format.byteCount(PackedInts.VERSION_CURRENT, numExceptions, bitsPerException);
      }
      if (bufferSize < BLOCK_SIZE) {
        blockSize += 1; // length of the block
      }
      return (int) blockSize;
    }

    int unaryBlockSize() {
      int deltaSum = 0;
      for (int i = 0; i < BLOCK_SIZE; ++i) {
        deltaSum += 1 + buffer[i];
      }
      int blockSize = (deltaSum + 0x07) >>> 3; // round to the next byte
      ++blockSize; // header
      if (bufferSize < BLOCK_SIZE) {
        blockSize += 1; // length of the block
      }
      return blockSize;
    }

    int computeOptimalNumberOfBits() {
      computeFreqs();
      bitsPerValue = 31;
      numExceptions = 0;
      while (bitsPerValue > 0 && freqs[bitsPerValue] == 0) {
        --bitsPerValue;
      }
      final int actualBitsPerValue = bitsPerValue;
      int blockSize = pforBlockSize(bitsPerValue, numExceptions, bitsPerException);

      // Now try different values for bitsPerValue and pick the best one
      for (int bitsPerValue = this.bitsPerValue - 1, numExceptions = freqs[this.bitsPerValue]; bitsPerValue >= 0 && numExceptions <= MAX_EXCEPTIONS; numExceptions += freqs[bitsPerValue--]) {
        final int newBlockSize = pforBlockSize(bitsPerValue, numExceptions, actualBitsPerValue - bitsPerValue);
        if (newBlockSize < blockSize) {
          this.bitsPerValue = bitsPerValue;
          this.numExceptions = numExceptions;
          blockSize = newBlockSize;
        }
      }
      this.bitsPerException = actualBitsPerValue - bitsPerValue;
      assert bufferSize < BLOCK_SIZE || numExceptions < bufferSize;
      return blockSize;
    }

    void pforEncode() {
      if (numExceptions > 0) {
        final int mask = (1 << bitsPerValue) - 1;
        int ex = 0;
        for (int i = 0; i < bufferSize; ++i) {
          if (buffer[i] > mask) {
            exceptionIndices[ex] = i;
            exceptions[ex++] = buffer[i] >>> bitsPerValue;
            buffer[i] &= mask;
          }
        }
        assert ex == numExceptions;
        Arrays.fill(exceptions, numExceptions, BLOCK_SIZE, 0);
      }

      if (bitsPerValue > 0) {
        final PackedInts.Encoder encoder = PackedInts.getEncoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, bitsPerValue);
        final int numIterations = ITERATIONS[bitsPerValue];
        encoder.encode(buffer, 0, data.bytes, data.length, numIterations);
        data.length += encoder.byteBlockCount() * numIterations;
      }

      if (numExceptions > 0) {
        assert bitsPerException > 0;
        data.writeByte((byte) numExceptions);
        data.writeByte((byte) bitsPerException);
        final PackedInts.Encoder encoder = PackedInts.getEncoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, bitsPerException);
        final int numIterations = (numExceptions + encoder.byteValueCount() - 1) / encoder.byteValueCount();
        encoder.encode(exceptions, 0, data.bytes, data.length, numIterations);
        data.length += PackedInts.Format.PACKED.byteCount(PackedInts.VERSION_CURRENT, numExceptions, bitsPerException);
        for (int i = 0; i < numExceptions; ++i) {
          data.writeByte((byte) exceptionIndices[i]);
        }
      }
    }

    void unaryEncode() {
      int current = 0;
      for (int i = 0, doc = -1; i < BLOCK_SIZE; ++i) {
        doc += 1 + buffer[i];
        while (doc >= 8) {
          data.writeByte((byte) current);
          current = 0;
          doc -= 8;
        }
        current |= 1 << doc;
      }
      if (current != 0) {
        data.writeByte((byte) current);
      }
    }

    void encodeBlock() {
      final int originalLength = data.length;
      Arrays.fill(buffer, bufferSize, BLOCK_SIZE, 0);
      final int unaryBlockSize = unaryBlockSize();
      final int pforBlockSize = computeOptimalNumberOfBits();
      final int blockSize;
      if (pforBlockSize <= unaryBlockSize) {
        // use pfor
        blockSize = pforBlockSize;
        data.bytes = ArrayUtil.grow(data.bytes, data.length + blockSize + MAX_BYTE_BLOCK_COUNT);
        int token = bufferSize < BLOCK_SIZE ? LAST_BLOCK : 0;
        token |= bitsPerValue;
        if (numExceptions > 0) {
          token |= HAS_EXCEPTIONS;
        }
        data.writeByte((byte) token);
        pforEncode();
      } else {
        // use unary
        blockSize = unaryBlockSize;
        final int token = UNARY | (bufferSize < BLOCK_SIZE ? LAST_BLOCK : 0);
        data.writeByte((byte) token);
        unaryEncode();
      }

      if (bufferSize < BLOCK_SIZE) {
        data.writeByte((byte) bufferSize);
      }

      ++numBlocks;

      assert data.length - originalLength == blockSize : (data.length - originalLength) + " <> " + blockSize;
    }

    /** Build the {@link PForDeltaDocIdSet} instance. */
    public PForDeltaDocIdSet build() {
      assert bufferSize < BLOCK_SIZE;

      if (cardinality == 0) {
        assert previousDoc == -1;
        return EMPTY;
      }

      encodeBlock();
      final byte[] dataArr = Arrays.copyOf(data.bytes, data.length + MAX_BYTE_BLOCK_COUNT);

      final int indexSize = (numBlocks - 1) / indexInterval + 1;
      final PackedLongValues docIDs, offsets;
      if (indexSize <= 1) {
        docIDs = offsets = SINGLE_ZERO;
      } else {
        final int pageSize = 128;
        final PackedLongValues.Builder docIDsBuilder = PackedLongValues.monotonicBuilder(pageSize, PackedInts.COMPACT);
        final PackedLongValues.Builder offsetsBuilder = PackedLongValues.monotonicBuilder(pageSize, PackedInts.COMPACT);
        // Now build the index
        final Iterator it = new Iterator(dataArr, cardinality, Integer.MAX_VALUE, SINGLE_ZERO, SINGLE_ZERO);
        index:
        for (int k = 0; k < indexSize; ++k) {
          docIDsBuilder.add(it.docID() + 1);
          offsetsBuilder.add(it.offset);
          for (int i = 0; i < indexInterval; ++i) {
            it.skipBlock();
            if (it.docID() == DocIdSetIterator.NO_MORE_DOCS) {
              break index;
            }
          }
        }
        docIDs = docIDsBuilder.build();
        offsets = offsetsBuilder.build();
      }

      return new PForDeltaDocIdSet(dataArr, cardinality, indexInterval, docIDs, offsets);
    }

  }

  final byte[] data;
  final PackedLongValues docIDs, offsets; // for the index
  final int cardinality, indexInterval;

  PForDeltaDocIdSet(byte[] data, int cardinality, int indexInterval, PackedLongValues docIDs, PackedLongValues offsets) {
    this.data = data;
    this.cardinality = cardinality;
    this.indexInterval = indexInterval;
    this.docIDs = docIDs;
    this.offsets = offsets;
  }

  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public DocIdSetIterator iterator() {
    if (data == null) {
      return null;
    } else {
      return new Iterator(data, cardinality, indexInterval, docIDs, offsets);
    }
  }

  static class Iterator extends DocIdSetIterator {

    // index
    final int indexInterval;
    final PackedLongValues docIDs, offsets;

    final int cardinality;
    final byte[] data;
    int offset; // offset in data

    final int[] nextDocs;
    int i; // index in nextDeltas

    final int[] nextExceptions;

    int blockIdx;
    int docID;

    Iterator(byte[] data, int cardinality, int indexInterval, PackedLongValues docIDs, PackedLongValues offsets) {
      this.data = data;
      this.cardinality = cardinality;
      this.indexInterval = indexInterval;
      this.docIDs = docIDs;
      this.offsets = offsets;
      offset = 0;
      nextDocs = new int[BLOCK_SIZE];
      Arrays.fill(nextDocs, -1);
      i = BLOCK_SIZE;
      nextExceptions = new int[BLOCK_SIZE];
      blockIdx = -1;
      docID = -1;
    }

    @Override
    public int docID() {
      return docID;
    }

    void pforDecompress(byte token) {
      final int bitsPerValue = token & 0x1F;
      if (bitsPerValue == 0) {
        Arrays.fill(nextDocs, 0);
      } else {
        DECODERS[bitsPerValue].decode(data, offset, nextDocs, 0, ITERATIONS[bitsPerValue]);
        offset += BYTE_BLOCK_COUNTS[bitsPerValue];
      }
      if ((token & HAS_EXCEPTIONS) != 0) {
        // there are exceptions
        final int numExceptions = data[offset++];
        final int bitsPerException = data[offset++];
        final int numIterations = (numExceptions + DECODERS[bitsPerException].byteValueCount() - 1) / DECODERS[bitsPerException].byteValueCount();
        DECODERS[bitsPerException].decode(data, offset, nextExceptions, 0, numIterations);
        offset += PackedInts.Format.PACKED.byteCount(PackedInts.VERSION_CURRENT, numExceptions, bitsPerException);
        for (int i = 0; i < numExceptions; ++i) {
          nextDocs[data[offset++]] |= nextExceptions[i] << bitsPerValue;
        }
      }
      for (int previousDoc = docID, i = 0; i < BLOCK_SIZE; ++i) {
        final int doc = previousDoc + 1 + nextDocs[i];
        previousDoc = nextDocs[i] = doc;
      }
    }

    void unaryDecompress(byte token) {
      assert (token & HAS_EXCEPTIONS) == 0;
      int docID = this.docID;
      for (int i = 0; i < BLOCK_SIZE; ) {
        final byte b = data[offset++];
        for (int bitList = BitUtil.bitList(b); bitList != 0; ++i, bitList >>>= 4) {
          nextDocs[i] = docID + (bitList & 0x0F);
        }
        docID += 8;
      }
    }

    void decompressBlock() {
      final byte token = data[offset++];

      if ((token & UNARY) != 0) {
        unaryDecompress(token);
      } else {
        pforDecompress(token);
      }

      if ((token & LAST_BLOCK) != 0) {
        final int blockSize = data[offset++];
        Arrays.fill(nextDocs, blockSize, BLOCK_SIZE, NO_MORE_DOCS);
      }
      ++blockIdx;
    }

    void skipBlock() {
      assert i == BLOCK_SIZE;
      decompressBlock();
      docID = nextDocs[BLOCK_SIZE - 1];
    }

    @Override
    public int nextDoc() {
      if (i == BLOCK_SIZE) {
        decompressBlock();
        i = 0;
      }
      return docID = nextDocs[i++];
    }

    int forwardBinarySearch(int target) {
      // advance forward and double the window at each step
      final int indexSize = (int) docIDs.size();
      int lo = Math.max(blockIdx / indexInterval, 0), hi = lo + 1;
      assert blockIdx == -1 || docIDs.get(lo) <= docID;
      assert lo + 1 == docIDs.size() || docIDs.get(lo + 1) > docID;
      while (true) {
        if (hi >= indexSize) {
          hi = indexSize - 1;
          break;
        } else if (docIDs.get(hi) >= target) {
          break;
        }
        final int newLo = hi;
        hi += (hi - lo) << 1;
        lo = newLo;
      }

      // we found a window containing our target, let's binary search now
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        final int midDocID = (int) docIDs.get(mid);
        if (midDocID <= target) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      assert docIDs.get(hi) <= target;
      assert hi + 1 == docIDs.size() || docIDs.get(hi + 1) > target;
      return hi;
    }

    @Override
    public int advance(int target) throws IOException {
      assert target > docID;
      if (nextDocs[BLOCK_SIZE - 1] < target) {
        // not in the next block, now use the index
        final int index = forwardBinarySearch(target);
        final int offset = (int) offsets.get(index);
        if (offset > this.offset) {
          this.offset = offset;
          docID = (int) docIDs.get(index) - 1;
          blockIdx = index * indexInterval - 1;
          while (true) {
            decompressBlock();
            if (nextDocs[BLOCK_SIZE - 1] >= target) {
              break;
            }
            docID = nextDocs[BLOCK_SIZE - 1];
          }
          i = 0;
        }
      }
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return cardinality;
    }

  }

  /** Return the number of documents in this {@link DocIdSet} in constant time. */
  public int cardinality() {
    return cardinality;
  }

  @Override
  public long ramBytesUsed() {
    if (this == EMPTY) {
      return 0L;
    }
    long ramBytesUsed = BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(data);
    if (docIDs != SINGLE_ZERO) {
      ramBytesUsed += docIDs.ramBytesUsed();
    }
    if (offsets != SINGLE_ZERO) {
      ramBytesUsed += offsets.ramBytesUsed();
    }
    return ramBytesUsed;
  }

}
