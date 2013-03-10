package org.apache.lucene.util.packed;

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

import static org.apache.lucene.util.packed.BlockPackedReaderIterator.readVLong;
import static org.apache.lucene.util.packed.BlockPackedReaderIterator.zigZagDecode;
import static org.apache.lucene.util.packed.BlockPackedWriter.BPV_SHIFT;
import static org.apache.lucene.util.packed.BlockPackedWriter.MIN_VALUE_EQUALS_0;
import static org.apache.lucene.util.packed.BlockPackedWriter.checkBlockSize;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

/**
 * Provides random access to a stream written with {@link BlockPackedWriter}.
 * @lucene.internal
 */
public final class BlockPackedReader {

  private final int blockShift, blockMask;
  private final long valueCount;
  private final long[] minValues;
  private final PackedInts.Reader[] subReaders;

  /** Sole constructor. */
  public BlockPackedReader(IndexInput in, int packedIntsVersion, int blockSize, long valueCount, boolean direct) throws IOException {
    checkBlockSize(blockSize);
    this.valueCount = valueCount;
    blockShift = Integer.numberOfTrailingZeros(blockSize);
    blockMask = blockSize - 1;
    final int numBlocks = (int) (valueCount / blockSize) + (valueCount % blockSize == 0 ? 0 : 1);
    if ((long) numBlocks * blockSize < valueCount) {
      throw new IllegalArgumentException("valueCount is too large for this block size");
    }
    long[] minValues = null;
    subReaders = new PackedInts.Reader[numBlocks];
    for (int i = 0; i < numBlocks; ++i) {
      final int token = in.readByte() & 0xFF;
      final int bitsPerValue = token >>> BPV_SHIFT;
      if (bitsPerValue > 64) {
        throw new IOException("Corrupted");
      }
      if ((token & MIN_VALUE_EQUALS_0) == 0) {
        if (minValues == null) {
          minValues = new long[numBlocks];
        }
        minValues[i] = zigZagDecode(1L + readVLong(in));
      }
      if (bitsPerValue == 0) {
        subReaders[i] = new PackedInts.NullReader(blockSize);
      } else {
        final int size = (int) Math.min(blockSize, valueCount - (long) i * blockSize);
        if (direct) {
          final long pointer = in.getFilePointer();
          subReaders[i] = PackedInts.getDirectReaderNoHeader(in, PackedInts.Format.PACKED, packedIntsVersion, size, bitsPerValue);
          in.seek(pointer + PackedInts.Format.PACKED.byteCount(packedIntsVersion, size, bitsPerValue));
        } else {
          subReaders[i] = PackedInts.getReaderNoHeader(in, PackedInts.Format.PACKED, packedIntsVersion, size, bitsPerValue);
        }
      }
    }
    this.minValues = minValues;
  }

  /** Get value at <code>index</code>. */
  public long get(long index) {
    assert index >= 0 && index < valueCount;
    final int block = (int) (index >>> blockShift);
    final int idx = (int) (index & blockMask);
    return (minValues == null ? 0 : minValues[block]) + subReaders[block].get(idx);
  }

}
