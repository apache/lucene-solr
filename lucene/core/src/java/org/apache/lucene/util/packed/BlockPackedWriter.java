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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.DataOutput;

/**
 * A writer for large sequences of longs.
 * <p>
 * The sequence is divided into fixed-size blocks and for each block, the
 * difference between each value and the minimum value of the block is encoded
 * using as few bits as possible. Memory usage of this class is proportional to
 * the block size. Each block has an overhead between 1 and 10 bytes to store
 * the minimum value and the number of bits per value of the block.
 * @see BlockPackedReader
 * @lucene.internal
 */
public final class BlockPackedWriter {

  static final int MAX_BLOCK_SIZE = 1 << (30 - 3);
  static final int MIN_VALUE_EQUALS_0 = 1 << 0;
  static final int BPV_SHIFT = 1;

  static void checkBlockSize(int blockSize) {
    if (blockSize <= 0 || blockSize > MAX_BLOCK_SIZE) {
      throw new IllegalArgumentException("blockSize must be > 0 and < " + MAX_BLOCK_SIZE + ", got " + blockSize);
    }
    if (blockSize % 64 != 0) {
      throw new IllegalArgumentException("blockSize must be a multiple of 64, got " + blockSize);
    }
  }

  static long zigZagEncode(long n) {
    return (n >> 63) ^ (n << 1);
  }

  // same as DataOutput.writeVLong but accepts negative values
  static void writeVLong(DataOutput out, long i) throws IOException {
    int k = 0;
    while ((i & ~0x7FL) != 0L && k++ < 8) {
      out.writeByte((byte)((i & 0x7FL) | 0x80L));
      i >>>= 7;
    }
    out.writeByte((byte) i);
  }

  final DataOutput out;
  final long[] values;
  byte[] blocks;
  int off;
  long ord;
  boolean finished;

  /**
   * Sole constructor.
   * @param blockSize the number of values of a single block, must be a multiple of <tt>64</tt>
   */
  public BlockPackedWriter(DataOutput out, int blockSize) {
    checkBlockSize(blockSize);
    this.out = out;
    values = new long[blockSize];
    off = 0;
    ord = 0L;
    finished = false;
  }

  private void checkNotFinished() {
    if (finished) {
      throw new IllegalStateException("Already finished");
    }
  }

  /** Append a new long. */
  public void add(long l) throws IOException {
    checkNotFinished();
    if (off == values.length) {
      flush();
    }
    values[off++] = l;
    ++ord;
  }

  /** Flush all buffered data to disk. This instance is not usable anymore
   *  after this method has been called. */
  public void finish() throws IOException {
    checkNotFinished();
    if (off > 0) {
      flush();
    }
    finished = true;
  }

  private void flush() throws IOException {
    assert off > 0;
    long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    for (int i = 0; i < off; ++i) {
      min = Math.min(values[i], min);
      max = Math.max(values[i], max);
    }

    final long delta = max - min;
    final int bitsRequired = delta < 0 ? 64 : delta == 0L ? 0 : PackedInts.bitsRequired(delta);
    if (bitsRequired == 64) {
      // no need to delta-encode
      min = 0L;
    } else if (min > 0L) {
      // make min as small as possible so that writeVLong requires fewer bytes
      min = Math.max(0L, max - PackedInts.maxValue(bitsRequired));
    }

    final int token = (bitsRequired << BPV_SHIFT) | (min == 0 ? MIN_VALUE_EQUALS_0 : 0);
    out.writeByte((byte) token);

    if (min != 0) {
      writeVLong(out, zigZagEncode(min) - 1);
    }

    if (bitsRequired > 0) {
      if (min != 0) {
        for (int i = 0; i < off; ++i) {
          values[i] -= min;
        }
      }
      final PackedInts.Encoder encoder = PackedInts.getEncoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, bitsRequired);
      final int iterations = values.length / encoder.valueCount();
      final int blockSize = encoder.blockCount() * 8 * iterations;
      if (blocks == null || blocks.length < blockSize) {
        blocks = new byte[blockSize];
      }
      if (off < values.length) {
        Arrays.fill(values, off, values.length, 0L);
      }
      encoder.encode(values, 0, blocks, 0, iterations);
      final int blockCount = (int) PackedInts.Format.PACKED.byteCount(PackedInts.VERSION_CURRENT, off, bitsRequired);
      out.writeBytes(blocks, blockCount);
    }

    off = 0;
  }

  /** Return the number of values which have been added. */
  public long ord() {
    return ord;
  }

}
