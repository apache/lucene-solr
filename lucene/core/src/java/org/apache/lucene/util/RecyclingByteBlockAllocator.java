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
package org.apache.lucene.util;

import org.apache.lucene.util.ByteBlockPool.Allocator;

/**
 * A {@link ByteBlockPool.Allocator} implementation that recycles unused byte blocks in a buffer and
 * reuses them in subsequent calls to {@link #getByteBlock()}.
 *
 * <p>Note: This class is not thread-safe
 *
 * @lucene.internal
 */
public final class RecyclingByteBlockAllocator extends ByteBlockPool.Allocator {
  private byte[][] freeByteBlocks;
  private final int maxBufferedBlocks;
  private int freeBlocks = 0;
  private final Counter bytesUsed;
  public static final int DEFAULT_BUFFERED_BLOCKS = 64;

  /**
   * Creates a new {@link RecyclingByteBlockAllocator}
   *
   * @param blockSize the block size in bytes
   * @param maxBufferedBlocks maximum number of buffered byte block
   * @param bytesUsed {@link Counter} reference counting internally allocated bytes
   */
  public RecyclingByteBlockAllocator(int blockSize, int maxBufferedBlocks, Counter bytesUsed) {
    super(blockSize);
    freeByteBlocks = new byte[maxBufferedBlocks][];
    this.maxBufferedBlocks = maxBufferedBlocks;
    this.bytesUsed = bytesUsed;
  }

  /**
   * Creates a new {@link RecyclingByteBlockAllocator}.
   *
   * @param blockSize the block size in bytes
   * @param maxBufferedBlocks maximum number of buffered byte block
   */
  public RecyclingByteBlockAllocator(int blockSize, int maxBufferedBlocks) {
    this(blockSize, maxBufferedBlocks, Counter.newCounter(false));
  }

  /**
   * Creates a new {@link RecyclingByteBlockAllocator} with a block size of {@link
   * ByteBlockPool#BYTE_BLOCK_SIZE}, upper buffered docs limit of {@link #DEFAULT_BUFFERED_BLOCKS}
   * ({@value #DEFAULT_BUFFERED_BLOCKS}).
   */
  public RecyclingByteBlockAllocator() {
    this(ByteBlockPool.BYTE_BLOCK_SIZE, 64, Counter.newCounter(false));
  }

  @Override
  public byte[] getByteBlock() {
    if (freeBlocks == 0) {
      bytesUsed.addAndGet(blockSize);
      return new byte[blockSize];
    }
    final byte[] b = freeByteBlocks[--freeBlocks];
    freeByteBlocks[freeBlocks] = null;
    return b;
  }

  @Override
  public void recycleByteBlocks(byte[][] blocks, int start, int end) {
    final int numBlocks = Math.min(maxBufferedBlocks - freeBlocks, end - start);
    final int size = freeBlocks + numBlocks;
    if (size >= freeByteBlocks.length) {
      final byte[][] newBlocks =
          new byte[ArrayUtil.oversize(size, RamUsageEstimator.NUM_BYTES_OBJECT_REF)][];
      System.arraycopy(freeByteBlocks, 0, newBlocks, 0, freeBlocks);
      freeByteBlocks = newBlocks;
    }
    final int stop = start + numBlocks;
    for (int i = start; i < stop; i++) {
      freeByteBlocks[freeBlocks++] = blocks[i];
      blocks[i] = null;
    }
    for (int i = stop; i < end; i++) {
      blocks[i] = null;
    }
    bytesUsed.addAndGet(-(end - stop) * blockSize);
    assert bytesUsed.get() >= 0;
  }

  /** @return the number of currently buffered blocks */
  public int numBufferedBlocks() {
    return freeBlocks;
  }

  /** @return the number of bytes currently allocated by this {@link Allocator} */
  public long bytesUsed() {
    return bytesUsed.get();
  }

  /** @return the maximum number of buffered byte blocks */
  public int maxBufferedBlocks() {
    return maxBufferedBlocks;
  }

  /**
   * Removes the given number of byte blocks from the buffer if possible.
   *
   * @param num the number of byte blocks to remove
   * @return the number of actually removed buffers
   */
  public int freeBlocks(int num) {
    assert num >= 0 : "free blocks must be >= 0 but was: " + num;
    final int stop;
    final int count;
    if (num > freeBlocks) {
      stop = 0;
      count = freeBlocks;
    } else {
      stop = freeBlocks - num;
      count = num;
    }
    while (freeBlocks > stop) {
      freeByteBlocks[--freeBlocks] = null;
    }
    bytesUsed.addAndGet(-count * blockSize);
    assert bytesUsed.get() >= 0;
    return count;
  }
}
