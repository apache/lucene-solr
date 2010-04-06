package org.apache.lucene.index.codecs.standard;

/**
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

import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.store.IndexInput;

import java.util.List;
import java.util.ArrayList;
import java.io.Closeable;
import java.io.IOException;

/** Represents a logical byte[] as a series of pages.  You
 *  can write-once into the logical byte[], using copy, and
 *  then retrieve slices (BytesRef) into it using fill. */
class PagedBytes implements Closeable {
  private final List<byte[]> blocks = new ArrayList<byte[]>();
  private final int blockSize;
  private final int blockBits;
  private final int blockMask;
  private int upto;
  private byte[] currentBlock;
  private final CloseableThreadLocal<byte[]> threadBuffers = new CloseableThreadLocal<byte[]>();

  private static final byte[] EMPTY_BYTES = new byte[0];

  /** 1<<blockBits must be bigger than biggest single
   *  BytesRef slice that will be pulled */
  public PagedBytes(int blockBits) {
    this.blockSize = 1 << blockBits;
    this.blockBits = blockBits;
    blockMask = blockSize-1;
    upto = blockSize;
  }

  /** Read this many bytes from in */
  public void copy(IndexInput in, long byteCount) throws IOException {
    while (byteCount > 0) {
      int left = blockSize - upto;
      if (left == 0) {
        if (currentBlock != null) {
          blocks.add(currentBlock);
        }
        currentBlock = new byte[blockSize];
        upto = 0;
        left = blockSize;
      }
      if (left < byteCount) {
        in.readBytes(currentBlock, upto, left, false);
        upto = blockSize;
        byteCount -= left;
      } else {
        in.readBytes(currentBlock, upto, (int) byteCount, false);
        upto += byteCount;
        byteCount = 0;
      }
    }
  }

  /** Commits final byte[], trimming it if necessary. */
  public void finish() {
    if (upto < blockSize) {
      final byte[] newBlock = new byte[upto];
      System.arraycopy(currentBlock, 0, newBlock, 0, upto);
      currentBlock = newBlock;
    }
    if (currentBlock == null) {
      currentBlock = EMPTY_BYTES;
    }
    blocks.add(currentBlock);
    currentBlock = null;
  }

  public long getPointer() {
    if (currentBlock == null) {
      return 0;
    } else {
      return (blocks.size() * ((long) blockSize)) + upto;
    }
  }

  /** Get a slice out of the byte array. */
  public void fill(BytesRef b, long start, int length) {
    assert length >= 0: "length=" + length;
    final int index = (int) (start >> blockBits);
    final int offset = (int) (start & blockMask);
    b.length = length;
    if (blockSize - offset >= length) {
      // Within block
      b.bytes = blocks.get(index);
      b.offset = offset;
    } else {
      // Split
      byte[] buffer = threadBuffers.get();
      if (buffer == null) {
        buffer = new byte[length];
        threadBuffers.set(buffer);
      } else if (buffer.length < length) {
        buffer = ArrayUtil.grow(buffer, length);
        threadBuffers.set(buffer);
      }
      b.bytes = buffer;
      b.offset = 0;
      System.arraycopy(blocks.get(index), offset, buffer, 0, blockSize-offset);
      System.arraycopy(blocks.get(1+index), 0, buffer, blockSize-offset, length-(blockSize-offset));
    }
  }

  public void close() {
    threadBuffers.close();
  }
}
