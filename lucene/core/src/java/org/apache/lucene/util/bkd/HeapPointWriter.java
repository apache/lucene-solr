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
package org.apache.lucene.util.bkd;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** Utility class to write new points into in-heap arrays.
 *
 *  @lucene.internal */
public final class HeapPointWriter implements PointWriter {
  public int[] docIDs;
  public long[] ordsLong;
  public int[] ords;
  private int nextWrite;
  private boolean closed;
  final int maxSize;
  public final int valuesPerBlock;
  final int packedBytesLength;
  final boolean singleValuePerDoc;
  // NOTE: can't use ByteBlockPool because we need random-write access when sorting in heap
  public final List<byte[]> blocks = new ArrayList<>();

  public HeapPointWriter(int initSize, int maxSize, int packedBytesLength, boolean longOrds, boolean singleValuePerDoc) {
    docIDs = new int[initSize];
    this.maxSize = maxSize;
    this.packedBytesLength = packedBytesLength;
    this.singleValuePerDoc = singleValuePerDoc;
    if (singleValuePerDoc) {
      this.ordsLong = null;
      this.ords = null;
    } else {
      if (longOrds) {
        this.ordsLong = new long[initSize];
      } else {
        this.ords = new int[initSize];
      }
    }
    // 4K per page, unless each value is > 4K:
    valuesPerBlock = Math.max(1, 4096/packedBytesLength);
  }

  public void copyFrom(HeapPointWriter other) {
    if (docIDs.length < other.nextWrite) {
      throw new IllegalStateException("docIDs.length=" + docIDs.length + " other.nextWrite=" + other.nextWrite);
    }
    System.arraycopy(other.docIDs, 0, docIDs, 0, other.nextWrite);
    if (singleValuePerDoc == false) {
      if (other.ords != null) {
        assert this.ords != null;
        System.arraycopy(other.ords, 0, ords, 0, other.nextWrite);
      } else {
        assert this.ordsLong != null;
        System.arraycopy(other.ordsLong, 0, ordsLong, 0, other.nextWrite);
      }
    }

    for(byte[] block : other.blocks) {
      blocks.add(block.clone());
    }
    nextWrite = other.nextWrite;
  }

  public void readPackedValue(int index, byte[] bytes) {
    assert bytes.length == packedBytesLength;
    int block = index / valuesPerBlock;
    int blockIndex = index % valuesPerBlock;
    System.arraycopy(blocks.get(block), blockIndex * packedBytesLength, bytes, 0, packedBytesLength);
  }

  /** Returns a reference, in <code>result</code>, to the byte[] slice holding this value */
  public void getPackedValueSlice(int index, BytesRef result) {
    int block = index / valuesPerBlock;
    int blockIndex = index % valuesPerBlock;
    result.bytes = blocks.get(block);
    result.offset = blockIndex * packedBytesLength;
    assert result.length == packedBytesLength;
  }

  void writePackedValue(int index, byte[] bytes) {
    assert bytes.length == packedBytesLength;
    int block = index / valuesPerBlock;
    int blockIndex = index % valuesPerBlock;
    //System.out.println("writePackedValue: index=" + index + " bytes.length=" + bytes.length + " block=" + block + " blockIndex=" + blockIndex + " valuesPerBlock=" + valuesPerBlock);
    while (blocks.size() <= block) {
      // If this is the last block, only allocate as large as necessary for maxSize:
      int valuesInBlock = Math.min(valuesPerBlock, maxSize - (blocks.size() * valuesPerBlock));
      blocks.add(new byte[valuesInBlock*packedBytesLength]);
    }
    System.arraycopy(bytes, 0, blocks.get(block), blockIndex * packedBytesLength, packedBytesLength);
  }

  @Override
  public void append(byte[] packedValue, long ord, int docID) {
    assert closed == false;
    assert packedValue.length == packedBytesLength;
    if (docIDs.length == nextWrite) {
      int nextSize = Math.min(maxSize, ArrayUtil.oversize(nextWrite+1, Integer.BYTES));
      assert nextSize > nextWrite: "nextSize=" + nextSize + " vs nextWrite=" + nextWrite;
      docIDs = Arrays.copyOf(docIDs, nextSize);
      if (singleValuePerDoc == false) {
        if (ordsLong != null) {
          ordsLong = Arrays.copyOf(ordsLong, nextSize);
        } else {
          ords = Arrays.copyOf(ords, nextSize);
        }
      }
    }
    writePackedValue(nextWrite, packedValue);
    if (singleValuePerDoc == false) {
      if (ordsLong != null) {
        ordsLong[nextWrite] = ord;
      } else {
        assert ord <= Integer.MAX_VALUE;
        ords[nextWrite] = (int) ord;
      }
    }
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  @Override
  public PointReader getReader(long start, long length) {
    assert start + length <= docIDs.length: "start=" + start + " length=" + length + " docIDs.length=" + docIDs.length;
    assert start + length <= nextWrite: "start=" + start + " length=" + length + " nextWrite=" + nextWrite;
    return new HeapPointReader(blocks, valuesPerBlock, packedBytesLength, ords, ordsLong, docIDs, (int) start, Math.toIntExact(start+length), singleValuePerDoc);
  }

  @Override
  public PointReader getSharedReader(long start, long length, List<Closeable> toCloseHeroically) {
    return new HeapPointReader(blocks, valuesPerBlock, packedBytesLength, ords, ordsLong, docIDs, (int) start, nextWrite, singleValuePerDoc);
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public void destroy() {
  }

  @Override
  public String toString() {
    return "HeapPointWriter(count=" + nextWrite + " alloc=" + docIDs.length + ")";
  }
}
