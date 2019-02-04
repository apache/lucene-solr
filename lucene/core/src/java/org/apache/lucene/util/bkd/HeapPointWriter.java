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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Utility class to write new points into in-heap arrays.
 *
 *  @lucene.internal
 *  */
public final class HeapPointWriter implements PointWriter {
  public int[] docIDs;
  private int nextWrite;
  private boolean closed;
  final int maxSize;
  public final int valuesPerBlock;
  final int packedBytesLength;
  // NOTE: can't use ByteBlockPool because we need random-write access when sorting in heap
  public final List<byte[]> blocks = new ArrayList<>();
  private byte[] scratch;


  public HeapPointWriter(int initSize, int maxSize, int packedBytesLength) {
    docIDs = new int[initSize];
    this.maxSize = maxSize;
    this.packedBytesLength = packedBytesLength;
    // 4K per page, unless each value is > 4K:
    valuesPerBlock = Math.max(1, 4096/packedBytesLength);
    scratch = new byte[packedBytesLength];
  }

  public void copyFrom(HeapPointWriter other) {
    if (docIDs.length < other.nextWrite) {
      throw new IllegalStateException("docIDs.length=" + docIDs.length + " other.nextWrite=" + other.nextWrite);
    }
    System.arraycopy(other.docIDs, 0, docIDs, 0, other.nextWrite);
    for(byte[] block : other.blocks) {
      blocks.add(block.clone());
    }
    nextWrite = other.nextWrite;
  }

  /** Returns a reference, in <code>result</code>, to the byte[] slice holding this value */
  public void getPackedValueSlice(int index, BytesRef result) {
    int block = index / valuesPerBlock;
    int blockIndex = index % valuesPerBlock;
    result.bytes = blocks.get(block);
    result.offset = blockIndex * packedBytesLength;
    result.length = packedBytesLength;
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

  void writePackedValue(int index, BytesRef bytes) {
    assert bytes.length == packedBytesLength;
    int block = index / valuesPerBlock;
    int blockIndex = index % valuesPerBlock;
    //System.out.println("writePackedValue: index=" + index + " bytes.length=" + bytes.length + " block=" + block + " blockIndex=" + blockIndex + " valuesPerBlock=" + valuesPerBlock);
    while (blocks.size() <= block) {
      // If this is the last block, only allocate as large as necessary for maxSize:
      int valuesInBlock = Math.min(valuesPerBlock, maxSize - (blocks.size() * valuesPerBlock));
      blocks.add(new byte[valuesInBlock*packedBytesLength]);
    }
    System.arraycopy(bytes.bytes, bytes.offset, blocks.get(block), blockIndex * packedBytesLength, packedBytesLength);
  }

  @Override
  public void append(byte[] packedValue, int docID) {
    assert closed == false;
    assert packedValue.length == packedBytesLength;
    if (docIDs.length == nextWrite) {
      int nextSize = Math.min(maxSize, ArrayUtil.oversize(nextWrite+1, Integer.BYTES));
      assert nextSize > nextWrite: "nextSize=" + nextSize + " vs nextWrite=" + nextWrite;
      docIDs = ArrayUtil.growExact(docIDs, nextSize);
    }
    writePackedValue(nextWrite, packedValue);
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  @Override
  public void append(BytesRef packedValue, int docID) {
    assert closed == false;
    assert packedValue.length == packedBytesLength;
    if (docIDs.length == nextWrite) {
      int nextSize = Math.min(maxSize, ArrayUtil.oversize(nextWrite+1, Integer.BYTES));
      assert nextSize > nextWrite: "nextSize=" + nextSize + " vs nextWrite=" + nextWrite;
      docIDs = ArrayUtil.growExact(docIDs, nextSize);
    }
    writePackedValue(nextWrite, packedValue);
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  public void swap(int i, int j) {
    int docID = docIDs[i];
    docIDs[i] = docIDs[j];
    docIDs[j] = docID;


    byte[] blockI = blocks.get(i / valuesPerBlock);
    int indexI = (i % valuesPerBlock) * packedBytesLength;
    byte[] blockJ = blocks.get(j / valuesPerBlock);
    int indexJ = (j % valuesPerBlock) * packedBytesLength;

    // scratch1 = values[i]
    System.arraycopy(blockI, indexI, scratch, 0, packedBytesLength);
    // values[i] = values[j]
    System.arraycopy(blockJ, indexJ, blockI, indexI, packedBytesLength);
    // values[j] = scratch1
    System.arraycopy(scratch, 0, blockJ, indexJ, packedBytesLength);
  }

  @Override
  public long count() {
    return nextWrite;
  }

  @Override
  public PointReader getReader(long start, long length) {
    assert start + length <= docIDs.length: "start=" + start + " length=" + length + " docIDs.length=" + docIDs.length;
    assert start + length <= nextWrite: "start=" + start + " length=" + length + " nextWrite=" + nextWrite;
    return new HeapPointReader(blocks, valuesPerBlock, packedBytesLength, docIDs, (int) start, Math.toIntExact(start+length));
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
