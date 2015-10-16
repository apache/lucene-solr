package org.apache.lucene.util.bkd;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

final class HeapPointWriter implements PointWriter {
  int[] docIDs;
  long[] ords;
  private int nextWrite;
  private boolean closed;
  final int maxSize;
  final int valuesPerBlock;
  final int packedBytesLength;
  final List<byte[]> blocks = new ArrayList<>();

  public HeapPointWriter(int initSize, int maxSize, int packedBytesLength) {
    System.out.println("HeapPointWriter.init initSize=" + initSize + " maxSize=" + maxSize + " packedBytesLength=" + packedBytesLength);
    docIDs = new int[initSize];
    ords = new long[initSize];
    this.maxSize = maxSize;
    this.packedBytesLength = packedBytesLength;
    // 4K per page, unless each value is > 4K:
    valuesPerBlock = Math.max(1, 4096/packedBytesLength);
  }

  public void copyFrom(HeapPointWriter other) {
    if (docIDs.length < other.nextWrite) {
      throw new IllegalStateException("docIDs.length=" + docIDs.length + " other.nextWrite=" + other.nextWrite);
    }
    System.arraycopy(other.docIDs, 0, docIDs, 0, other.nextWrite);
    System.arraycopy(other.ords, 0, ords, 0, other.nextWrite);
    for(byte[] block : other.blocks) {
      blocks.add(block.clone());
    }
    nextWrite = other.nextWrite;
  }

  void readPackedValue(int index, byte[] bytes) {
    assert bytes.length == packedBytesLength;
    int block = index / valuesPerBlock;
    int blockIndex = index % valuesPerBlock;
    System.arraycopy(blocks.get(block), blockIndex * packedBytesLength, bytes, 0, packedBytesLength);
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

  private int[] growExact(int[] arr, int size) {
    assert size > arr.length;
    int[] newArr = new int[size];
    System.arraycopy(arr, 0, newArr, 0, arr.length);
    return newArr;
  }

  private long[] growExact(long[] arr, int size) {
    assert size > arr.length;
    long[] newArr = new long[size];
    System.arraycopy(arr, 0, newArr, 0, arr.length);
    return newArr;
  }

  @Override
  public void append(byte[] packedValue, long ord, int docID) {
    assert closed == false;
    assert packedValue.length == packedBytesLength;
    if (ords.length == nextWrite) {
      int nextSize = Math.min(maxSize, ArrayUtil.oversize(nextWrite+1, RamUsageEstimator.NUM_BYTES_INT));
      assert nextSize > nextWrite: "nextSize=" + nextSize + " vs nextWrite=" + nextWrite;
      ords = growExact(ords, nextSize);
      docIDs = growExact(docIDs, nextSize);
    }
    writePackedValue(nextWrite, packedValue);
    ords[nextWrite] = ord;
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  @Override
  public PointReader getReader(long start) {
    return new HeapPointReader(blocks, valuesPerBlock, packedBytesLength, ords, docIDs, (int) start, nextWrite);
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
    return "HeapPointWriter(count=" + nextWrite + " alloc=" + ords.length + ")";
  }
}
