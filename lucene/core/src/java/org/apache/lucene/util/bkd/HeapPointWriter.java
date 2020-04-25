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

import java.util.Arrays;

import org.apache.lucene.util.BytesRef;

/**
 * Utility class to write new points into in-heap arrays.
 *
 *  @lucene.internal
 *  */
public final class HeapPointWriter implements PointWriter {
  public final byte[] block;
  final int size;
  final int packedBytesLength;
  final int packedBytesDocIDLength;
  private final byte[] scratch;
  private int nextWrite;
  private boolean closed;

  private HeapPointReader.HeapPointValue pointValue;


  public HeapPointWriter(int size, int packedBytesLength) {
    this.packedBytesDocIDLength = packedBytesLength + Integer.BYTES;
    this.packedBytesLength = packedBytesLength;
    this.block = new byte[packedBytesDocIDLength * size];
    this.size = size;
    this.scratch = new byte[packedBytesDocIDLength];
    if (size > 0) {
      pointValue = new HeapPointReader.HeapPointValue(block, packedBytesLength);
    } else {
      //no values
      pointValue =  null;
    }
  }

  /** Returns a reference, in <code>result</code>, to the byte[] slice holding this value */
  public PointValue getPackedValueSlice(int index) {
    assert index < nextWrite : "nextWrite=" + (nextWrite) + " vs index=" + index;
    pointValue.setOffset(index * packedBytesDocIDLength);
    return pointValue;
  }

  @Override
  public void append(byte[] packedValue, int docID) {
    assert closed == false : "point writer is already closed";
    assert packedValue.length == packedBytesLength : "[packedValue] must have length [" + packedBytesLength + "] but was [" + packedValue.length + "]";
    assert nextWrite < size : "nextWrite=" + (nextWrite + 1) + " vs size=" + size;
    System.arraycopy(packedValue, 0, block, nextWrite * packedBytesDocIDLength, packedBytesLength);
    int position = nextWrite * packedBytesDocIDLength + packedBytesLength;
    block[position] = (byte) (docID >> 24);
    block[++position] = (byte) (docID >> 16);
    block[++position] = (byte) (docID >> 8);
    block[++position] = (byte) (docID >> 0);
    nextWrite++;
  }

  @Override
  public void append(PointValue pointValue) {
    assert closed == false : "point writer is already closed";
    assert nextWrite < size : "nextWrite=" + (nextWrite + 1) + " vs size=" + size;
    BytesRef packedValueDocID = pointValue.packedValueDocIDBytes();
    assert packedValueDocID.length == packedBytesDocIDLength : "[packedValue] must have length [" + (packedBytesDocIDLength) + "] but was [" + packedValueDocID.length + "]";
    System.arraycopy(packedValueDocID.bytes, packedValueDocID.offset, block, nextWrite * packedBytesDocIDLength, packedBytesDocIDLength);
    nextWrite++;
  }

  public void swap(int i, int j) {

    int indexI = i * packedBytesDocIDLength;
    int indexJ = j * packedBytesDocIDLength;

    // scratch1 = values[i]
    System.arraycopy(block, indexI, scratch, 0, packedBytesDocIDLength);
    // values[i] = values[j]
    System.arraycopy(block, indexJ, block, indexI, packedBytesDocIDLength);
    // values[j] = scratch1
    System.arraycopy(scratch, 0, block, indexJ, packedBytesDocIDLength);
  }

  public int computeCardinality(int from, int to, int numDataDims, int bytesPerDim, int[] commonPrefixLengths) {
    assert packedBytesLength == numDataDims * bytesPerDim;
    int leafCardinality = 1;
    for (int i = from + 1; i < to; i++) {
      for (int dim = 0; dim < numDataDims; dim++) {
        final int start = dim * bytesPerDim + commonPrefixLengths[dim];
        final int end = dim * bytesPerDim + bytesPerDim;
        if (Arrays.mismatch(block, i * packedBytesDocIDLength + start, i * packedBytesDocIDLength + end,
            block, (i - 1) * packedBytesDocIDLength  + start, (i - 1) * packedBytesDocIDLength + end) != -1) {
          leafCardinality++;
          break;
        }
      }
    }
    return leafCardinality;
  }

  @Override
  public long count() {
    return nextWrite;
  }

  @Override
  public PointReader getReader(long start, long length) {
    assert closed : "point writer is still open and trying to get a reader";
    assert start + length <= size: "start=" + start + " length=" + length + " docIDs.length=" + size;
    assert start + length <= nextWrite: "start=" + start + " length=" + length + " nextWrite=" + nextWrite;
    return new HeapPointReader(block, packedBytesLength, (int) start, Math.toIntExact(start+length));
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
    return "HeapPointWriter(count=" + nextWrite + " size=" + size + ")";
  }
}
