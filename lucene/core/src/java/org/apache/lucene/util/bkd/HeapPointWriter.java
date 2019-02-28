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

import org.apache.lucene.util.BytesRef;

/**
 * Utility class to write new points into in-heap arrays.
 *
 *  @lucene.internal
 *  */
public final class HeapPointWriter implements PointWriter {
  public final int[] docIDs;
  public final byte[] block;
  final int size;
  final int packedBytesLength;
  private final byte[] scratch;
  private int nextWrite;
  private boolean closed;

  private HeapPointReader.HeapPointValue offlinePointValue;


  public HeapPointWriter(int size, int packedBytesLength) {
    this.docIDs = new int[size];
    this.block = new byte[packedBytesLength * size];
    this.size = size;
    this.packedBytesLength = packedBytesLength;
    this.scratch = new byte[packedBytesLength];
    if (size > 0) {
      offlinePointValue = new HeapPointReader.HeapPointValue(block, packedBytesLength);
    } else {
      //no values
      offlinePointValue =  new HeapPointReader.HeapPointValue(block, 0);
    }
  }

  /** Returns a reference, in <code>result</code>, to the byte[] slice holding this value */
  public PointValue getPackedValueSlice(int index) {
    assert index < nextWrite : "nextWrite=" + (nextWrite) + " vs index=" + index;
    offlinePointValue.setValue(index * packedBytesLength, docIDs[index]);
    return offlinePointValue;
  }

  @Override
  public void append(byte[] packedValue, int docID) {
    assert closed == false : "point writer is already closed";
    assert packedValue.length == packedBytesLength : "[packedValue] must have length [" + packedBytesLength + "] but was [" + packedValue.length + "]";
    assert nextWrite < size : "nextWrite=" + (nextWrite + 1) + " vs size=" + size;
    System.arraycopy(packedValue, 0, block, nextWrite * packedBytesLength, packedBytesLength);
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  @Override
  public void append(PointValue pointValue) {
    assert closed == false : "point writer is already closed";
    assert nextWrite < size : "nextWrite=" + (nextWrite + 1) + " vs size=" + size;
    BytesRef packedValue = pointValue.packedValue();
    assert packedValue.length == packedBytesLength : "[packedValue] must have length [" + (packedBytesLength) + "] but was [" + packedValue.length + "]";
    System.arraycopy(packedValue.bytes, packedValue.offset, block, nextWrite * packedBytesLength, packedBytesLength);
    docIDs[nextWrite] = pointValue.docID();
    nextWrite++;
  }

  public void swap(int i, int j) {
    int docID = docIDs[i];
    docIDs[i] = docIDs[j];
    docIDs[j] = docID;

    int indexI = i * packedBytesLength;
    int indexJ = j * packedBytesLength;

    // scratch1 = values[i]
    System.arraycopy(block, indexI, scratch, 0, packedBytesLength);
    // values[i] = values[j]
    System.arraycopy(block, indexJ, block, indexI, packedBytesLength);
    // values[j] = scratch1
    System.arraycopy(scratch, 0, block, indexJ, packedBytesLength);
  }

  @Override
  public long count() {
    return nextWrite;
  }

  @Override
  public PointReader getReader(long start, long length) {
    assert closed : "point writer is still open and trying to get a reader";
    assert start + length <= docIDs.length: "start=" + start + " length=" + length + " docIDs.length=" + docIDs.length;
    assert start + length <= nextWrite: "start=" + start + " length=" + length + " nextWrite=" + nextWrite;
    return new HeapPointReader(block, packedBytesLength, docIDs, (int) start, Math.toIntExact(start+length));
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
    return "HeapPointWriter(count=" + nextWrite + " size=" + docIDs.length + ")";
  }
}
