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


  public HeapPointWriter(int size, int packedBytesLength) {
    this.docIDs = new int[size];
    this.block = new byte[packedBytesLength * size];
    this.size = size;
    this.packedBytesLength = packedBytesLength;
    this.scratch = new byte[packedBytesLength];
  }

  /** Returns a reference, in <code>result</code>, to the byte[] slice holding this value */
  public void getPackedValueSlice(int index, BytesRef result) {
    result.bytes = block;
    result.offset = index * packedBytesLength;
    result.length = packedBytesLength;
  }

  @Override
  public void append(byte[] packedValue, int docID) {
    assert closed == false;
    assert packedValue.length == packedBytesLength;
    System.arraycopy(packedValue, 0,block, nextWrite * packedBytesLength, packedBytesLength);
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  @Override
  public void append(BytesRef packedValue, int docID) {
    assert closed == false;
    assert packedValue.length == packedBytesLength;
    System.arraycopy(packedValue.bytes, packedValue.offset, block, nextWrite * packedBytesLength, packedBytesLength);
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  @Override
  public void append(BytesRef packedValueWithDocId) {
    assert closed == false;
    assert packedValueWithDocId.length == packedBytesLength + Integer.BYTES;
    System.arraycopy(packedValueWithDocId.bytes, packedValueWithDocId.offset, block, nextWrite * packedBytesLength, packedBytesLength);
    int docID = fromByteArray(packedValueWithDocId.offset + packedBytesLength, packedValueWithDocId.bytes);
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  private int fromByteArray(int offset, byte[] bytes) {
    return (bytes[offset] & 0xFF) << 24 | (bytes[++offset] & 0xFF) << 16 | (bytes[++offset] & 0xFF) << 8 | (bytes[++offset] & 0xFF);
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
