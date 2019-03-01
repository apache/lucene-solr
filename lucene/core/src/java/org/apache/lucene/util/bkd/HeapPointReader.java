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
 * Utility class to read buffered points from in-heap arrays.
 *
 * @lucene.internal
 * */
public final class HeapPointReader implements PointReader {
  private int curRead;
  final byte[] block;
  final int packedBytesLength;
  final int[] docIDs;
  final int end;
  private final HeapPointValue pointValue;

  public HeapPointReader(byte[] block, int packedBytesLength, int[] docIDs, int start, int end) {
    this.block = block;
    this.docIDs = docIDs;
    curRead = start-1;
    this.end = end;
    this.packedBytesLength = packedBytesLength;
    if (start < end) {
      this.pointValue = new HeapPointValue(block, packedBytesLength);
    } else {
      //no values
      this.pointValue = new HeapPointValue(block, 0);
    }
  }

  @Override
  public boolean next() {
    curRead++;
    return curRead < end;
  }

  @Override
  public PointValue pointValue() {
    pointValue.setValue(curRead * packedBytesLength, docIDs[curRead]);
    return pointValue;
  }

  @Override
  public void close() {
  }

  /**
   * Reusable implementation for a point value on-heap
   */
  static class HeapPointValue implements PointValue {

    BytesRef packedValue;
    BytesRef docIDBytes;
    int docID;

    public HeapPointValue(byte[] value, int packedLength) {
      packedValue = new BytesRef(value, 0, packedLength);
      docIDBytes = new BytesRef(new byte[4]);
    }

    /**
     * Sets a new value by changing the offset and docID.
     */
    public void setValue(int offset, int docID) {
      this.docID = docID;
      packedValue.offset = offset;
    }

    @Override
    public BytesRef packedValue() {
      return packedValue;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public BytesRef docIDBytes() {
      docIDBytes.bytes[0] = (byte) (docID >> 24);
      docIDBytes.bytes[1] = (byte) (docID >> 16);
      docIDBytes.bytes[2] = (byte) (docID >> 8);
      docIDBytes.bytes[3] = (byte) (docID >> 0);
      return docIDBytes;
    }
  }
}
