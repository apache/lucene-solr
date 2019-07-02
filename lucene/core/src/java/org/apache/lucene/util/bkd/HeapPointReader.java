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
  final int packedBytesDocIDLength;
  final int end;
  private final HeapPointValue pointValue;

  public HeapPointReader(byte[] block, int packedBytesLength, int start, int end) {
    this.block = block;
    curRead = start-1;
    this.end = end;
    this.packedBytesLength = packedBytesLength;
    this.packedBytesDocIDLength = packedBytesLength + Integer.BYTES;
    if (start < end) {
      this.pointValue = new HeapPointValue(block, packedBytesLength);
    } else {
      //no values
      this.pointValue = null;
    }
  }

  @Override
  public boolean next() {
    curRead++;
    return curRead < end;
  }

  @Override
  public PointValue pointValue() {
    pointValue.setOffset(curRead * packedBytesDocIDLength);
    return pointValue;
  }

  @Override
  public void close() {
  }

  /**
   * Reusable implementation for a point value on-heap
   */
  static class HeapPointValue implements PointValue {

    final BytesRef packedValue;
    final BytesRef packedValueDocID;
    final int packedValueLength;

    HeapPointValue(byte[] value, int packedValueLength) {
      this.packedValueLength = packedValueLength;
      this.packedValue = new BytesRef(value, 0, packedValueLength);
      this.packedValueDocID = new BytesRef(value, 0, packedValueLength + Integer.BYTES);
    }

    /**
     * Sets a new value by changing the offset.
     */
    public void setOffset(int offset) {
      packedValue.offset = offset;
      packedValueDocID.offset = offset;
    }

    @Override
    public BytesRef packedValue() {
      return packedValue;
    }

    @Override
    public int docID() {
      int position = packedValueDocID.offset + packedValueLength;
      return ((packedValueDocID.bytes[position] & 0xFF) << 24) | ((packedValueDocID.bytes[++position] & 0xFF) << 16)
          | ((packedValueDocID.bytes[++position] & 0xFF) <<  8) |  (packedValueDocID.bytes[++position] & 0xFF);
    }

    @Override
    public BytesRef packedValueDocIDBytes() {
      return packedValueDocID;
    }
  }
}
