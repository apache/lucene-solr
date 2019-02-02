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

import java.util.List;

import org.apache.lucene.util.BytesRef;

/** Utility class to read buffered points from in-heap arrays.
 *
 * @lucene.internal */
public final class HeapPointReader extends PointReader {
  private int curRead;
  final List<byte[]> blocks;
  final int valuesPerBlock;
  final int packedBytesLength;
  final int[] docIDs;
  final int end;
  final BytesRef scratch;

  public HeapPointReader(List<byte[]> blocks, int valuesPerBlock, int packedBytesLength, int[] docIDs, int start, int end) {
    this.blocks = blocks;
    this.valuesPerBlock = valuesPerBlock;
    this.docIDs = docIDs;
    curRead = start-1;
    this.end = end;
    this.packedBytesLength = packedBytesLength;
    scratch = new BytesRef();
    scratch.length = packedBytesLength;
  }

  @Override
  public boolean next() {
    curRead++;
    return curRead < end;
  }

  @Override
  public BytesRef packedValue() {
    getPackedValueSlice(curRead, scratch);
    return scratch;
  }

  /** Returns a reference, in <code>result</code>, to the byte[] slice holding this value */
  void getPackedValueSlice(int index, BytesRef result) {
    int block = index / valuesPerBlock;
    int blockIndex = index % valuesPerBlock;
    result.bytes = blocks.get(block);
    result.offset = blockIndex * packedBytesLength;
    assert result.length == packedBytesLength;
  }

  @Override
  public int docID() {
    return docIDs[curRead];
  }

  @Override
  public void close() {
  }
}
