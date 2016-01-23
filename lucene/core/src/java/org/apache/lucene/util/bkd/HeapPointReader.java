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

import java.util.List;

import org.apache.lucene.util.PagedBytes;

final class HeapPointReader implements PointReader {
  private int curRead;
  final List<byte[]> blocks;
  final int valuesPerBlock;
  final int packedBytesLength;
  final long[] ords;
  final int[] docIDs;
  final int end;
  final byte[] scratch;

  HeapPointReader(List<byte[]> blocks, int valuesPerBlock, int packedBytesLength, long[] ords, int[] docIDs, int start, int end) {
    this.blocks = blocks;
    this.valuesPerBlock = valuesPerBlock;
    this.ords = ords;
    this.docIDs = docIDs;
    curRead = start-1;
    this.end = end;
    this.packedBytesLength = packedBytesLength;
    scratch = new byte[packedBytesLength];
  }

  void writePackedValue(int index, byte[] bytes) {
    int block = index / valuesPerBlock;
    int blockIndex = index % valuesPerBlock;
    while (blocks.size() <= block) {
      blocks.add(new byte[valuesPerBlock*packedBytesLength]);
    }
    System.arraycopy(bytes, 0, blocks.get(blockIndex), blockIndex * packedBytesLength, packedBytesLength);
  }

  void readPackedValue(int index, byte[] bytes) {
    int block = index / valuesPerBlock;
    int blockIndex = index % valuesPerBlock;
    System.arraycopy(blocks.get(block), blockIndex * packedBytesLength, bytes, 0, packedBytesLength);
  }

  @Override
  public boolean next() {
    curRead++;
    return curRead < end;
  }

  @Override
  public byte[] packedValue() {
    readPackedValue(curRead, scratch);
    return scratch;
  }

  @Override
  public int docID() {
    return docIDs[curRead];
  }

  @Override
  public long ord() {
    return ords[curRead];
  }

  @Override
  public void close() {
  }
}
