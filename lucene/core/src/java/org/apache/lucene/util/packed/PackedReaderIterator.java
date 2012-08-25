package org.apache.lucene.util.packed;

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

import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.LongsRef;

final class PackedReaderIterator extends PackedInts.ReaderIteratorImpl {

  final PackedInts.Format format;
  final BulkOperation bulkOperation;
  final long[] nextBlocks;
  final LongsRef nextValues;
  final int iterations;
  int position;

  PackedReaderIterator(PackedInts.Format format, int valueCount, int bitsPerValue, DataInput in, int mem) {
    super(valueCount, bitsPerValue, in);
    this.format = format;
    bulkOperation = BulkOperation.of(format, bitsPerValue);
    iterations = bulkOperation.computeIterations(valueCount, mem);
    assert iterations > 0;
    nextBlocks = new long[iterations * bulkOperation.blockCount()];
    nextValues = new LongsRef(new long[iterations * bulkOperation.valueCount()], 0, 0);
    assert iterations * bulkOperation.valueCount() == nextValues.longs.length;
    assert iterations * bulkOperation.blockCount() == nextBlocks.length;
    nextValues.offset = nextValues.longs.length;
    position = -1;
  }

  @Override
  public LongsRef next(int count) throws IOException {
    assert nextValues.length >= 0;
    assert count > 0;
    assert nextValues.offset + nextValues.length <= nextValues.longs.length;
    
    nextValues.offset += nextValues.length;

    final int remaining = valueCount - position - 1;
    if (remaining <= 0) {
      throw new EOFException();
    }
    count = Math.min(remaining, count);

    if (nextValues.offset == nextValues.longs.length) {
      final int remainingBlocks = format.nblocks(bitsPerValue, remaining);
      final int blocksToRead = Math.min(remainingBlocks, nextBlocks.length);
      for (int i = 0; i < blocksToRead; ++i) {
        nextBlocks[i] = in.readLong();
      }
      for (int i = blocksToRead; i < nextBlocks.length; ++i) {
        nextBlocks[i] = 0L;
      }

      bulkOperation.decode(nextBlocks, 0, nextValues.longs, 0, iterations);
      nextValues.offset = 0;
    }

    nextValues.length = Math.min(nextValues.longs.length - nextValues.offset, count);
    position += nextValues.length;
    return nextValues;
  }

  @Override
  public int ord() {
    return position;
  }

}
