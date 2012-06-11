package org.apache.lucene.util.packed;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts.ReaderIteratorImpl;

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

final class Packed64SingleBlockReaderIterator extends ReaderIteratorImpl {

  private long pending;
  private int shift;
  private final long mask;
  private int position;

  Packed64SingleBlockReaderIterator(int valueCount, int bitsPerValue, IndexInput in)
      throws IOException {
    super(valueCount, bitsPerValue, in);
    pending = 0;
    shift = 64;
    mask = ~(~0L << bitsPerValue);
    position = -1;
  }

  @Override
  public long next() throws IOException {
    if (shift + bitsPerValue > 64) {
      pending = in.readLong();
      shift = 0;
    }
    final long next = (pending >>> shift) & mask;
    shift += bitsPerValue;
    ++position;
    return next;
  }

  @Override
  public int ord() {
    return position;
  }

  @Override
  public long advance(int ord) throws IOException {
    assert ord < valueCount : "ord must be less than valueCount";
    assert ord > position : "ord must be greater than the current position";

    final int valuesPerBlock = 64 / bitsPerValue;
    final long nextBlock = (position + valuesPerBlock) / valuesPerBlock;
    final long targetBlock = ord / valuesPerBlock;
    final long blocksToSkip = targetBlock - nextBlock;
    if (blocksToSkip > 0) {
      final long skip = blocksToSkip << 3;
      final long filePointer = in.getFilePointer();

      in.seek(filePointer + skip);
      shift = 64;

      final int offsetInBlock = ord % valuesPerBlock;
      for (int i = 0; i < offsetInBlock; ++i) {
        next();
      }
    } else {
      for (int i = position; i < ord - 1; ++i) {
        next();
      }
    }

    position = ord - 1;
    return next();
  }

}
