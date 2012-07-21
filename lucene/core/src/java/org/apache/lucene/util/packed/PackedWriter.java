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

import org.apache.lucene.store.DataOutput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.LongBuffer;

// Packs high order byte first, to match
// IndexOutput.writeInt/Long/Short byte order

final class PackedWriter extends PackedInts.Writer {

  boolean finished;
  final PackedInts.Format format;
  final BulkOperation bulkOperation;
  final LongBuffer nextBlocks;
  final LongBuffer nextValues;
  final int iterations;
  int written;

  PackedWriter(PackedInts.Format format, DataOutput out, int valueCount, int bitsPerValue, int mem) {
    super(out, valueCount, bitsPerValue);
    this.format = format;
    bulkOperation = BulkOperation.of(format, bitsPerValue);
    iterations = bulkOperation.computeIterations(valueCount, mem);
    nextBlocks = LongBuffer.allocate(iterations * bulkOperation.blocks());
    nextValues = LongBuffer.allocate(iterations * bulkOperation.values());
    written = 0;
    finished = false;
  }

  @Override
  protected PackedInts.Format getFormat() {
    return format;
  }

  @Override
  public void add(long v) throws IOException {
    assert v >= 0 && v <= PackedInts.maxValue(bitsPerValue);
    assert !finished;
    if (valueCount != -1 && written >= valueCount) {
      throw new EOFException("Writing past end of stream");
    }
    nextValues.put(v);
    if (nextValues.remaining() == 0) {
      flush();
    }
    ++written;
  }

  @Override
  public void finish() throws IOException {
    assert !finished;
    if (valueCount != -1) {
      while (written < valueCount) {
        add(0L);
      }
    }
    flush();
    finished = true;
  }

  private void flush() throws IOException {
    final int nvalues = nextValues.position();
    nextValues.rewind();
    nextBlocks.clear();
    bulkOperation.encode(nextValues, nextBlocks, iterations);
    final int blocks = format.nblocks(bitsPerValue, nvalues);
    nextBlocks.rewind();
    for (int i = 0; i < blocks; ++i) {
      out.writeLong(nextBlocks.get());
    }
    nextValues.clear();
  }

  @Override
  public int ord() {
    return written - 1;
  }
}
