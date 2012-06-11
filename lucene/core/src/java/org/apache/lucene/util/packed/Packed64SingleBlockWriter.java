package org.apache.lucene.util.packed;

import java.io.IOException;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts.Writer;

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

/**
 * A {@link Writer} for {@link Packed64SingleBlock} readers.
 */
final class Packed64SingleBlockWriter extends Writer {

  private long pending;
  private int shift;
  private int written;

  Packed64SingleBlockWriter(DataOutput out, int valueCount,
      int bitsPerValue) throws IOException {
    super(out, valueCount, bitsPerValue);
    assert Packed64SingleBlock.isSupported(bitsPerValue) : bitsPerValue + " is not supported";
    pending = 0;
    shift = 0;
    written = 0;
  }

  @Override
  protected int getFormat() {
    return PackedInts.PACKED_SINGLE_BLOCK;
  }

  @Override
  public void add(long v) throws IOException {
    assert v <= PackedInts.maxValue(bitsPerValue) : "v=" + v
        + " maxValue=" + PackedInts.maxValue(bitsPerValue);
    assert v >= 0;

    if (shift + bitsPerValue > Long.SIZE) {
      out.writeLong(pending);
      pending = 0;
      shift = 0;
    }
    pending |= v << shift;
    shift += bitsPerValue;
    ++written;
  }

  @Override
  public void finish() throws IOException {
    while (written < valueCount) {
      add(0L); // Auto flush
    }

    if (shift > 0) {
      // add was called at least once
      out.writeLong(pending);
    }
  }

  @Override
  public String toString() {
    return "Packed64SingleBlockWriter(written " + written + "/" + valueCount + " with "
            + bitsPerValue + " bits/value)";
  }
}