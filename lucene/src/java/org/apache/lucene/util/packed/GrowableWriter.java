package org.apache.lucene.util.packed;

/**
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
 * Implements {@link PackedInts.Mutable}, but grows the
 * bit count of the underlying packed ints on-demand.
 *
 * <p>@lucene.internal</p>
 */

public class GrowableWriter implements PackedInts.Mutable {

  private long currentMaxValue;
  private PackedInts.Mutable current;
  private final boolean roundFixedSize;

  public GrowableWriter(int startBitsPerValue, int valueCount, boolean roundFixedSize) {
    this.roundFixedSize = roundFixedSize;
    current = PackedInts.getMutable(valueCount, getSize(startBitsPerValue));
    currentMaxValue = PackedInts.maxValue(current.getBitsPerValue());
  }

  private final int getSize(int bpv) {
    if (roundFixedSize) {
      return PackedInts.getNextFixedSize(bpv);
    } else {
      return bpv;
    }
  }

  public long get(int index) {
    return current.get(index);
  }

  public int size() {
    return current.size();
  }

  public int getBitsPerValue() {
    return current.getBitsPerValue();
  }

  public PackedInts.Mutable getMutable() {
    return current;
  }

  @Override
  public Object getArray() {
    return current.getArray();
  }

  @Override
  public boolean hasArray() {
    return current.hasArray();
  }

  public void set(int index, long value) {
    if (value >= currentMaxValue) {
      int bpv = getBitsPerValue();
      while(currentMaxValue <= value && currentMaxValue != Long.MAX_VALUE) {
        bpv++;
        currentMaxValue *= 2;
      }
      final int valueCount = size();
      PackedInts.Mutable next = PackedInts.getMutable(valueCount, getSize(bpv));
      for(int i=0;i<valueCount;i++) {
        next.set(i, current.get(i));
      }
      current = next;
      currentMaxValue = PackedInts.maxValue(current.getBitsPerValue());
    }
    current.set(index, value);
  }

  public void clear() {
    current.clear();
  }

  public GrowableWriter resize(int newSize) {
    GrowableWriter next = new GrowableWriter(getBitsPerValue(), newSize, roundFixedSize);
    final int limit = Math.min(size(), newSize);
    for(int i=0;i<limit;i++) {
      next.set(i, get(i));
    }
    return next;
  }
}
