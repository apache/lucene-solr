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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
class NormValuesWriter {

  private final static long MISSING = 0L;

  private PackedLongValues.Builder pending;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private final FieldInfo fieldInfo;

  public NormValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    bytesUsed = pending.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, long value) {
    // Fill in any holes:
    for (int i = (int)pending.size(); i < docID; ++i) {
      pending.add(MISSING);
    }

    pending.add(value);
    updateBytesUsed();
  }

  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  public void finish(int maxDoc) {
  }

  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, NormsConsumer normsConsumer) throws IOException {

    final int maxDoc = state.segmentInfo.maxDoc();
    final PackedLongValues values = pending.build();

    normsConsumer.addNormsField(fieldInfo,
                               new Iterable<Number>() {
                                 @Override
                                 public Iterator<Number> iterator() {
                                   if (sortMap == null) {
                                      return new NumericIterator(maxDoc, values);
                                   } else {
                                      return new SortingNumericIterator(maxDoc, values, sortMap);
                                   }
                                 }
                               });
  }

  // iterates over the values we have in ram
  private static class NumericIterator implements Iterator<Number> {
    final PackedLongValues.Iterator iter;
    final int size;
    final int maxDoc;
    int upto;

    NumericIterator(int maxDoc, PackedLongValues values) {
      this.maxDoc = maxDoc;
      this.iter = values.iterator();
      this.size = (int) values.size();
    }

    @Override
    public boolean hasNext() {
      return upto < maxDoc;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Long value;
      if (upto < size) {
        value = iter.next();
      } else {
        value = MISSING;
      }
      upto++;
      return value;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  // sort the values we have in ram according to the provided sort map
  private static class SortingNumericIterator implements Iterator<Number> {
    final PackedLongValues values;
    final Sorter.DocMap sortMap;
    final int size;
    final int maxDoc;
    int upto;

    SortingNumericIterator(int maxDoc, PackedLongValues values, Sorter.DocMap sortMap) {
      this.maxDoc = maxDoc;
      this.values = values;
      this.size = (int) values.size();
      this.sortMap = sortMap;
    }

    @Override
    public boolean hasNext() {
      return upto < maxDoc;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Long value;
      int oldUpto = sortMap.newToOld(upto);
      if (oldUpto < size) {
        value = values.get(oldUpto);
      } else {
        value = MISSING;
      }
      upto++;
      return value;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

