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
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
class NumericDocValuesWriter extends DocValuesWriter {

  private final static long MISSING = 0L;

  private PackedLongValues.Builder pending;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private FixedBitSet docsWithField;
  private final FieldInfo fieldInfo;

  PackedLongValues finalValues;

  public NumericDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new FixedBitSet(64);
    bytesUsed = pending.ramBytesUsed() + docsWithFieldBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, long value) {
    if (docID < pending.size()) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }

    // Fill in any holes:
    for (int i = (int)pending.size(); i < docID; ++i) {
      pending.add(MISSING);
    }

    pending.add(value);
    docsWithField = FixedBitSet.ensureCapacity(docsWithField, docID);
    docsWithField.set(docID);
    
    updateBytesUsed();
  }
  
  private long docsWithFieldBytesUsed() {
    // size of the long[] + some overhead
    return RamUsageEstimator.sizeOf(docsWithField.getBits()) + 64;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + docsWithFieldBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void finish(int maxDoc) {
    finalValues = pending.build();
  }


  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.maxDoc();

    dvConsumer.addNumericField(fieldInfo,
                               new Iterable<Number>() {
                                 @Override
                                 public Iterator<Number> iterator() {
                                   if (sortMap == null) {
                                     return new NumericIterator(maxDoc, finalValues, docsWithField);
                                   } else {
                                     return new SortingNumericIterator(maxDoc, finalValues, docsWithField, sortMap);
                                   }
                                 }
                               });
  }

  @Override
  Sorter.DocComparator getDocComparator(int numDoc, SortField sortField) throws IOException {
    return getDocComparator(sortField, sortField.getType(),
        (docID) -> docID < docsWithField.length() ? docsWithField.get(docID) : false,
        (docID) -> finalValues.get(docID));
  }

  static Sorter.DocComparator getDocComparator(SortField sortField, SortField.Type sortType, IntPredicate docsWithField, IntToLongFunction docValueFunction) {
    final int reverseMul = sortField.getReverse() ? -1 : 1;
    switch (sortType) {
      case LONG: {
        final long missingValue = sortField.getMissingValue() != null ? (Long) sortField.getMissingValue() : 0;
        IntToLongFunction docValueOrMissing =
            (docID) -> docsWithField.test(docID) ? docValueFunction.applyAsLong(docID) : missingValue;
        return new Sorter.DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            final long value1 = docValueOrMissing.applyAsLong(docID1);
            final long value2 = docValueOrMissing.applyAsLong(docID2);
            return reverseMul * Long.compare(value1, value2);
          }
        };
      }

      case INT: {
        final int missingValue = sortField.getMissingValue() != null ? (Integer) sortField.getMissingValue() : 0;
        IntToLongFunction docValueOrMissing =
            (docID) -> docsWithField.test(docID) ? docValueFunction.applyAsLong(docID) : missingValue;

        return new Sorter.DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            final int value1 = (int) docValueOrMissing.applyAsLong(docID1);
            final int value2 = (int) docValueOrMissing.applyAsLong(docID2);
            return reverseMul * Integer.compare(value1, value2);
          }
        };
      }

      case DOUBLE: {
        final double missingValue = sortField.getMissingValue() != null ? (Double) sortField.getMissingValue() : 0;
        IntToDoubleFunction docValueOrMissing =
            (docID) -> docsWithField.test(docID) ? Double.longBitsToDouble(docValueFunction.applyAsLong(docID)) : missingValue;

        return new Sorter.DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            final double value1 = docValueOrMissing.applyAsDouble(docID1);
            final double value2 = docValueOrMissing.applyAsDouble(docID2);
            return reverseMul * Double.compare(value1, value2);
          }
        };
      }

      case FLOAT: {
        final float missingValue = sortField.getMissingValue() != null ? (Float) sortField.getMissingValue() : 0;
        IntToDoubleFunction docValueOrMissing =
            (docID) -> docsWithField.test(docID) ? Float.intBitsToFloat((int) docValueFunction.applyAsLong(docID)) : missingValue;

        return new Sorter.DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            final float value1 = (float) docValueOrMissing.applyAsDouble(docID1);
            final float value2 = (float) docValueOrMissing.applyAsDouble(docID2);
            return reverseMul * Float.compare(value1, value2);
          }
        };
      }

      default:
        throw new IllegalArgumentException("unhandled SortField.getType()=" + sortField.getType());
    }
  }

  // iterates over the values we have in ram
  private static class NumericIterator implements Iterator<Number> {
    final PackedLongValues.Iterator iter;
    final FixedBitSet docsWithField;
    final int size;
    final int maxDoc;
    int upto;

    NumericIterator(int maxDoc, PackedLongValues values, FixedBitSet docsWithFields) {
      this.maxDoc = maxDoc;
      this.iter = values.iterator();
      this.size = (int) values.size();
      this.docsWithField = docsWithFields;
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
        long v = iter.next();
        if (docsWithField.get(upto)) {
          value = v;
        } else {
          value = null;
        }
      } else {
        value = null;
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
    final FixedBitSet docsWithField;
    final Sorter.DocMap sortMap;
    final int size;
    final int maxDoc;
    int upto;

    SortingNumericIterator(int maxDoc, PackedLongValues values, FixedBitSet docsWithFields, Sorter.DocMap sortMap) {
      this.maxDoc = maxDoc;
      this.values = values;
      this.size = (int) values.size();
      this.docsWithField = docsWithFields;
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
      int old = sortMap.newToOld(upto);
      if (old < size && docsWithField.get(old)) {
        value = values.get(old);
      } else {
        value = null;
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
