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
import java.util.Arrays;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Buffers up pending long[] per doc, sorts, then flushes when segment flushes. */
class SortedNumericDocValuesWriter extends DocValuesWriter<SortedNumericDocValues> {
  private PackedLongValues.Builder pending; // stream of all values
  private PackedLongValues.Builder pendingCounts; // count of values per doc
  private DocsWithFieldSet docsWithField;
  private final Counter iwBytesUsed;
  private long bytesUsed; // this only tracks differences in 'pending' and 'pendingCounts'
  private final FieldInfo fieldInfo;
  private int currentDoc = -1;
  private long[] currentValues = new long[8];
  private int currentUpto = 0;

  private PackedLongValues finalValues;
  private PackedLongValues finalValuesCount;

  SortedNumericDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    pendingCounts = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed() + docsWithField.ramBytesUsed() + RamUsageEstimator.sizeOf(currentValues);
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, long value) {
    assert docID >= currentDoc;
    if (docID != currentDoc) {
      finishCurrentDoc();
      currentDoc = docID;
    }

    addOneValue(value);
    updateBytesUsed();
  }
  
  // finalize currentDoc: this sorts the values in the current doc
  private void finishCurrentDoc() {
    if (currentDoc == -1) {
      return;
    }
    Arrays.sort(currentValues, 0, currentUpto);
    for (int i = 0; i < currentUpto; i++) {
      pending.add(currentValues[i]);
    }
    // record the number of values for this doc
    pendingCounts.add(currentUpto);
    currentUpto = 0;

    docsWithField.add(currentDoc);
  }

  private void addOneValue(long value) {
    if (currentUpto == currentValues.length) {
      currentValues = ArrayUtil.grow(currentValues, currentValues.length+1);
    }
    
    currentValues[currentUpto] = value;
    currentUpto++;
  }
  
  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed() + docsWithField.ramBytesUsed() + RamUsageEstimator.sizeOf(currentValues);
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  SortedNumericDocValues getDocValues() {
    if (finalValues == null) {
      assert finalValuesCount == null;
      finishCurrentDoc();
      finalValues = pending.build();
      finalValuesCount = pendingCounts.build();
    }
    return new BufferedSortedNumericDocValues(finalValues, finalValuesCount, docsWithField.iterator());
  }

  static final class LongValues {
    final long[] offsets;
    final PackedLongValues values;
    LongValues(int maxDoc, Sorter.DocMap sortMap, SortedNumericDocValues oldValues, float acceptableOverheadRatio) throws IOException {
      offsets = new long[maxDoc];
      PackedLongValues.Builder valuesBuiler = PackedLongValues.packedBuilder(acceptableOverheadRatio);
      int docID;
      long offsetIndex = 1; // 0 means the doc has no values
      while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
        int newDocID = sortMap.oldToNew(docID);
        int numValues = oldValues.docValueCount();
        valuesBuiler.add(numValues);
        offsets[newDocID] = offsetIndex++;
        for (int i = 0; i < numValues; i++) {
          valuesBuiler.add(oldValues.nextValue());
          offsetIndex++;
        }
      }
      values = valuesBuiler.build();
    }


  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final PackedLongValues values;
    final PackedLongValues valueCounts;
    if (finalValues == null) {
      finishCurrentDoc();
      values = pending.build();
      valueCounts = pendingCounts.build();
    } else {
      values = finalValues;
      valueCounts = finalValuesCount;
    }

    final LongValues sorted;
    if (sortMap != null) {
      sorted = new LongValues(state.segmentInfo.maxDoc(), sortMap,
          new BufferedSortedNumericDocValues(values, valueCounts, docsWithField.iterator()), PackedInts.FASTEST);
    } else {
      sorted = null;
    }

    dvConsumer.addSortedNumericField(fieldInfo,
                                     new EmptyDocValuesProducer() {
                                       @Override
                                       public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfoIn) {
                                         if (fieldInfoIn != fieldInfo) {
                                           throw new IllegalArgumentException("wrong fieldInfo");
                                         }
                                         final SortedNumericDocValues buf =
                                             new BufferedSortedNumericDocValues(values, valueCounts, docsWithField.iterator());
                                         if (sorted == null) {
                                           return buf;
                                         } else {
                                           return new SortingSortedNumericDocValues(buf, sorted);
                                         }
                                       }
                                     });
  }

  private static class BufferedSortedNumericDocValues extends SortedNumericDocValues {
    final PackedLongValues.Iterator valuesIter;
    final PackedLongValues.Iterator valueCountsIter;
    final DocIdSetIterator docsWithField;
    private int valueCount;
    private int valueUpto;

    BufferedSortedNumericDocValues(PackedLongValues values, PackedLongValues valueCounts, DocIdSetIterator docsWithField) {
      valuesIter = values.iterator();
      valueCountsIter = valueCounts.iterator();
      this.docsWithField = docsWithField;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      for (int i = valueUpto; i < valueCount; ++i) {
        valuesIter.next();
      }

      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        valueCount = Math.toIntExact(valueCountsIter.next());
        valueUpto = 0;
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docValueCount() {
      return valueCount;
    }

    @Override
    public long nextValue() {
      if (valueUpto == valueCount) {
        throw new IllegalStateException();
      }
      valueUpto++;
      return valuesIter.next();
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }
  }

  static class SortingSortedNumericDocValues extends SortedNumericDocValues {
    private final SortedNumericDocValues in;
    private final LongValues values;
    private int docID = -1;
    private long upto;
    private int numValues = - 1;
    private long limit;

    SortingSortedNumericDocValues(SortedNumericDocValues in, LongValues values) {
      this.in = in;
      this.values = values;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      do {
        docID++;
        if (docID >= values.offsets.length) {
          return docID = NO_MORE_DOCS;
        }
      } while (values.offsets[docID] <= 0);
      upto = values.offsets[docID];
      numValues = Math.toIntExact(values.values.get(upto-1));
      limit = upto + numValues;
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException("use nextDoc instead");
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      docID = target;
      upto = values.offsets[docID];
      if (values.offsets[docID] > 0) {
        numValues = Math.toIntExact(values.values.get(upto-1));
        limit = upto + numValues;
        return true;
      } else {
        limit = upto;
      }
      return false;
    }

    @Override
    public long nextValue() {
      if (upto == limit) {
        throw new AssertionError();
      } else {
        return values.values.get(upto++);
      }
    }

    @Override
    public long cost() {
      return in.cost();
    }

    @Override
    public int docValueCount() {
      return numValues;
    }
  }
}
