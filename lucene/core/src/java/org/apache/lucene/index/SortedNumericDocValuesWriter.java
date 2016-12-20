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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.IntToLongFunction;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** Buffers up pending long[] per doc, sorts, then flushes when segment flushes. */
class SortedNumericDocValuesWriter extends DocValuesWriter {
  private PackedLongValues.Builder pending; // stream of all values
  private PackedLongValues.Builder pendingCounts; // count of values per doc
  private final Counter iwBytesUsed;
  private long bytesUsed; // this only tracks differences in 'pending' and 'pendingCounts'
  private final FieldInfo fieldInfo;
  private int currentDoc;
  private long currentValues[] = new long[8];
  private int currentUpto = 0;
  private int maxCount = 0;

  PackedLongValues finalValues;
  PackedLongValues finalValueCounts;
  int[] valueStartPtrs;

  public SortedNumericDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    pendingCounts = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    bytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, long value) {    
    if (docID != currentDoc) {
      finishCurrentDoc();
    }

    // Fill in any holes:
    while(currentDoc < docID) {
      pendingCounts.add(0); // no values
      currentDoc++;
    }

    addOneValue(value);
    updateBytesUsed();
  }
  
  // finalize currentDoc: this sorts the values in the current doc
  private void finishCurrentDoc() {
    Arrays.sort(currentValues, 0, currentUpto);
    for (int i = 0; i < currentUpto; i++) {
      pending.add(currentValues[i]);
    }
    // record the number of values for this doc
    pendingCounts.add(currentUpto);
    maxCount = Math.max(maxCount, currentUpto);
    currentUpto = 0;
    currentDoc++;
  }

  @Override
  public void finish(int maxDoc) {
    finishCurrentDoc();
    
    // fill in any holes
    for (int i = currentDoc; i < maxDoc; i++) {
      pendingCounts.add(0); // no values
    }

    assert pendingCounts.size() == maxDoc;
    finalValues = pending.build();
    finalValueCounts = pendingCounts.build();
  }

  @Override
  Sorter.DocComparator getDocComparator(int numDoc, SortField sortField) throws IOException {
    SortedNumericSortField sf = (SortedNumericSortField) sortField;
    valueStartPtrs = new int[numDoc];
    int ptr = 0;
    int doc = 0;
    PackedLongValues.Iterator it = finalValueCounts.iterator();
    while (it.hasNext()) {
      valueStartPtrs[doc++] = ptr;
      ptr += it.next();
    }

    final IntToLongFunction function = (docID) -> {
      int count = (int) finalValueCounts.get(docID);
      assert count > 0;
      int start = valueStartPtrs[docID];
      switch (sf.getSelector()) {
        case MIN:
          return finalValues.get(start);

        case MAX:
          return finalValues.get(start + count - 1);

        default:
          throw new IllegalStateException("Should never happen");
      }
    };
    return NumericDocValuesWriter.getDocComparator(sf, sf.getNumericType(), (docID) -> finalValueCounts.get(docID) > 0, function);
  }

  private void addOneValue(long value) {
    if (currentUpto == currentValues.length) {
      currentValues = ArrayUtil.grow(currentValues, currentValues.length+1);
    }
    
    currentValues[currentUpto] = value;
    currentUpto++;
  }
  
  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed() + RamUsageEstimator.sizeOf(currentValues);
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.maxDoc();

    if (sortMap != null) {
      valueStartPtrs = new int[maxDoc];
      int ptr = 0;
      int doc = 0;
      PackedLongValues.Iterator it = finalValueCounts.iterator();
      while (it.hasNext()) {
        valueStartPtrs[doc++] = ptr;
        ptr += it.next();
      }
    }

    dvConsumer.addSortedNumericField(fieldInfo,
                              // doc -> valueCount
                              new Iterable<Number>() {
                                @Override
                                public Iterator<Number> iterator() {
                                  if (sortMap == null) {
                                    return new CountIterator(finalValueCounts);
                                  } else {
                                    return new SortingCountIterator(finalValueCounts, sortMap);
                                  }
                                }
                              },
                              // values
                              new Iterable<Number>() {
                                @Override
                                public Iterator<Number> iterator() {
                                  if (sortMap == null) {
                                    return new ValuesIterator(finalValues);
                                  } else {
                                    return new SortingValuesIterator(finalValues, finalValueCounts, maxCount, sortMap, valueStartPtrs);
                                  }
                                }
                              });
  }
  
  // iterates over the values for each doc we have in ram
  private static class ValuesIterator implements Iterator<Number> {
    final PackedLongValues.Iterator iter;

    ValuesIterator(PackedLongValues values) {
      iter = values.iterator();
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return iter.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  // sort the values we have in ram according to the provided sort map
  private static class SortingValuesIterator implements Iterator<Number> {
    final PackedLongValues values;
    final PackedLongValues counts;
    final Sorter.DocMap sortMap;
    final int[] startPtrs;

    final long numValues;
    long valueUpto;
    final long currentDoc[];
    int currentDocSize;
    int docUpto;
    int currentLength;

    private SortingValuesIterator(PackedLongValues values, PackedLongValues counts, int maxCount, Sorter.DocMap sortMap,
                                  int[] startPtrs) {
      this.values = values;
      this.numValues = values.size();
      this.counts = counts;
      this.sortMap = sortMap;
      this.startPtrs = startPtrs;
      this.currentDoc = new long[maxCount];
    }

    @Override
    public boolean hasNext() {
      return valueUpto < numValues;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      while (currentDocSize == currentLength) {
        currentDocSize = 0;
        int oldUpto = sortMap.newToOld(docUpto);
        currentLength = (int) counts.get(oldUpto);
        int start = startPtrs[oldUpto];
        for (int i = 0; i < currentLength; i++) {
          currentDoc[i] = values.get(start+i);
        }
        docUpto++;
      }
      long value = currentDoc[currentDocSize];
      currentDocSize++;
      valueUpto++;
      // TODO: make reusable Number
      return value;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class CountIterator implements Iterator<Number> {
    final PackedLongValues.Iterator iter;

    CountIterator(PackedLongValues valueCounts) {
      this.iter = valueCounts.iterator();
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return iter.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class SortingCountIterator implements Iterator<Number> {
    final PackedLongValues counts;
    final Sorter.DocMap sortMap;
    final int size;
    int currentUpto;

    SortingCountIterator(PackedLongValues valueCounts, Sorter.DocMap sortMap) {
      this.counts = valueCounts;
      this.sortMap = sortMap;
      this.size = (int) valueCounts.size();
    }

    @Override
    public boolean hasNext() {
      return currentUpto < size;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      int oldUpto = sortMap.newToOld(currentUpto++);
      return counts.get(oldUpto);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
