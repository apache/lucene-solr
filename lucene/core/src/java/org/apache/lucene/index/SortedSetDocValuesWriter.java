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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

/** Buffers up pending byte[]s per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
class SortedSetDocValuesWriter extends DocValuesWriter {
  final BytesRefHash hash;
  private PackedLongValues.Builder pending; // stream of all termIDs
  private PackedLongValues.Builder pendingCounts; // termIDs per doc
  private final Counter iwBytesUsed;
  private long bytesUsed; // this only tracks differences in 'pending' and 'pendingCounts'
  private final FieldInfo fieldInfo;
  private int currentDoc;
  private int currentValues[] = new int[8];
  private int currentUpto = 0;
  private int maxCount = 0;

  PackedLongValues finalOrds;
  PackedLongValues finalOrdCounts;
  int[] finalSortedValues;
  int[] finalOrdMap;
  int[] valueStartPtrs;

  public SortedSetDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    hash = new BytesRefHash(
        new ByteBlockPool(
            new ByteBlockPool.DirectTrackingAllocator(iwBytesUsed)),
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = PackedLongValues.packedBuilder(PackedInts.COMPACT);
    pendingCounts = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    bytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, BytesRef value) {
    if (value == null) {
      throw new IllegalArgumentException("field \"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + (BYTE_BLOCK_SIZE - 2));
    }
    
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
  
  // finalize currentDoc: this deduplicates the current term ids
  private void finishCurrentDoc() {
    Arrays.sort(currentValues, 0, currentUpto);
    int lastValue = -1;
    int count = 0;
    for (int i = 0; i < currentUpto; i++) {
      int termID = currentValues[i];
      // if it's not a duplicate
      if (termID != lastValue) {
        pending.add(termID); // record the term id
        count++;
      }
      lastValue = termID;
    }
    // record the number of unique term ids for this doc
    pendingCounts.add(count);
    maxCount = Math.max(maxCount, count);
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
    final int valueCount = hash.size();
    finalOrds = pending.build();
    finalOrdCounts = pendingCounts.build();

    finalSortedValues = hash.sort();
    finalOrdMap = new int[valueCount];
    for (int ord = 0; ord < valueCount; ord++) {
      finalOrdMap[finalSortedValues[ord]] = ord;
    }
  }

  private void addOneValue(BytesRef value) {
    int termID = hash.add(value);
    if (termID < 0) {
      termID = -termID-1;
    } else {
      // reserve additional space for each unique value:
      // 1. when indexing, when hash is 50% full, rehash() suddenly needs 2*size ints.
      //    TODO: can this same OOM happen in THPF?
      // 2. when flushing, we need 1 int per value (slot in the ordMap).
      iwBytesUsed.addAndGet(2 * Integer.BYTES);
    }
    
    if (currentUpto == currentValues.length) {
      currentValues = ArrayUtil.grow(currentValues, currentValues.length+1);
      // reserve additional space for max # values per-doc
      // when flushing, we need an int[] to sort the mapped-ords within the doc
      iwBytesUsed.addAndGet((currentValues.length - currentUpto) * 2 * Integer.BYTES);
    }
    
    currentValues[currentUpto] = termID;
    currentUpto++;
  }
  
  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.maxDoc();
    final int maxCountPerDoc = maxCount;

    final int valueCount = hash.size();
    if (valueStartPtrs == null) {
      valueStartPtrs = new int[maxDoc];
      int ptr = 0;
      int doc = 0;
      PackedLongValues.Iterator it = finalOrdCounts.iterator();
      while (it.hasNext()) {
        valueStartPtrs[doc++] = ptr;
        ptr += it.next();
      }
    }

    dvConsumer.addSortedSetField(fieldInfo,

                              // ord -> value
                              new Iterable<BytesRef>() {
                                @Override
                                public Iterator<BytesRef> iterator() {
                                  return new ValuesIterator(finalSortedValues, valueCount, hash);
                                }
                              },
                              
                              // doc -> ordCount
                              new Iterable<Number>() {
                                @Override
                                public Iterator<Number> iterator() {
                                  if (sortMap == null) {
                                    return new OrdCountIterator(maxDoc, finalOrdCounts);
                                  } else {
                                    return new SortingOrdCountIterator(maxDoc, finalOrdCounts, sortMap);
                                  }
                                }
                              },

                              // ords
                              new Iterable<Number>() {
                                @Override
                                public Iterator<Number> iterator() {
                                  if (sortMap == null) {
                                    return new OrdsIterator(finalOrdMap, maxCountPerDoc, finalOrds, finalOrdCounts);
                                  } else {
                                    return new SortingOrdsIterator(finalOrdMap, maxCountPerDoc, finalOrds, finalOrdCounts, sortMap, valueStartPtrs);
                                  }
                                }
                              });
  }

  @Override
  Sorter.DocComparator getDocComparator(int numDoc, SortField sortField) throws IOException {
    assert sortField instanceof SortedSetSortField;
    SortedSetSortField sf = (SortedSetSortField) sortField;
    valueStartPtrs = new int[numDoc];
    int ptr = 0;
    int doc = 0;
    PackedLongValues.Iterator it = finalOrdCounts.iterator();
    while (it.hasNext()) {
      valueStartPtrs[doc++] = ptr;
      ptr += it.next();
    }

    final int missingOrd;
    if (sortField.getMissingValue() == SortField.STRING_LAST) {
      missingOrd = Integer.MAX_VALUE;
    } else {
      missingOrd = Integer.MIN_VALUE;
    }
    // the ordinals per document are not sorted so we select the best ordinal per document based on the sort field selector
    // only once and we record the result in an array.
    int[] bestOrds = new int[numDoc];
    Arrays.fill(bestOrds, missingOrd);
    for (int docID = 0; docID < numDoc; docID++) {
      int count = (int) finalOrdCounts.get(docID);
      if (count == 0) {
        continue;
      }
      int start = valueStartPtrs[docID];
      switch (sf.getSelector()) {
        case MIN:
          int min = Integer.MAX_VALUE;
          for (int i = 0; i < count; i++) {
            min = Math.min(finalOrdMap[(int) finalOrds.get(start + i)], min);
          }
          bestOrds[docID] = min;
          break;

        case MAX:
          int max = 0;
          for (int i = 0; i < count; i++) {
            max = Math.max(finalOrdMap[(int) finalOrds.get(start + i)], max);
          }
          bestOrds[docID] = max;
          break;

        default:
          throw new IllegalStateException("unhandled SortedSetSortField.getSelector()=" + sf.getSelector());
      }
    }

    final int reverseMul = sortField.getReverse() ? -1 : 1;
    return new Sorter.DocComparator() {
      @Override
      public int compare(int docID1, int docID2) {
        return reverseMul * Integer.compare(bestOrds[docID1], bestOrds[docID2]);
      }
    };
  }

  // iterates over the unique values we have in ram
  private static class ValuesIterator implements Iterator<BytesRef> {
    final int sortedValues[];
    final BytesRefHash hash;
    final BytesRef scratch = new BytesRef();
    final int valueCount;
    int ordUpto;
    
    ValuesIterator(int sortedValues[], int valueCount, BytesRefHash hash) {
      this.sortedValues = sortedValues;
      this.valueCount = valueCount;
      this.hash = hash;
    }

    @Override
    public boolean hasNext() {
      return ordUpto < valueCount;
    }

    @Override
    public BytesRef next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      hash.get(sortedValues[ordUpto], scratch);
      ordUpto++;
      return scratch;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  
  // iterates over the ords for each doc we have in ram
  private static class OrdsIterator implements Iterator<Number> {
    final PackedLongValues.Iterator iter;
    final PackedLongValues.Iterator iterCounts;
    final int ordMap[];
    final long numOrds;

    long ordUpto;
    
    final int currentDoc[];
    int currentUpto;
    int currentLength;
    
    OrdsIterator(int ordMap[], int maxCount, PackedLongValues ords, PackedLongValues ordCounts) {
      this.currentDoc = new int[maxCount];
      this.ordMap = ordMap;
      this.numOrds = ords.size();
      this.iter = ords.iterator();
      this.iterCounts = ordCounts.iterator();
    }
    
    @Override
    public boolean hasNext() {
      return ordUpto < numOrds;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      while (currentUpto == currentLength) {
        // refill next doc, and sort remapped ords within the doc.
        currentUpto = 0;
        currentLength = (int) iterCounts.next();
        for (int i = 0; i < currentLength; i++) {
          currentDoc[i] = ordMap[(int) iter.next()];
        }
        Arrays.sort(currentDoc, 0, currentLength);
      }
      int ord = currentDoc[currentUpto];
      currentUpto++;
      ordUpto++;
      // TODO: make reusable Number
      return ord;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  // iterates over the ords for each doc we have in ram
  private static class SortingOrdsIterator implements Iterator<Number> {
    final PackedLongValues ords;
    final PackedLongValues ordCounts;
    final int ordMap[];
    final int starts[];
    final long numOrds;
    final Sorter.DocMap sortMap;

    long ordUpto;

    final int currentDoc[];
    int docUpto;
    int currentUpto;
    int currentLength;

    SortingOrdsIterator(int ordMap[], int maxCount, PackedLongValues ords, PackedLongValues ordCounts, Sorter.DocMap sortMap, int[] starts) {
      this.currentDoc = new int[maxCount];
      this.ordMap = ordMap;
      this.numOrds = ords.size();
      this.ords = ords;
      this.ordCounts = ordCounts;
      this.sortMap = sortMap;
      this.starts = starts;
    }

    @Override
    public boolean hasNext() {
      return ordUpto < numOrds;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      while (currentUpto == currentLength) {
        // refill next doc, and sort remapped ords within the doc.
        currentUpto = 0;
        int oldUpto = sortMap.newToOld(docUpto);
        int start = starts[oldUpto];
        currentLength = (int) ordCounts.get(oldUpto);
        for (int i = 0; i < currentLength; i++) {
          currentDoc[i] = ordMap[(int) ords.get(start+i)];
        }
        Arrays.sort(currentDoc, 0, currentLength);
        docUpto ++;
      }
      int ord = currentDoc[currentUpto];
      currentUpto++;
      ordUpto++;
      // TODO: make reusable Number
      return ord;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  
  private static class OrdCountIterator implements Iterator<Number> {
    final PackedLongValues counts;
    final PackedLongValues.Iterator iter;
    final int maxDoc;
    int docUpto;
    
    OrdCountIterator(int maxDoc, PackedLongValues ordCounts) {
      this.maxDoc = maxDoc;
      assert ordCounts.size() == maxDoc;
      this.iter = ordCounts.iterator();
      this.counts = ordCounts;
    }
    
    @Override
    public boolean hasNext() {
      return docUpto < maxDoc;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      docUpto ++;
      // TODO: make reusable Number
      return iter.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class SortingOrdCountIterator implements Iterator<Number> {
    final PackedLongValues counts;
    final Sorter.DocMap sortMap;
    final int maxDoc;
    int docUpto;

    SortingOrdCountIterator(int maxDoc, PackedLongValues ordCounts, Sorter.DocMap sortMap) {
      this.maxDoc = maxDoc;
      assert ordCounts.size() == maxDoc;
      this.counts = ordCounts;
      this.sortMap = sortMap;
    }

    @Override
    public boolean hasNext() {
      return docUpto < maxDoc;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      int oldUpto = sortMap.newToOld(docUpto);
      docUpto++;
      // TODO: make reusable number
      return counts.get(oldUpto);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
