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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

/** Buffers up pending byte[] per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
class SortedDocValuesWriter extends DocValuesWriter {
  final BytesRefHash hash;
  private PackedLongValues.Builder pending;
  private final Counter iwBytesUsed;
  private long bytesUsed; // this currently only tracks differences in 'pending'
  private final FieldInfo fieldInfo;

  PackedLongValues finalOrds;
  int[] finalSortedValues;
  int[] finalOrdMap;

  private static final int EMPTY_ORD = -1;

  public SortedDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    hash = new BytesRefHash(
        new ByteBlockPool(
            new ByteBlockPool.DirectTrackingAllocator(iwBytesUsed)),
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    bytesUsed = pending.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, BytesRef value) {
    if (docID < pending.size()) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException("field \"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + (BYTE_BLOCK_SIZE - 2));
    }

    // Fill in any holes:
    while(pending.size() < docID) {
      pending.add(EMPTY_ORD);
    }

    addOneValue(value);
  }

  @Override
  public void finish(int maxDoc) {
    while(pending.size() < maxDoc) {
      pending.add(EMPTY_ORD);
    }
    updateBytesUsed();

    assert pending.size() == maxDoc;
    final int valueCount = hash.size();
    if (finalOrds == null) {
      finalOrds = pending.build();
      finalSortedValues = hash.sort();
      finalOrdMap = new int[valueCount];

      for (int ord = 0; ord < valueCount; ord++) {
        finalOrdMap[finalSortedValues[ord]] = ord;
      }
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
    
    pending.add(termID);
    updateBytesUsed();
  }
  
  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.maxDoc();
    final int valueCount = hash.size();

    dvConsumer.addSortedField(fieldInfo,

                              // ord -> value
                              new Iterable<BytesRef>() {
                                @Override
                                public Iterator<BytesRef> iterator() {
                                  return new ValuesIterator(finalSortedValues, valueCount, hash);
                                }
                              },

                              // doc -> ord
                              new Iterable<Number>() {
                                @Override
                                public Iterator<Number> iterator() {
                                  if (sortMap == null) {
                                    return new OrdsIterator(finalOrdMap, maxDoc, finalOrds);
                                  } else {
                                    return new SortingOrdsIterator(finalOrdMap, maxDoc, finalOrds, sortMap);
                                  }
                                }
                              });
  }

  @Override
  Sorter.DocComparator getDocComparator(int numDoc, SortField sortField) throws IOException {
    assert sortField.getType() == SortField.Type.STRING;
    final int missingOrd;
    if (sortField.getMissingValue() == SortField.STRING_LAST) {
      missingOrd = Integer.MAX_VALUE;
    } else {
      missingOrd = Integer.MIN_VALUE;
    }
    final int reverseMul = sortField.getReverse() ? -1 : 1;
    return new Sorter.DocComparator() {
      @Override
      public int compare(int docID1, int docID2) {
        int ord1 = (int) finalOrds.get(docID1);
        if (ord1 == -1) {
          ord1 = missingOrd;
        } else {
          ord1 = finalOrdMap[ord1];
        }

        int ord2 = (int) finalOrds.get(docID2);
        if (ord2 == -1) {
          ord2 = missingOrd;
        } else {
          ord2 = finalOrdMap[ord2];
        }
        return reverseMul * Integer.compare(ord1, ord2);
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
    final int ordMap[];
    final int maxDoc;
    int docUpto;

    OrdsIterator(int ordMap[], int maxDoc, PackedLongValues ords) {
      this.ordMap = ordMap;
      this.maxDoc = maxDoc;
      assert ords.size() == maxDoc;
      this.iter = ords.iterator();
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
      int ord = (int) iter.next();
      docUpto++;
      return ord == -1 ? ord : ordMap[ord];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  // sort the ords we have in ram according to the provided sort map
  private static class SortingOrdsIterator implements Iterator<Number> {
    final PackedLongValues ords;
    final int ordMap[];
    final Sorter.DocMap sortMap;
    final int maxDoc;
    int docUpto;

    SortingOrdsIterator(int ordMap[], int maxDoc, PackedLongValues ords, Sorter.DocMap sortMap) {
      this.ordMap = ordMap;
      this.maxDoc = maxDoc;
      assert ords.size() == maxDoc;
      this.ords = ords;
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
      int ord = (int) ords.get(oldUpto);
      docUpto++;
      return ord == -1 ? ord : ordMap[ord];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
