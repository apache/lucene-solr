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
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

/** Buffers up pending byte[] per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
class SortedDocValuesWriter extends DocValuesWriter {
  final BytesRefHash hash;
  private PackedLongValues.Builder pending;
  private DocsWithFieldSet docsWithField;
  private final Counter iwBytesUsed;
  private long bytesUsed; // this currently only tracks differences in 'pending'
  private final FieldInfo fieldInfo;
  private int lastDocID = -1;

  private PackedLongValues finalOrds;
  private int[] finalSortedValues;
  private int[] finalOrdMap;

  public SortedDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    hash = new BytesRefHash(
        new ByteBlockPool(
            new ByteBlockPool.DirectTrackingAllocator(iwBytesUsed)),
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, BytesRef value) {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException("field \"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + (BYTE_BLOCK_SIZE - 2));
    }

    addOneValue(value);
    docsWithField.add(docID);

    lastDocID = docID;
  }

  @Override
  public void finish(int maxDoc) {
    updateBytesUsed();
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
    final long newBytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  Sorter.DocComparator getDocComparator(int maxDoc, SortField sortField) throws IOException {
    assert sortField.getType().equals(SortField.Type.STRING);
    assert finalSortedValues == null && finalOrdMap == null &&finalOrds == null;
    int valueCount = hash.size();
    finalSortedValues = hash.sort();
    finalOrds = pending.build();
    finalOrdMap = new int[valueCount];
    for (int ord = 0; ord < valueCount; ord++) {
      finalOrdMap[finalSortedValues[ord]] = ord;
    }
    final SortedDocValues docValues =
        new BufferedSortedDocValues(hash, valueCount, finalOrds, finalSortedValues, finalOrdMap,
            docsWithField.iterator());
    return Sorter.getDocComparator(maxDoc, sortField, () -> docValues, () -> null);
  }

  private int[] sortDocValues(int maxDoc, Sorter.DocMap sortMap, SortedDocValues oldValues) throws IOException {
    int[] ords = new int[maxDoc];
    Arrays.fill(ords, -1);
    int docID;
    while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
      int newDocID = sortMap.oldToNew(docID);
      ords[newDocID] = oldValues.ordValue();
    }
    return ords;
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final int valueCount = hash.size();
    final PackedLongValues ords;
    final int[] sortedValues;
    final int[] ordMap;
    if (finalOrds == null) {
      sortedValues = hash.sort();
      ords = pending.build();
      ordMap = new int[valueCount];
      for (int ord = 0; ord < valueCount; ord++) {
        ordMap[sortedValues[ord]] = ord;
      }
    } else {
      sortedValues = finalSortedValues;
      ords = finalOrds;
      ordMap = finalOrdMap;
    }

    final int[] sorted;
    if (sortMap != null) {
      sorted = sortDocValues(state.segmentInfo.maxDoc(), sortMap,
          new BufferedSortedDocValues(hash, valueCount, ords, sortedValues, ordMap, docsWithField.iterator()));
    } else {
      sorted = null;
    }
    dvConsumer.addSortedField(fieldInfo,
                              new EmptyDocValuesProducer() {
                                @Override
                                public SortedDocValues getSorted(FieldInfo fieldInfoIn) {
                                  if (fieldInfoIn != fieldInfo) {
                                    throw new IllegalArgumentException("wrong fieldInfo");
                                  }
                                  final SortedDocValues buf =
                                      new BufferedSortedDocValues(hash, valueCount, ords, sortedValues, ordMap, docsWithField.iterator());
                                  if (sorted == null) {
                                   return buf;
                                  }
                                  return new SortingLeafReader.SortingSortedDocValues(buf, sorted);
                                }
                              });
  }

  private static class BufferedSortedDocValues extends SortedDocValues {
    final BytesRefHash hash;
    final BytesRef scratch = new BytesRef();
    final int[] sortedValues;
    final int[] ordMap;
    final int valueCount;
    private int ord;
    final PackedLongValues.Iterator iter;
    final DocIdSetIterator docsWithField;

    public BufferedSortedDocValues(BytesRefHash hash, int valueCount, PackedLongValues docToOrd, int[] sortedValues, int[] ordMap, DocIdSetIterator docsWithField) {
      this.hash = hash;
      this.valueCount = valueCount;
      this.sortedValues = sortedValues;
      this.iter = docToOrd.iterator();
      this.ordMap = ordMap;
      this.docsWithField = docsWithField;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        ord = Math.toIntExact(iter.next());
        ord = ordMap[ord];
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
    public long cost() {
      return docsWithField.cost();
    }

    @Override
    public int ordValue() {
      return ord;
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      assert ord >= 0 && ord < sortedValues.length;
      assert sortedValues[ord] >= 0 && sortedValues[ord] < sortedValues.length;
      hash.get(sortedValues[ord], scratch);
      return scratch;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }
  }
}
