package org.apache.lucene.index;

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

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;


/** Buffers up pending byte[] per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
class SortedDocValuesWriter extends DocValuesWriter {
  final BytesRefHash hash;
  private int[] pending = new int[DEFAULT_PENDING_SIZE];
  private int pendingIndex = 0;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private final FieldInfo fieldInfo;

  private static final BytesRef EMPTY = new BytesRef(BytesRef.EMPTY_BYTES);
  private static final int DEFAULT_PENDING_SIZE = 16;

  public SortedDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    hash = new BytesRefHash(
        new ByteBlockPool(
            new ByteBlockPool.DirectTrackingAllocator(iwBytesUsed)),
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    iwBytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_INT * DEFAULT_PENDING_SIZE);
  }

  public void addValue(int docID, BytesRef value) {
    if (docID < pendingIndex) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException("field \"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + (BYTE_BLOCK_SIZE - 2));
    }

    // Fill in any holes:
    while(pendingIndex < docID) {
      addOneValue(EMPTY);
    }

    addOneValue(value);
  }

  @Override
  public void finish(int maxDoc) {
    if (pendingIndex < maxDoc) {
      addOneValue(EMPTY);
    }
  }

  private void addOneValue(BytesRef value) {
    int ord = hash.add(value);
    if (ord < 0) {
      ord = -ord-1;
    }
    
    if (pendingIndex <= pending.length) {
      int pendingLen = pending.length;
      pending = ArrayUtil.grow(pending, pendingIndex+1);
      iwBytesUsed.addAndGet((pending.length - pendingLen) * RamUsageEstimator.NUM_BYTES_INT);
    }
    pending[pendingIndex++] = ord;
  }

  @Override
  public void flush(SegmentWriteState state, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.getDocCount();

    final int emptyOrd;
    if (pendingIndex < maxDoc) {
      // Make sure we added EMPTY value before sorting:
      int ord = hash.add(EMPTY);
      if (ord < 0) {
        emptyOrd = -ord-1;
      } else {
        emptyOrd = ord;
      }
    } else {
      emptyOrd = -1; // nocommit: HUH? how can this possibly work?
    }

    final int valueCount = hash.size();

    // nocommit: account for both sortedValues and ordMap as-we-go...
    final int[] sortedValues = hash.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    final int sortedValueRamUsage = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_INT*valueCount;
    final int[] ordMap = new int[valueCount];

    for(int ord=0;ord<valueCount;ord++) {
      ordMap[sortedValues[ord]] = ord;
    }

    final int bufferedDocCount = pendingIndex;

    dvConsumer.addSortedField(fieldInfo,

                              // ord -> value
                              new Iterable<BytesRef>() {
                                @Override
                                public Iterator<BytesRef> iterator() {
                                  return new ValuesIterator(sortedValues, valueCount);
                                }
                              },

                              // doc -> ord
                              new Iterable<Number>() {
                                @Override
                                public Iterator<Number> iterator() {
                                  return new OrdsIterator(ordMap, bufferedDocCount, maxDoc, emptyOrd);
                                }
                              });
  }

  @Override
  public void abort() {
  }
  
  // iterates over the unique values we have in ram
  private class ValuesIterator implements Iterator<BytesRef> {
    final int sortedValues[];
    final BytesRef scratch = new BytesRef();
    final int valueCount;
    int ordUpto;
    
    ValuesIterator(int sortedValues[], int valueCount) {
      this.sortedValues = sortedValues;
      this.valueCount = valueCount;
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
  private class OrdsIterator implements Iterator<Number> {
    final int ordMap[];
    final int size;
    final int maxDoc;
    final int emptyOrd; // nocommit
    int docUpto;
    
    OrdsIterator(int ordMap[], int size, int maxDoc, int emptyOrd) {
      this.ordMap = ordMap;
      this.size = size;
      this.maxDoc = maxDoc;
      this.emptyOrd = emptyOrd;
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
      int ord;
      if (docUpto < size) {
        ord = pending[docUpto];
      } else {
        ord = emptyOrd;
      }
      docUpto++;
      // TODO: make reusable Number
      return ordMap[ord];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
