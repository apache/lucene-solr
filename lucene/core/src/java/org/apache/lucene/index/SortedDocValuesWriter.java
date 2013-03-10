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
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.AppendingLongBuffer;

/** Buffers up pending byte[] per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
class SortedDocValuesWriter extends DocValuesWriter {
  final BytesRefHash hash;
  private AppendingLongBuffer pending;
  private final Counter iwBytesUsed;
  private long bytesUsed; // this currently only tracks differences in 'pending'
  private final FieldInfo fieldInfo;

  private static final BytesRef EMPTY = new BytesRef(BytesRef.EMPTY_BYTES);

  public SortedDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    hash = new BytesRefHash(
        new ByteBlockPool(
            new ByteBlockPool.DirectTrackingAllocator(iwBytesUsed)),
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = new AppendingLongBuffer();
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
      addOneValue(EMPTY);
    }

    addOneValue(value);
  }

  @Override
  public void finish(int maxDoc) {
    while(pending.size() < maxDoc) {
      addOneValue(EMPTY);
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
      iwBytesUsed.addAndGet(2 * RamUsageEstimator.NUM_BYTES_INT);
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
  public void flush(SegmentWriteState state, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.getDocCount();

    assert pending.size() == maxDoc;
    final int valueCount = hash.size();

    final int[] sortedValues = hash.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    final int[] ordMap = new int[valueCount];

    for(int ord=0;ord<valueCount;ord++) {
      ordMap[sortedValues[ord]] = ord;
    }

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
                                  return new OrdsIterator(ordMap, maxDoc);
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
    final AppendingLongBuffer.Iterator iter = pending.iterator();
    final int ordMap[];
    final int maxDoc;
    int docUpto;
    
    OrdsIterator(int ordMap[], int maxDoc) {
      this.ordMap = ordMap;
      this.maxDoc = maxDoc;
      assert pending.size() == maxDoc;
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
      // TODO: make reusable Number
      return ordMap[ord];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
