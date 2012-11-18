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

import java.io.IOException;

import org.apache.lucene.codecs.SortedDocValuesConsumer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;


/** Buffers up pending byte[] per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
// nocommit name?
// nocommit make this a consumer in the chain?
class SortedBytesDVWriter {
  final BytesRefHash hash;
  private int[] pending = new int[DEFAULT_PENDING_SIZE];
  private int pendingIndex = 0;
  private final Counter iwBytesUsed;
  private final FieldInfo fieldInfo;

  private static final BytesRef EMPTY = new BytesRef(BytesRef.EMPTY_BYTES);
  private static final int DEFAULT_PENDING_SIZE = 16;

  // -2 means not set yet; -1 means length isn't fixed;
  // -otherwise it's the fixed length seen so far:
  int fixedLength = -2;
  int maxLength;

  public SortedBytesDVWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
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
    if (value == null) {
      // nocommit improve message
      throw new IllegalArgumentException("null sortedValue not allowed (field=" + fieldInfo.name + ")");
    }

    // Fill in any holes:
    while(pendingIndex < docID) {
      addOneValue(EMPTY);
    }

    addOneValue(value);
  }

  private void addOneValue(BytesRef value) {
    mergeLength(value.length);
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

  private void mergeLength(int length) {
    if (fixedLength == -2) {
      fixedLength = length;
    } else if (fixedLength != length) {
      fixedLength = -1;
    }
    maxLength = Math.max(maxLength, length);
  }

  public void flush(FieldInfo fieldInfo, SegmentWriteState state, SortedDocValuesConsumer consumer) throws IOException {

    final int maxDoc = state.segmentInfo.getDocCount();
    int emptyOrd = -1;
    if (pendingIndex < maxDoc) {
      // Make sure we added EMPTY value before sorting:
      emptyOrd = hash.add(EMPTY);
      if (emptyOrd < 0) {
        emptyOrd = -emptyOrd-1;
      }
    }

    int valueCount = hash.size();

    int[] sortedValues = hash.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    final int sortedValueRamUsage = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_INT*valueCount;
    iwBytesUsed.addAndGet(sortedValueRamUsage);
    final int[] ordMap = new int[valueCount];
    // Write values, in sorted order:
    BytesRef scratch = new BytesRef();
    for(int ord=0;ord<valueCount;ord++) {
      consumer.addValue(hash.get(sortedValues[ord], scratch));
      ordMap[sortedValues[ord]] = ord;
    }
    final int bufferedDocCount = pendingIndex;

    for(int docID=0;docID<bufferedDocCount;docID++) {
      consumer.addDoc(ordMap[pending[docID]]);
    }
    for(int docID=bufferedDocCount;docID<maxDoc;docID++) {
      consumer.addDoc(ordMap[emptyOrd]);
    }
    iwBytesUsed.addAndGet(-sortedValueRamUsage);
    reset();
  }

  public void abort() {
    reset();
  }

  private void reset() {
    iwBytesUsed.addAndGet((pending.length - DEFAULT_PENDING_SIZE) * RamUsageEstimator.NUM_BYTES_INT);
    pending = ArrayUtil.shrink(pending, DEFAULT_PENDING_SIZE);
    pendingIndex = 0;
    hash.clear();
    fixedLength = -2;
    maxLength = 0;
  }
}
