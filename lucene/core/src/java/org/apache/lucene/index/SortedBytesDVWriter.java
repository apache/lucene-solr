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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.BinaryDocValuesConsumer;
import org.apache.lucene.codecs.SortedDocValuesConsumer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;


/** Buffers up pending byte[] per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
// nocommit name?
// nocommit make this a consumer in the chain?
class SortedBytesDVWriter {

  // nocommit more ram efficient?
  // nocommit pass allocator that counts RAM used!
  final BytesRefHash hash = new BytesRefHash();
  private final ArrayList<Integer> pending = new ArrayList<Integer>();
  private final Counter iwBytesUsed;
  private int bytesUsed;
  private final FieldInfo fieldInfo;

  private static final BytesRef EMPTY = new BytesRef(BytesRef.EMPTY_BYTES);

  // -2 means not set yet; -1 means length isn't fixed;
  // -otherwise it's the fixed length seen so far:
  int fixedLength = -2;
  int maxLength;

  public SortedBytesDVWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
  }

  public void addValue(int docID, BytesRef value) {
    final int oldBytesUsed = bytesUsed;
    if (value == null) {
      // nocommit improve message
      throw new IllegalArgumentException("null sortedValue not allowed (field=" + fieldInfo.name + ")");
    }

    // Fill in any holes:
    while(pending.size() < docID) {
      addOneValue(EMPTY);
    }

    addOneValue(value);
    iwBytesUsed.addAndGet(bytesUsed - oldBytesUsed);
  }

  private void addOneValue(BytesRef value) {
    mergeLength(value.length);

    int ord = hash.add(value);
    if (ord < 0) {
      ord = -ord-1;
    } else {
      // nocommit this is undercounting!
      bytesUsed += value.length;
    }
    pending.add(ord);
    // estimate 25% overhead for ArrayList:
    bytesUsed += (int) (RamUsageEstimator.NUM_BYTES_OBJECT_REF * 1.25) + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.NUM_BYTES_INT;
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
    int valueCount = hash.size();

    final int maxDoc = state.segmentInfo.getDocCount();
    int emptyOrd = -1;
    if (pending.size() < maxDoc) {
      // Make sure we added EMPTY value before sorting:
      emptyOrd = hash.add(EMPTY);
      if (emptyOrd < 0) {
        emptyOrd = -emptyOrd-1;
      }
    }

    int[] sortedValues = hash.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    // nocommit must budget this into RAM consumption up front!
    int[] ordMap = new int[valueCount];

    // Write values, in sorted order:
    BytesRef scratch = new BytesRef();
    for(int ord=0;ord<valueCount;ord++) {
      consumer.addValue(hash.get(sortedValues[ord], scratch));
      ordMap[sortedValues[ord]] = ord;
    }
    final int bufferedDocCount = pending.size();

    for(int docID=0;docID<bufferedDocCount;docID++) {
      consumer.addDoc(ordMap[pending.get(docID)]);
    }
    for(int docID=bufferedDocCount;docID<maxDoc;docID++) {
      consumer.addDoc(ordMap[emptyOrd]);
    }
    reset();
  }

  public void abort() {
    reset();
  }

  private void reset() {
    pending.clear();
    pending.trimToSize();
    hash.clear();
    iwBytesUsed.addAndGet(-bytesUsed);
    bytesUsed = 0;
    fixedLength = -2;
    maxLength = 0;
  }
}
