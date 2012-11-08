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

import org.apache.lucene.codecs.NumericDocValuesConsumer;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;

// nocommit pick numeric or number ... then fix all places ...

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
// nocommit name?
// nocommit make this a consumer in the chain?
class NumberDVWriter {

  private final static Long MISSING = new Long(0);

  // nocommit more ram efficient?
  private final ArrayList<Long> pending = new ArrayList<Long>();
  private final Counter iwBytesUsed;
  private int bytesUsed;
  private final FieldInfo fieldInfo;

  long minValue;
  long maxValue;
  private boolean anyValues;

  public NumberDVWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
  }

  public void addValue(int docID, long value) {
    final int oldBytesUsed = bytesUsed;
    mergeValue(value);

    // Fill in any holes:
    while(pending.size() < docID) {
      pending.add(MISSING);
      bytesUsed += RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      mergeValue(0);
    }

    pending.add(value);

    // estimate 25% overhead for ArrayList:
    bytesUsed += (int) (RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.NUM_BYTES_LONG + (RamUsageEstimator.NUM_BYTES_OBJECT_REF * 1.25));
    iwBytesUsed.addAndGet(bytesUsed - oldBytesUsed);
    //System.out.println("ADD: " + value);
  }

  private void mergeValue(long value) {
    if (!anyValues) {
      anyValues = true;
      minValue = maxValue = value;
    } else {
      maxValue = Math.max(value, maxValue);
      minValue = Math.min(value, minValue);
    }
  }

  public void flush(FieldInfo fieldInfo, SegmentWriteState state, NumericDocValuesConsumer consumer) throws IOException {
    final int bufferedDocCount = pending.size();

    for(int docID=0;docID<bufferedDocCount;docID++) {
      consumer.add(pending.get(docID));
    }
    final int maxDoc = state.segmentInfo.getDocCount();
    for(int docID=bufferedDocCount;docID<maxDoc;docID++) {
      consumer.add(0);
    }
    reset();
    //System.out.println("FLUSH");
  }

  public void abort() {
    reset();
  }

  // nocommit do we really need this...?  can't parent alloc
  // a new instance after flush?
  private void reset() {
    pending.clear();
    pending.trimToSize();
    iwBytesUsed.addAndGet(-bytesUsed);
    anyValues = false;
    minValue = maxValue = 0;
    bytesUsed = 0;
  }
}