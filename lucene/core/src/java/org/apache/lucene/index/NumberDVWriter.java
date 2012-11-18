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

import org.apache.lucene.codecs.NumericDocValuesConsumer;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.AppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;

// nocommit pick numeric or number ... then fix all places ...

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
// nocommit name?
// nocommit make this a consumer in the chain?
class NumberDVWriter {

  private final static long MISSING = 0L;

  private AppendingLongBuffer pending;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private final FieldInfo fieldInfo;

  long minValue;
  long maxValue;
  private boolean anyValues;

  public NumberDVWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    pending = new AppendingLongBuffer();
    bytesUsed = pending.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
  }

  public void addValue(int docID, long value) {
    mergeValue(value);

    // Fill in any holes:
    for (int i = pending.size(); i < docID; ++i) {
      pending.add(MISSING);
      mergeValue(0);
    }

    pending.add(value);

    updateBytesUsed();
  }

  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
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

    AppendingLongBuffer.Iterator it = pending.iterator();
    for(int docID=0;docID<bufferedDocCount;docID++) {
      assert it.hasNext();
      long v = it.next();
      consumer.add(v);
    }
    assert !it.hasNext();
    final int maxDoc = state.segmentInfo.getDocCount();
    for(int docID=bufferedDocCount;docID<maxDoc;docID++) {
      consumer.add(0);
    }
    consumer.finish();
    reset();
    //System.out.println("FLUSH");
  }

  public void abort() {
    reset();
  }

  // nocommit do we really need this...?  can't parent alloc
  // a new instance after flush?
  private void reset() {
    pending = new AppendingLongBuffer();
    updateBytesUsed();
    anyValues = false;
    minValue = maxValue = 0;
  }

}