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
import java.util.Iterator;

import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.AppendingLongBuffer;

// nocommit pick numeric or number ... then fix all places ...

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
// nocommit rename to NumericDVWriter?
// nocommit make this a consumer in the chain?
class NumberDVWriter extends DocValuesWriter {

  private final static long MISSING = 0L;

  private AppendingLongBuffer pending;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private final FieldInfo fieldInfo;

  public NumberDVWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    pending = new AppendingLongBuffer();
    bytesUsed = pending.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
  }

  public void addValue(int docID, long value) {
    if (docID < pending.size()) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }

    // Fill in any holes:
    for (int i = pending.size(); i < docID; ++i) {
      pending.add(MISSING);
    }

    pending.add(value);

    updateBytesUsed();
  }

  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void finish(int maxDoc) {
  }

  @Override
  public void flush(SegmentWriteState state, SimpleDVConsumer dvConsumer) throws IOException {

    final int maxDoc = state.segmentInfo.getDocCount();

    dvConsumer.addNumericField(fieldInfo,
                               new Iterable<Number>() {

                                 @Override
                                 public Iterator<Number> iterator() {
                                   return new Iterator<Number>() {
                                     int upto;
                                     AppendingLongBuffer.Iterator iter = pending.iterator();

                                     @Override
                                     public boolean hasNext() {
                                       return upto < maxDoc;
                                     }

                                     @Override
                                     public void remove() {
                                       throw new UnsupportedOperationException();
                                     }

                                     @Override
                                     public Number next() {
                                       // nocommit make
                                       // mutable Number:
                                       long value;
                                       if (upto < pending.size()) {
                                         value =  iter.next();
                                       } else {
                                         value = 0;
                                       }
                                       upto++;
                                       return value;
                                     }
                                   };
                                 }
                               });

    reset();
  }

  public void abort() {
    reset();
  }

  // nocommit do we really need this...?  can't/doesn't parent alloc
  // a new instance after flush?
  void reset() {
    pending = new AppendingLongBuffer();
    updateBytesUsed();
  }
}