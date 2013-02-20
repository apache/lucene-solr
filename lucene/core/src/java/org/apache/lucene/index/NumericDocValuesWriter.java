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
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.AppendingLongBuffer;

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
class NumericDocValuesWriter extends DocValuesWriter {

  private final static long MISSING = 0L;

  private AppendingLongBuffer pending;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private final FieldInfo fieldInfo;

  public NumericDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    pending = new AppendingLongBuffer();
    bytesUsed = pending.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, long value) {
    if (docID < pending.size()) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }

    // Fill in any holes:
    for (int i = (int)pending.size(); i < docID; ++i) {
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
  public void flush(SegmentWriteState state, DocValuesConsumer dvConsumer) throws IOException {

    final int maxDoc = state.segmentInfo.getDocCount();

    dvConsumer.addNumericField(fieldInfo,
                               new Iterable<Number>() {
                                 @Override
                                 public Iterator<Number> iterator() {
                                   return new NumericIterator(maxDoc);
                                 }
                               });
  }

  @Override
  public void abort() {
  }
  
  // iterates over the values we have in ram
  private class NumericIterator implements Iterator<Number> {
    final AppendingLongBuffer.Iterator iter = pending.iterator();
    final int size = (int)pending.size();
    final int maxDoc;
    int upto;
    
    NumericIterator(int maxDoc) {
      this.maxDoc = maxDoc;
    }
    
    @Override
    public boolean hasNext() {
      return upto < maxDoc;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      long value;
      if (upto < size) {
        value = iter.next();
      } else {
        value = 0;
      }
      upto++;
      // TODO: make reusable Number
      return value;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
