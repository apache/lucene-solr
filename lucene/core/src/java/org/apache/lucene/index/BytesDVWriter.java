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

import org.apache.lucene.codecs.BinaryDocValuesConsumer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;


/** Buffers up pending byte[] per doc, then flushes when
 *  segment flushes. */
// nocommit name?
// nocommit make this a consumer in the chain?
class BytesDVWriter {

  private final BytesRefArray bytesRefArray;
  private final FieldInfo fieldInfo;
  private int addeValues = 0;
  private final BytesRef emptyBytesRef = new BytesRef();

  // -2 means not set yet; -1 means length isn't fixed;
  // -otherwise it's the fixed length seen so far:
  int fixedLength = -2;
  int maxLength;
  int totalSize;

  public BytesDVWriter(FieldInfo fieldInfo, Counter counter) {
    this.fieldInfo = fieldInfo;
    this.bytesRefArray = new BytesRefArray(counter);
    this.totalSize = 0;
  }

  public void addValue(int docID, BytesRef value) {
    if (value == null) {
      // nocommit improve message
      throw new IllegalArgumentException("null binaryValue not allowed (field=" + fieldInfo.name + ")");
    }
    mergeLength(value.length);
    
    // Fill in any holes:
    while(addeValues < docID) {
      addeValues++;
      bytesRefArray.append(emptyBytesRef);
      mergeLength(0);
    }
    addeValues++;
    bytesRefArray.append(value);
  }

  private void mergeLength(int length) {
    if (fixedLength == -2) {
      fixedLength = length;
    } else if (fixedLength != length) {
      fixedLength = -1;
    }
    maxLength = Math.max(maxLength, length);
    totalSize += length;
  }

  public void flush(FieldInfo fieldInfo, SegmentWriteState state, BinaryDocValuesConsumer consumer) throws IOException {
    final int bufferedDocCount = addeValues;
    BytesRef value = new BytesRef();
    for(int docID=0;docID<bufferedDocCount;docID++) {
      bytesRefArray.get(value, docID);
      consumer.add(value);
    }
    final int maxDoc = state.segmentInfo.getDocCount();
    value.length = 0;
    for(int docID=bufferedDocCount;docID<maxDoc;docID++) {
      consumer.add(value);
    }
    consumer.finish();
    reset();
    //System.out.println("FLUSH");
  }

  public void abort() {
    reset();
  }

  private void reset() {
    bytesRefArray.clear();
    fixedLength = -2;
    maxLength = 0;
  }
}