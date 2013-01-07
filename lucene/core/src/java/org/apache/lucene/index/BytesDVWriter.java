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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;


/** Buffers up pending byte[] per doc, then flushes when
 *  segment flushes. */
// nocommit name?
// nocommit make this a consumer in the chain?
class BytesDVWriter extends DocValuesWriter {

  private final BytesRefArray bytesRefArray;
  private final FieldInfo fieldInfo;
  private int addedValues = 0;
  private final BytesRef emptyBytesRef = new BytesRef();

  public BytesDVWriter(FieldInfo fieldInfo, Counter counter) {
    this.fieldInfo = fieldInfo;
    this.bytesRefArray = new BytesRefArray(counter);
  }

  public void addValue(int docID, BytesRef value) {
    if (docID < addedValues) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      // nocommit improve message
      throw new IllegalArgumentException("null binaryValue not allowed (field=" + fieldInfo.name + ")");
    }
    
    // Fill in any holes:
    while(addedValues < docID) {
      addedValues++;
      bytesRefArray.append(emptyBytesRef);
    }
    addedValues++;
    bytesRefArray.append(value);
  }

  @Override
  public void finish(int maxDoc) {
  }

  @Override
  public void flush(SegmentWriteState state, SimpleDVConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.getDocCount();

    dvConsumer.addBinaryField(fieldInfo,
                              new Iterable<BytesRef>() {

                                @Override
                                public Iterator<BytesRef> iterator() {
                                   return new Iterator<BytesRef>() {
                                     BytesRef value = new BytesRef();
                                     int upto;

                                     @Override
                                     public boolean hasNext() {
                                       return upto < maxDoc;
                                     }

                                     @Override
                                     public void remove() {
                                       throw new UnsupportedOperationException();
                                     }

                                     @Override
                                     public BytesRef next() {
                                       // nocommit make
                                       // mutable Number:
                                       if (upto < bytesRefArray.size()) {
                                         bytesRefArray.get(value, upto);
                                       } else {
                                         value.length = 0;
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

  private void reset() {
    bytesRefArray.clear();
  }
}