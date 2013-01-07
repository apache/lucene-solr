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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;


/** Buffers up pending byte[] per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
// nocommit name?
// nocommit make this a consumer in the chain?
class SortedBytesDVWriter extends DocValuesWriter {
  final BytesRefHash hash;
  private int[] pending = new int[DEFAULT_PENDING_SIZE];
  private int pendingIndex = 0;
  private final Counter iwBytesUsed;
  private final FieldInfo fieldInfo;

  private static final BytesRef EMPTY = new BytesRef(BytesRef.EMPTY_BYTES);
  private static final int DEFAULT_PENDING_SIZE = 16;

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
    if (docID < pendingIndex) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
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
  public void flush(SegmentWriteState state, SimpleDVConsumer dvConsumer) throws IOException {
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
      emptyOrd = -1;
    }

    final int valueCount = hash.size();

    final int[] sortedValues = hash.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    final int sortedValueRamUsage = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_INT*valueCount;
    iwBytesUsed.addAndGet(sortedValueRamUsage);
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
                                  return new Iterator<BytesRef>() {
                                    int ordUpto;
                                    BytesRef scratch = new BytesRef();

                                    @Override
                                    public boolean hasNext() {
                                      return ordUpto < valueCount;
                                    }

                                    @Override
                                    public void remove() {
                                      throw new UnsupportedOperationException();
                                    }

                                    @Override
                                    public BytesRef next() {
                                      hash.get(sortedValues[ordUpto], scratch);
                                      ordUpto++;
                                      return scratch;
                                    }
                                  };
                                }
                              },

                              // doc -> ord
                              new Iterable<Number>() {
                                @Override
                                public Iterator<Number> iterator() {
                                  return new Iterator<Number>() {
                                    int docUpto;

                                    @Override
                                    public boolean hasNext() {
                                      return docUpto < maxDoc;
                                    }

                                    @Override
                                    public void remove() {
                                      throw new UnsupportedOperationException();
                                    }

                                    @Override
                                    public Number next() {
                                      int ord;
                                      if (docUpto < bufferedDocCount) {
                                        ord = pending[docUpto];
                                      } else {
                                        ord = emptyOrd;
                                      }
                                      docUpto++;
                                      // nocommit make
                                      // resuable Number?
                                      return ordMap[ord];
                                    }
                                  };
                                }
                              });
    
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
  }
}
