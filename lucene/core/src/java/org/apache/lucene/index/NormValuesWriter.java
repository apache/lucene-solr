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
package org.apache.lucene.index;


import java.io.IOException;

import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
class NormValuesWriter {

  private final static long MISSING = 0L;

  private PackedLongValues.Builder pending;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private final FieldInfo fieldInfo;

  public NormValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    bytesUsed = pending.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, long value) {
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

  public void finish(int maxDoc) {
  }

  public void flush(SegmentWriteState state, NormsConsumer normsConsumer) throws IOException {

    final int maxDoc = state.segmentInfo.maxDoc();
    final PackedLongValues values = pending.build();

    normsConsumer.addNormsField(fieldInfo,
                                new NormsProducer() {
                                  @Override
                                  public NumericDocValues getNorms(FieldInfo fieldInfo2) {
                                   if (fieldInfo != NormValuesWriter.this.fieldInfo) {
                                     throw new IllegalArgumentException("wrong fieldInfo");
                                   }
                                   return new BufferedNorms(maxDoc, values);
                                  }

                                  @Override
                                  public void checkIntegrity() {
                                  }

                                  @Override
                                  public void close() {
                                  }
                                  
                                  @Override
                                  public long ramBytesUsed() {
                                    return 0;
                                  }
                               });
  }

  // TODO: norms should only visit docs that had a field indexed!!
  
  // iterates over the values we have in ram
  private static class BufferedNorms extends NumericDocValues {
    final PackedLongValues.Iterator iter;
    final int size;
    final int maxDoc;
    private int docID = -1;
    private long value;
    
    BufferedNorms(int maxDoc, PackedLongValues values) {
      this.maxDoc = maxDoc;
      this.iter = values.iterator();
      this.size = (int) values.size();
    }
    
    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        docID = NO_MORE_DOCS;
      }
      if (docID < size) {
        value = iter.next();
      } else {
        value = MISSING;
      }
      return docID;
    }
    
    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return maxDoc;
    }

    @Override
    public long longValue() {
      return value;
    }
  }
}

