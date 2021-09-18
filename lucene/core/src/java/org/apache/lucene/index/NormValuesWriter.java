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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
class NormValuesWriter {

  private DocsWithFieldSet docsWithField;
  private PackedLongValues.Builder pending;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private final FieldInfo fieldInfo;
  private int lastDocID = -1;

  public NormValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    docsWithField = new DocsWithFieldSet();
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, long value) {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("Norm for \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }

    pending.add(value);
    docsWithField.add(docID);

    updateBytesUsed();

    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  public void finish(int maxDoc) {
  }

  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, NormsConsumer normsConsumer) throws IOException {
    final PackedLongValues values = pending.build();
    final NumericDocValuesWriter.NumericDVs sorted;
    if (sortMap != null) {
      sorted = NumericDocValuesWriter.sortDocValues(state.segmentInfo.maxDoc(), sortMap,
          new BufferedNorms(values, docsWithField.iterator()));
    } else {
      sorted = null;
    }
    normsConsumer.addNormsField(fieldInfo,
                                new NormsProducer() {
                                  @Override
                                  public NumericDocValues getNorms(FieldInfo fieldInfo2) {
                                   if (fieldInfo != NormValuesWriter.this.fieldInfo) {
                                     throw new IllegalArgumentException("wrong fieldInfo");
                                   }
                                   if (sorted == null) {
                                     return new BufferedNorms(values, docsWithField.iterator());
                                   } else {
                                     return new NumericDocValuesWriter.SortingNumericDocValues(sorted);
                                   }
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
    final DocIdSetIterator docsWithField;
    private long value;

    BufferedNorms(PackedLongValues values, DocIdSetIterator docsWithFields) {
      this.iter = values.iterator();
      this.docsWithField = docsWithFields;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        value = iter.next();
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }

    @Override
    public long longValue() {
      return value;
    }
  }
}

