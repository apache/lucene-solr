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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
class NumericDocValuesWriter extends DocValuesWriter<NumericDocValues> {

  private PackedLongValues.Builder pending;
  private PackedLongValues finalValues;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private DocsWithFieldSet docsWithField;
  private final FieldInfo fieldInfo;
  private int lastDocID = -1;

  NumericDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, long value) {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
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

  @Override
  NumericDocValues getDocValues() {
    if (finalValues == null) {
      finalValues = pending.build();
    }
    return new BufferedNumericDocValues(finalValues, docsWithField.iterator());
  }

  static NumericDVs sortDocValues(int maxDoc, Sorter.DocMap sortMap, NumericDocValues oldDocValues) throws IOException {
    FixedBitSet docsWithField = new FixedBitSet(maxDoc);
    long[] values = new long[maxDoc];
    while (true) {
      int docID = oldDocValues.nextDoc();
      if (docID == NO_MORE_DOCS) {
        break;
      }
      int newDocID = sortMap.oldToNew(docID);
      docsWithField.set(newDocID);
      values[newDocID] = oldDocValues.longValue();
    }
    return new NumericDVs(values, docsWithField);
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    if (finalValues == null) {
      finalValues = pending.build();
    }
    final NumericDVs sorted;
    if (sortMap != null) {
      NumericDocValues oldValues = new BufferedNumericDocValues(finalValues, docsWithField.iterator());
      sorted = sortDocValues(state.segmentInfo.maxDoc(), sortMap, oldValues);
    } else {
      sorted = null;
    }

    dvConsumer.addNumericField(fieldInfo,
                               new EmptyDocValuesProducer() {
                                 @Override
                                 public NumericDocValues getNumeric(FieldInfo fieldInfo) {
                                   if (fieldInfo != NumericDocValuesWriter.this.fieldInfo) {
                                     throw new IllegalArgumentException("wrong fieldInfo");
                                   }
                                   if (sorted == null) {
                                     return new BufferedNumericDocValues(finalValues, docsWithField.iterator());
                                   } else {
                                     return new SortingNumericDocValues(sorted);
                                   }
                                 }
                               });
  }

  // iterates over the values we have in ram
  private static class BufferedNumericDocValues extends NumericDocValues {
    final PackedLongValues.Iterator iter;
    final DocIdSetIterator docsWithField;
    private long value;

    BufferedNumericDocValues(PackedLongValues values, DocIdSetIterator docsWithFields) {
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

  static class SortingNumericDocValues extends NumericDocValues {

    private final NumericDVs dvs;
    private int docID = -1;
    private long cost = -1;

    SortingNumericDocValues(NumericDVs dvs) {
      this.dvs = dvs;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      if (docID+1 == dvs.docsWithField.length()) {
        docID = NO_MORE_DOCS;
      } else {
        docID = dvs.docsWithField.nextSetBit(docID+1);
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException("use nextDoc() instead");
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      // needed in IndexSorter#{Long|Int|Double|Float}Sorter
      docID = target;
      return dvs.docsWithField.get(target);
    }

    @Override
    public long longValue() {
      return dvs.values[docID];
    }

    @Override
    public long cost() {
      if (cost == -1) {
        cost = dvs.docsWithField.cardinality();
      }
      return cost;
    }
  }

  static class NumericDVs {
    private final long[] values;
    private final BitSet docsWithField;

    NumericDVs(long[] values, BitSet docsWithField) {
      this.values = values;
      this.docsWithField = docsWithField;
    }
  }
}
