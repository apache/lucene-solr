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
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Buffers up pending byte[] per doc, then flushes when
 *  segment flushes. */
class BinaryDocValuesWriter extends DocValuesWriter<BinaryDocValues> {

  /** Maximum length for a binary field. */
  private static final int MAX_LENGTH = ArrayUtil.MAX_ARRAY_LENGTH;

  // 4 KB block sizes for PagedBytes storage:
  private final static int BLOCK_BITS = 12;

  private final PagedBytes bytes;
  private final DataOutput bytesOut;

  private final Counter iwBytesUsed;
  private final PackedLongValues.Builder lengths;
  private DocsWithFieldSet docsWithField;
  private final FieldInfo fieldInfo;
  private long bytesUsed;
  private int lastDocID = -1;
  private int maxLength = 0;

  private PackedLongValues finalLengths;

  BinaryDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.bytes = new PagedBytes(BLOCK_BITS);
    this.bytesOut = bytes.getDataOutput();
    this.lengths = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    this.iwBytesUsed = iwBytesUsed;
    this.docsWithField = new DocsWithFieldSet();
    this.bytesUsed = lengths.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, BytesRef value) {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > MAX_LENGTH) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + MAX_LENGTH);
    }

    maxLength = Math.max(value.length, maxLength);
    lengths.add(value.length);
    try {
      bytesOut.writeBytes(value.bytes, value.offset, value.length);
    } catch (IOException ioe) {
      // Should never happen!
      throw new RuntimeException(ioe);
    }
    docsWithField.add(docID);
    updateBytesUsed();

    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = lengths.ramBytesUsed() + bytes.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  BinaryDocValues getDocValues() {
    if (finalLengths == null) {
      finalLengths = this.lengths.build();
    }
    return new BufferedBinaryDocValues(finalLengths, maxLength, bytes.getDataInput(), docsWithField.iterator());
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    bytes.freeze(false);
    if (finalLengths == null) {
      finalLengths = this.lengths.build();
    }
    final BinaryDVs sorted;
    if (sortMap != null) {
      sorted = new BinaryDVs(state.segmentInfo.maxDoc(), sortMap,
          new BufferedBinaryDocValues(finalLengths, maxLength, bytes.getDataInput(), docsWithField.iterator()));
    } else {
      sorted = null;
    }
    dvConsumer.addBinaryField(fieldInfo,
                              new EmptyDocValuesProducer() {
                                @Override
                                public BinaryDocValues getBinary(FieldInfo fieldInfoIn) {
                                  if (fieldInfoIn != fieldInfo) {
                                    throw new IllegalArgumentException("wrong fieldInfo");
                                  }
                                  if (sorted == null) {
                                    return new BufferedBinaryDocValues(finalLengths, maxLength, bytes.getDataInput(), docsWithField.iterator());
                                  } else {
                                    return new SortingBinaryDocValues(sorted);
                                  }
                                }
                              });
  }

  // iterates over the values we have in ram
  private static class BufferedBinaryDocValues extends BinaryDocValues {
    final BytesRefBuilder value;
    final PackedLongValues.Iterator lengthsIterator;
    final DocIdSetIterator docsWithField;
    final DataInput bytesIterator;
    
    BufferedBinaryDocValues(PackedLongValues lengths, int maxLength, DataInput bytesIterator, DocIdSetIterator docsWithFields) {
      this.value = new BytesRefBuilder();
      this.value.grow(maxLength);
      this.lengthsIterator = lengths.iterator();
      this.bytesIterator = bytesIterator;
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
        int length = Math.toIntExact(lengthsIterator.next());
        value.setLength(length);
        bytesIterator.readBytes(value.bytes(), 0, length);
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
    public BytesRef binaryValue() {
      return value.get();
    }
  }

  static class SortingBinaryDocValues extends BinaryDocValues {
    private final BinaryDVs dvs;
    private final BytesRefBuilder spare = new BytesRefBuilder();
    private int docID = -1;

    SortingBinaryDocValues(BinaryDVs dvs) {
      this.dvs = dvs;
    }

    @Override
    public int nextDoc() {
      do {
        docID++;
        if (docID == dvs.offsets.length) {
          return docID = NO_MORE_DOCS;
        }
      } while (dvs.offsets[docID] <= 0);
      return docID;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException("use nextDoc instead");
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      throw new UnsupportedOperationException("use nextDoc instead");
    }

    @Override
    public BytesRef binaryValue() {
      dvs.values.get(spare, dvs.offsets[docID]-1);
      return spare.get();
    }

    @Override
    public long cost() {
      return dvs.values.size();
    }
  }

  static final class BinaryDVs {
    final int[] offsets;
    final BytesRefArray values;
    BinaryDVs(int maxDoc, Sorter.DocMap sortMap, BinaryDocValues oldValues) throws IOException {
      offsets = new int[maxDoc];
      values = new BytesRefArray(Counter.newCounter());
      int offset = 1; // 0 means no values for this document
      int docID;
      while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
        int newDocID = sortMap.oldToNew(docID);
        values.append(oldValues.binaryValue());
        offsets[newDocID] = offset++;
      }
    }
  }
}
