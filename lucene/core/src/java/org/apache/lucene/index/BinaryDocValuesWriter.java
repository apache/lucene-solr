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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** Buffers up pending byte[] per doc, then flushes when
 *  segment flushes. */
class BinaryDocValuesWriter extends DocValuesWriter {

  /** Maximum length for a binary field. */
  private static final int MAX_LENGTH = ArrayUtil.MAX_ARRAY_LENGTH;

  // 32 KB block sizes for PagedBytes storage:
  private final static int BLOCK_BITS = 15;

  private final PagedBytes bytes;
  private final DataOutput bytesOut;

  private final Counter iwBytesUsed;
  private final PackedLongValues.Builder lengths;
  private FixedBitSet docsWithField;
  private final FieldInfo fieldInfo;
  private int addedValues;
  private long bytesUsed;

  public BinaryDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.bytes = new PagedBytes(BLOCK_BITS);
    this.bytesOut = bytes.getDataOutput();
    this.lengths = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    this.iwBytesUsed = iwBytesUsed;
    this.docsWithField = new FixedBitSet(64);
    this.bytesUsed = docsWithFieldBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, BytesRef value) {
    if (docID < addedValues) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > MAX_LENGTH) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + MAX_LENGTH);
    }

    // Fill in any holes:
    while(addedValues < docID) {
      addedValues++;
      lengths.add(0);
    }
    addedValues++;
    lengths.add(value.length);
    try {
      bytesOut.writeBytes(value.bytes, value.offset, value.length);
    } catch (IOException ioe) {
      // Should never happen!
      throw new RuntimeException(ioe);
    }
    docsWithField = FixedBitSet.ensureCapacity(docsWithField, docID);
    docsWithField.set(docID);
    updateBytesUsed();
  }
  
  private long docsWithFieldBytesUsed() {
    // size of the long[] + some overhead
    return RamUsageEstimator.sizeOf(docsWithField.getBits()) + 64;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = lengths.ramBytesUsed() + bytes.ramBytesUsed() + docsWithFieldBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void finish(int maxDoc) {

  }

  @Override
  Sorter.DocComparator getDocComparator(int numDoc, SortField sortField) throws IOException {
    throw new IllegalArgumentException("It is forbidden to sort on a binary field");
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.maxDoc();
    bytes.freeze(false);
    final PackedLongValues lengths = this.lengths.build();

    final long[] starts;
    if (sortMap != null) {
      starts = new long[maxDoc];
      PackedLongValues.Iterator it = lengths.iterator();
      long ptr = 0;
      int doc = 0;
      while (it.hasNext()) {
        starts[doc++] = ptr;
        ptr += it.next();
      }
    } else {
      starts = null;
    }

    dvConsumer.addBinaryField(fieldInfo,
                              new Iterable<BytesRef>() {
                                @Override
                                public Iterator<BytesRef> iterator() {
                                  if (sortMap == null) {
                                    return new BytesIterator(maxDoc, lengths);
                                  } else {
                                    return new SortingBytesIterator(maxDoc, lengths, sortMap, starts);
                                  }
                                }
                              });
  }

  // iterates over the values we have in ram
  private class BytesIterator implements Iterator<BytesRef> {
    final BytesRefBuilder value = new BytesRefBuilder();
    final PackedLongValues.Iterator lengthsIterator;
    final PagedBytes.PagedBytesDataInput bytesIterator = bytes.getDataInput();
    final int size = (int) lengths.size();
    final int maxDoc;
    int upto;
    
    BytesIterator(int maxDoc, PackedLongValues lengths) {
      this.maxDoc = maxDoc;
      this.lengthsIterator = lengths.iterator();
    }
    
    @Override
    public boolean hasNext() {
      return upto < maxDoc;
    }

    @Override
    public BytesRef next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final BytesRef v;
      if (upto < size) {
        final int length = (int) lengthsIterator.next();
        value.grow(length);
        value.setLength(length);
        bytesIterator.readBytes(value.bytes(), 0, value.length());
        if (docsWithField.get(upto)) {
          v = value.get();
        } else {
          v = null;
        }
      } else {
        v = null;
      }
      upto++;
      return v;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  // sort the values we have in ram according to the provided sort map
  private class SortingBytesIterator implements Iterator<BytesRef> {
    final BytesRefBuilder value = new BytesRefBuilder();
    final PackedLongValues values;
    final PackedLongValues.Iterator lengthsIterator;
    final long[] starts;
    final PagedBytes.PagedBytesDataInput bytesIterator = bytes.getDataInput();
    final Sorter.DocMap sortMap;
    final int size = (int) lengths.size();
    final int maxDoc;
    int upto;

    SortingBytesIterator(int maxDoc, PackedLongValues lengths, Sorter.DocMap sortMap, long[] starts) {
      this.maxDoc = maxDoc;
      this.lengthsIterator = lengths.iterator();
      this.values = lengths;
      this.sortMap = sortMap;
      this.starts = starts;
    }

    @Override
    public boolean hasNext() {
      return upto < maxDoc;
    }

    @Override
    public BytesRef next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final BytesRef v;
      int oldID = sortMap.newToOld(upto);
      if (oldID < values.size()) {
        int length = (int) values.get(oldID);
        long pos = starts[oldID];
        bytesIterator.setPosition(pos);
        value.grow(length);
        value.setLength(length);
        bytesIterator.readBytes(value.bytes(), 0, value.length());
        if (docsWithField.get(oldID)) {
          v = value.get();
        } else {
          v = null;
        }
      } else {
        v = null;
      }
      upto++;
      return v;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
