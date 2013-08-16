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
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.AppendingDeltaPackedLongBuffer;
import org.apache.lucene.util.packed.PackedInts;

/** Buffers up pending byte[] per doc, then flushes when
 *  segment flushes. */
class BinaryDocValuesWriter extends DocValuesWriter {

  /** Maximum length for a binary field; we set this to "a
   *  bit" below Integer.MAX_VALUE because the exact max
   *  allowed byte[] is JVM dependent, so we want to avoid
   *  a case where a large value worked in one JVM but
   *  failed later at search time with a different JVM.  */
  private static final int MAX_LENGTH = Integer.MAX_VALUE-256;

  // 32 KB block sizes for PagedBytes storage:
  private final static int BLOCK_BITS = 15;

  private final PagedBytes bytes;
  private final DataOutput bytesOut;

  private final Counter iwBytesUsed;
  private final AppendingDeltaPackedLongBuffer lengths;
  private final FieldInfo fieldInfo;
  private int addedValues;
  private long bytesUsed;

  public BinaryDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.bytes = new PagedBytes(BLOCK_BITS);
    this.bytesOut = bytes.getDataOutput();
    this.lengths = new AppendingDeltaPackedLongBuffer(PackedInts.COMPACT);
    this.iwBytesUsed = iwBytesUsed;
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
    updateBytesUsed();
  }

  private void updateBytesUsed() {
    final long newBytesUsed = lengths.ramBytesUsed() + bytes.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void finish(int maxDoc) {
  }

  @Override
  public void flush(SegmentWriteState state, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.getDocCount();
    bytes.freeze(false);
    dvConsumer.addBinaryField(fieldInfo,
                              new Iterable<BytesRef>() {
                                @Override
                                public Iterator<BytesRef> iterator() {
                                   return new BytesIterator(maxDoc);                                 
                                }
                              });
  }

  @Override
  public void abort() {
  }
  
  // iterates over the values we have in ram
  private class BytesIterator implements Iterator<BytesRef> {
    final BytesRef value = new BytesRef();
    final AppendingDeltaPackedLongBuffer.Iterator lengthsIterator = lengths.iterator();
    final DataInput bytesIterator = bytes.getDataInput();
    final int size = (int) lengths.size();
    final int maxDoc;
    int upto;
    
    BytesIterator(int maxDoc) {
      this.maxDoc = maxDoc;
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
      if (upto < size) {
        int length = (int) lengthsIterator.next();
        value.grow(length);
        value.length = length;
        try {
          bytesIterator.readBytes(value.bytes, value.offset, value.length);
        } catch (IOException ioe) {
          // Should never happen!
          throw new RuntimeException(ioe);
        }
      } else {
        // This is to handle last N documents not having
        // this DV field in the end of the segment:
        value.length = 0;
      }
      upto++;
      return value;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
