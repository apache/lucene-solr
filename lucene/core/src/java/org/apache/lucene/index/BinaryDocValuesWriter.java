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

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.AppendingLongBuffer;


/** Buffers up pending byte[] per doc, then flushes when
 *  segment flushes. */
class BinaryDocValuesWriter extends DocValuesWriter {

  private final RAMFile bytes;
  private final RAMOutputStream bytesWriter;
  private final AppendingLongBuffer lengths;
  private final FieldInfo fieldInfo;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private int addedValues = 0;

  public BinaryDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.bytes = new RAMFile();
    this.bytesWriter = new RAMOutputStream(bytes);
    this.iwBytesUsed = iwBytesUsed;
    this.lengths = new AppendingLongBuffer();
  }

  public void addValue(int docID, BytesRef value) {
    if (docID < addedValues) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + (BYTE_BLOCK_SIZE - 2));
    }
    
    // Fill in any holes:
    while(addedValues < docID) {
      addedValues++;
      lengths.add(0);
    }
    addedValues++;
    lengths.add(value.length);
    try {
      bytesWriter.writeBytes(value.bytes, value.offset, value.length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    updateBytesUsed();
  }
  
  private void updateBytesUsed() {
    // nocommit not totally accurate, but just fix not to use RAMFile anyway
    long numBuffers = (bytesWriter.getFilePointer() / 1024) + 1; // round up
    long oversize = numBuffers * (1024 + 32); // fudge for arraylist/etc overhead
    final long newBytesUsed = lengths.ramBytesUsed() + oversize;
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void finish(int maxDoc) {
    try {
      bytesWriter.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush(SegmentWriteState state, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.getDocCount();
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
    final AppendingLongBuffer.Iterator lengthsIterator = lengths.iterator();
    final int size = lengths.size();
    final int maxDoc;
    final RAMInputStream bytesReader;
    int upto;
    
    BytesIterator(int maxDoc) {
      this.maxDoc = maxDoc;
      try {
        bytesReader = new RAMInputStream("BinaryDocValuesWriter", bytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
        try {
          bytesReader.readBytes(value.bytes, 0, length);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        value.length = length;
      } else {
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
