package org.apache.lucene.index.values;

/**
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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.values.Bytes.BytesBaseSource;
import org.apache.lucene.index.values.Bytes.BytesReaderBase;
import org.apache.lucene.index.values.Bytes.BytesWriterBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.ByteBlockPool.DirectTrackingAllocator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;

// Simplest storage: stores fixed length byte[] per
// document, with no dedup and no sorting.
/**
 * @lucene.experimental
 */
class FixedStraightBytesImpl {

  static final String CODEC_NAME = "FixedStraightBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class Writer extends BytesWriterBase {
    private int size = -1;
    // start at -1 if the first added value is > 0
    private int lastDocID = -1;
    private final ByteBlockPool pool;
    private boolean merge;
    private final int byteBlockSize;
    private IndexOutput datOut;

    public Writer(Directory dir, String id, AtomicLong bytesUsed, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context);
      pool = new ByteBlockPool(new DirectTrackingAllocator(bytesUsed));
      byteBlockSize = BYTE_BLOCK_SIZE;
    }

    @Override
    public void add(int docID, BytesRef bytes) throws IOException {
      assert lastDocID < docID;
      assert !merge;
      if (size == -1) {
        if (bytes.length > BYTE_BLOCK_SIZE) {
          throw new IllegalArgumentException("bytes arrays > " + Short.MAX_VALUE + " are not supported");
        }
        size = bytes.length;
        pool.nextBuffer();
      } else if (bytes.length != size) {
        throw new IllegalArgumentException("expected bytes size=" + size
            + " but got " + bytes.length);
      }
      if (lastDocID+1 < docID) {
        advancePool(docID);
      }
      pool.copy(bytes);
      lastDocID = docID;
    }
    
    private final void advancePool(int docID) {
      assert !merge;
      long numBytes = (docID - (lastDocID+1))*size;
      while(numBytes > 0) {
        if (numBytes + pool.byteUpto < byteBlockSize) {
          pool.byteUpto += numBytes;
          numBytes = 0;
        } else {
          numBytes -= byteBlockSize - pool.byteUpto;
          pool.nextBuffer();
        }
      }
      assert numBytes == 0;
    }

    @Override
    protected void merge(MergeState state) throws IOException {
      merge = true;
      datOut = getDataOut();
      boolean success = false;
      try {
      if (state.liveDocs == null && state.reader instanceof Reader) {
        Reader reader = (Reader) state.reader;
        final int maxDocs = reader.maxDoc;
        if (maxDocs == 0) {
          return;
        }
        if (size == -1) {
          size = reader.size;
          datOut.writeInt(size);
        }
        if (lastDocID+1 < state.docBase) {
          fill(datOut, state.docBase);
          lastDocID = state.docBase-1;
        }
        // TODO should we add a transfer to API to each reader?
        final IndexInput cloneData = reader.cloneData();
        try {
          datOut.copyBytes(cloneData, size * maxDocs);
        } finally {
          IOUtils.closeSafely(true, cloneData);  
        }
        
        lastDocID += maxDocs;
      } else {
        super.merge(state);
      }
      success = true;
      } finally {
        if (!success) {
          IOUtils.closeSafely(!success, datOut);
        }
      }
    }
    
    

    @Override
    protected void mergeDoc(int docID) throws IOException {
      assert lastDocID < docID;
      if (size == -1) {
        size = bytesRef.length;
        datOut.writeInt(size);
      }
      assert size == bytesRef.length;
      if (lastDocID+1 < docID) {
        fill(datOut, docID);
      }
      datOut.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
      lastDocID = docID;
    }



    // Fills up to but not including this docID
    private void fill(IndexOutput datOut, int docID) throws IOException {
      assert size >= 0;
      final long numBytes = (docID - (lastDocID+1))*size;
      final byte zero = 0;
      for (long i = 0; i < numBytes; i++) {
        datOut.writeByte(zero);
      }
    }

    @Override
    public void finish(int docCount) throws IOException {
      boolean success = false;
      try {
        if (!merge) {
          // indexing path - no disk IO until here
          assert datOut == null;
          datOut = getDataOut();
          if (size == -1) {
            datOut.writeInt(0);
          } else {
            datOut.writeInt(size);
            pool.writePool(datOut);
          }
          if (lastDocID + 1 < docCount) {
            fill(datOut, docCount);
          }
        } else {
          // merge path - datOut should be initialized
          assert datOut != null;
          if (size == -1) {// no data added
            datOut.writeInt(0);
          } else {
            fill(datOut, docCount);
          }
        }
        success = true;
      } finally {
        pool.dropBuffersAndReset();
        IOUtils.closeSafely(!success, datOut);
      }
    }
  }
  
  public static class Reader extends BytesReaderBase {
    private final int size;
    private final int maxDoc;

    Reader(Directory dir, String id, int maxDoc, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, false, context);
      size = datIn.readInt();
      this.maxDoc = maxDoc;
    }

    @Override
    public Source load() throws IOException {
      return size == 1 ? new SingleByteSource(cloneData(), maxDoc) : 
        new StraightBytesSource(cloneData(), size, maxDoc);
    }

    @Override
    public void close() throws IOException {
      datIn.close();
    }
    
    // specialized version for single bytes
    private static class SingleByteSource extends Source {
      private final int maxDoc;
      private final byte[] data;

      public SingleByteSource(IndexInput datIn, int maxDoc) throws IOException {
        this.maxDoc = maxDoc;
        try {
          data = new byte[maxDoc];
          datIn.readBytes(data, 0, data.length, false);
        } finally {
          IOUtils.closeSafely(false, datIn);
        }

      }

      @Override
      public BytesRef getBytes(int docID, BytesRef bytesRef) {
        bytesRef.length = 1;
        bytesRef.bytes = data;
        bytesRef.offset = docID;
        return bytesRef;
      }
      
      @Override
      public ValueType type() {
        return ValueType.BYTES_FIXED_STRAIGHT;
      }

      @Override
      public ValuesEnum getEnum(AttributeSource attrSource) throws IOException {
        return new SourceEnum(attrSource, type(), this, maxDoc) {
          @Override
          public int advance(int target) throws IOException {
            if (target >= numDocs) {
              return pos = NO_MORE_DOCS;
            }
            bytesRef.length = 1;
            bytesRef.bytes = data;
            bytesRef.offset = target;
            return pos = target;
          }
        };
      }

    }

    private static class StraightBytesSource extends BytesBaseSource {
      private final int size;
      private final int maxDoc;

      public StraightBytesSource(IndexInput datIn, int size, int maxDoc)
          throws IOException {
        super(datIn, null, new PagedBytes(PAGED_BYTES_BITS), size * maxDoc);
        this.size = size;
        this.maxDoc = maxDoc;
      }

      @Override
      public BytesRef getBytes(int docID, BytesRef bytesRef) {
        return data.fillSlice(bytesRef, docID * size, size);
      }
      
      @Override
      public int getValueCount() {
        return maxDoc;
      }

      @Override
      public ValueType type() {
        return ValueType.BYTES_FIXED_STRAIGHT;
      }

      @Override
      protected int maxDoc() {
        return maxDoc;
      }
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      return new FixedStraightBytesEnum(source, cloneData(), size, maxDoc);
    }

    private static final class FixedStraightBytesEnum extends ValuesEnum {
      private final IndexInput datIn;
      private final int size;
      private final int maxDoc;
      private int pos = -1;
      private final long fp;

      public FixedStraightBytesEnum(AttributeSource source, IndexInput datIn,
          int size, int maxDoc) throws IOException {
        super(source, ValueType.BYTES_FIXED_STRAIGHT);
        this.datIn = datIn;
        this.size = size;
        this.maxDoc = maxDoc;
        bytesRef.grow(size);
        bytesRef.length = size;
        bytesRef.offset = 0;
        fp = datIn.getFilePointer();
      }

      protected void copyFrom(ValuesEnum valuesEnum) {
        bytesRef = valuesEnum.bytesRef;
        if (bytesRef.bytes.length < size) {
          bytesRef.grow(size);
        }
        bytesRef.length = size;
        bytesRef.offset = 0;
      }

      public void close() throws IOException {
        datIn.close();
      }

      @Override
      public int advance(int target) throws IOException {
        if (target >= maxDoc || size == 0) {
          return pos = NO_MORE_DOCS;
        }
        if ((target - 1) != pos) // pos inc == 1
          datIn.seek(fp + target * size);
        datIn.readBytes(bytesRef.bytes, 0, size);
        return pos = target;
      }

      @Override
      public int docID() {
        return pos;
      }

      @Override
      public int nextDoc() throws IOException {
        if (pos >= maxDoc) {
          return pos = NO_MORE_DOCS;
        }
        return advance(pos + 1);
      }
    }

    @Override
    public ValueType type() {
      return ValueType.BYTES_FIXED_STRAIGHT;
    }
  }
}
