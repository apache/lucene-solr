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

import java.io.IOException;

import org.apache.lucene.index.values.Bytes.BytesBaseSource;
import org.apache.lucene.index.values.Bytes.BytesReaderBase;
import org.apache.lucene.index.values.Bytes.BytesWriterBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.AttributeSource;
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
    private byte[] oneRecord;

    public Writer(Directory dir, String id, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, false, null, null, context);
    }


    @Override
    public void add(int docID, BytesRef bytes) throws IOException {
      if (size == -1) {
        size = bytes.length;
        datOut.writeInt(size);
        oneRecord = new byte[size];
      } else if (bytes.length != size) {
        throw new IllegalArgumentException("expected bytes size=" + size
            + " but got " + bytes.length);
      }
      fill(docID);
      assert bytes.bytes.length >= bytes.length;
      datOut.writeBytes(bytes.bytes, bytes.offset, bytes.length);
    }

    @Override
    protected void merge(MergeState state) throws IOException {
      if (state.bits == null && state.reader instanceof Reader) {
        Reader reader = (Reader) state.reader;
        final int maxDocs = reader.maxDoc;
        if (maxDocs == 0) {
          return;
        }
        if (size == -1) {
          size = reader.size;
          datOut.writeInt(size);
          oneRecord = new byte[size];
        }
        fill(state.docBase);
        // TODO should we add a transfer to API to each reader?
        final IndexInput cloneData = reader.cloneData();
        try {
          datOut.copyBytes(cloneData, size * maxDocs);
        } finally {
          cloneData.close();  
        }
        
        lastDocID += maxDocs - 1;
      } else {
        super.merge(state);
      }
    }

    // Fills up to but not including this docID
    private void fill(int docID) throws IOException {
      assert size >= 0;
      for (int i = lastDocID + 1; i < docID; i++) {
        datOut.writeBytes(oneRecord, size);
      }
      lastDocID = docID;
    }

    @Override
    public void finish(int docCount) throws IOException {
      try {
        if (size == -1) {// no data added
          datOut.writeInt(0);
        } else {
          fill(docCount);
        }
      } finally {
        super.finish(docCount);
      }
    }

    public long ramBytesUsed() {
      return oneRecord == null ? 0 : oneRecord.length;
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
