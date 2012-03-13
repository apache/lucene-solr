package org.apache.lucene.codecs.lucene40.values;

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

import org.apache.lucene.codecs.lucene40.values.Bytes.BytesReaderBase;
import org.apache.lucene.codecs.lucene40.values.Bytes.BytesSourceBase;
import org.apache.lucene.codecs.lucene40.values.Bytes.BytesWriterBase;
import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ByteBlockPool.DirectTrackingAllocator;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

// Simplest storage: stores fixed length byte[] per
// document, with no dedup and no sorting.
/**
 * @lucene.experimental
 */
class FixedStraightBytesImpl {

  static final String CODEC_NAME = "FixedStraightBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  
  static abstract class FixedBytesWriterBase extends BytesWriterBase {
    protected final DocValuesField bytesSpareField = new DocValuesField("", new BytesRef(), Type.BYTES_FIXED_STRAIGHT);
    protected int lastDocID = -1;
    // start at -1 if the first added value is > 0
    protected int size = -1;
    private final int byteBlockSize = BYTE_BLOCK_SIZE;
    private final ByteBlockPool pool;

    protected FixedBytesWriterBase(Directory dir, String id, String codecName,
        int version, Counter bytesUsed, IOContext context) throws IOException {
     this(dir, id, codecName, version, bytesUsed, context, Type.BYTES_FIXED_STRAIGHT);
    }
    
    protected FixedBytesWriterBase(Directory dir, String id, String codecName,
        int version, Counter bytesUsed, IOContext context, Type type) throws IOException {
      super(dir, id, codecName, version, bytesUsed, context, type);
      pool = new ByteBlockPool(new DirectTrackingAllocator(bytesUsed));
      pool.nextBuffer();
    }
    
    @Override
    public void add(int docID, IndexableField value) throws IOException {
      final BytesRef bytes = value.binaryValue();
      assert bytes != null;
      assert lastDocID < docID;

      if (size == -1) {
        if (bytes.length > BYTE_BLOCK_SIZE) {
          throw new IllegalArgumentException("bytes arrays > " + BYTE_BLOCK_SIZE + " are not supported");
        }
        size = bytes.length;
      } else if (bytes.length != size) {
        throw new IllegalArgumentException("byte[] length changed for BYTES_FIXED_STRAIGHT type (before=" + size + " now=" + bytes.length);
      }
      if (lastDocID+1 < docID) {
        advancePool(docID);
      }
      pool.copy(bytes);
      lastDocID = docID;
    }
    
    private final void advancePool(int docID) {
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
    
    protected void set(BytesRef ref, int docId) {
      assert BYTE_BLOCK_SIZE % size == 0 : "BYTE_BLOCK_SIZE ("+ BYTE_BLOCK_SIZE + ") must be a multiple of the size: " + size;
      ref.offset = docId*size;
      ref.length = size;
      pool.deref(ref);
    }
    
    protected void resetPool() {
      pool.dropBuffersAndReset();
    }
    
    protected void writeData(IndexOutput out) throws IOException {
      pool.writePool(out);
    }
    
    protected void writeZeros(int num, IndexOutput out) throws IOException {
      final byte[] zeros = new byte[size];
      for (int i = 0; i < num; i++) {
        out.writeBytes(zeros, zeros.length);
      }
    }
  }

  static class Writer extends FixedBytesWriterBase {
    private boolean hasMerged;
    private IndexOutput datOut;
    
    public Writer(Directory dir, String id, Counter bytesUsed, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context);
    }

    public Writer(Directory dir, String id, String codecName, int version, Counter bytesUsed, IOContext context) throws IOException {
      super(dir, id, codecName, version, bytesUsed, context);
    }


    @Override
    protected void merge(DocValues readerIn, int docBase, int docCount, Bits liveDocs) throws IOException {
      datOut = getOrCreateDataOut();
      boolean success = false;
      try {
        if (!hasMerged && size != -1) {
          datOut.writeInt(size);
        }

        if (liveDocs == null && tryBulkMerge(readerIn)) {
          FixedStraightReader reader = (FixedStraightReader) readerIn;
          final int maxDocs = reader.maxDoc;
          if (maxDocs == 0) {
            return;
          }
          if (size == -1) {
            size = reader.size;
            datOut.writeInt(size);
          } else if (size != reader.size) {
            throw new IllegalArgumentException("expected bytes size=" + size
                + " but got " + reader.size);
           }
          if (lastDocID+1 < docBase) {
            fill(datOut, docBase);
            lastDocID = docBase-1;
          }
          // TODO should we add a transfer to API to each reader?
          final IndexInput cloneData = reader.cloneData();
          try {
            datOut.copyBytes(cloneData, size * maxDocs);
          } finally {
            IOUtils.close(cloneData);  
          }
        
          lastDocID += maxDocs;
        } else {
          super.merge(readerIn, docBase, docCount, liveDocs);
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(datOut);
        }
        hasMerged = true;
      }
    }
    
    protected boolean tryBulkMerge(DocValues docValues) {
      return docValues instanceof FixedStraightReader;
    }
    
    @Override
    protected void mergeDoc(Field scratchField, Source source, int docID, int sourceDoc) throws IOException {
      assert lastDocID < docID;
      setMergeBytes(source, sourceDoc);
      if (size == -1) {
        size = bytesRef.length;
        datOut.writeInt(size);
      }
      assert size == bytesRef.length : "size: " + size + " ref: " + bytesRef.length;
      if (lastDocID+1 < docID) {
        fill(datOut, docID);
      }
      datOut.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
      lastDocID = docID;
    }
    
    protected void setMergeBytes(Source source, int sourceDoc) {
      source.getBytes(sourceDoc, bytesRef);
    }

    // Fills up to but not including this docID
    private void fill(IndexOutput datOut, int docID) throws IOException {
      assert size >= 0;
      writeZeros((docID - (lastDocID+1)), datOut);
    }

    @Override
    public void finish(int docCount) throws IOException {
      boolean success = false;
      try {
        if (!hasMerged) {
          // indexing path - no disk IO until here
          assert datOut == null;
          datOut = getOrCreateDataOut();
          if (size == -1) {
            datOut.writeInt(0);
          } else {
            datOut.writeInt(size);
            writeData(datOut);
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
        resetPool();
        if (success) {
          IOUtils.close(datOut);
        } else {
          IOUtils.closeWhileHandlingException(datOut);
        }
      }
    }
  
  }
  
  public static class FixedStraightReader extends BytesReaderBase {
    protected final int size;
    protected final int maxDoc;
    
    FixedStraightReader(Directory dir, String id, int maxDoc, IOContext context) throws IOException {
      this(dir, id, CODEC_NAME, VERSION_CURRENT, maxDoc, context, Type.BYTES_FIXED_STRAIGHT);
    }

    protected FixedStraightReader(Directory dir, String id, String codec, int version, int maxDoc, IOContext context, Type type) throws IOException {
      super(dir, id, codec, version, false, context, type);
      size = datIn.readInt();
      this.maxDoc = maxDoc;
    }

    @Override
    public Source load() throws IOException {
      return size == 1 ? new SingleByteSource(cloneData(), maxDoc) : 
        new FixedStraightSource(cloneData(), size, maxDoc, type);
    }

    @Override
    public void close() throws IOException {
      datIn.close();
    }
   
    @Override
    public Source getDirectSource() throws IOException {
      return new DirectFixedStraightSource(cloneData(), size, getType());
    }
    
    @Override
    public int getValueSize() {
      return size;
    }
  }
  
  // specialized version for single bytes
  private static final class SingleByteSource extends Source {
    private final byte[] data;

    public SingleByteSource(IndexInput datIn, int maxDoc) throws IOException {
      super(Type.BYTES_FIXED_STRAIGHT);
      try {
        data = new byte[maxDoc];
        datIn.readBytes(data, 0, data.length, false);
      } finally {
        IOUtils.close(datIn);
      }
    }
    
    @Override
    public boolean hasArray() {
      return true;
    }

    @Override
    public Object getArray() {
      return data;
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      bytesRef.length = 1;
      bytesRef.bytes = data;
      bytesRef.offset = docID;
      return bytesRef;
    }
  }

  
  private final static class FixedStraightSource extends BytesSourceBase {
    private final int size;

    public FixedStraightSource(IndexInput datIn, int size, int maxDoc, Type type)
        throws IOException {
      super(datIn, null, new PagedBytes(PAGED_BYTES_BITS), size * maxDoc,
          type);
      this.size = size;
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      return data.fillSlice(bytesRef, docID * size, size);
    }
  }
  
  public final static class DirectFixedStraightSource extends DirectSource {
    private final int size;

    DirectFixedStraightSource(IndexInput input, int size, Type type) {
      super(input, type);
      this.size = size;
    }

    @Override
    protected int position(int docID) throws IOException {
      data.seek(baseOffset + size * docID);
      return size;
    }

  }
}
