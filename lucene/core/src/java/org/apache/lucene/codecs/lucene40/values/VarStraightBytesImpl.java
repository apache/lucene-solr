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
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ByteBlockPool.DirectTrackingAllocator;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts.ReaderIterator;
import org.apache.lucene.util.packed.PackedInts;

// Variable length byte[] per document, no sharing

/**
 * @lucene.experimental
 */
class VarStraightBytesImpl {

  static final String CODEC_NAME = "VarStraightBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class Writer extends BytesWriterBase {
    private long address;
    // start at -1 if the first added value is > 0
    private int lastDocID = -1;
    private long[] docToAddress;
    private final ByteBlockPool pool;
    private IndexOutput datOut;
    private boolean merge = false;
    public Writer(Directory dir, String id, Counter bytesUsed, IOContext context)
        throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context, Type.BYTES_VAR_STRAIGHT);
      pool = new ByteBlockPool(new DirectTrackingAllocator(bytesUsed));
      docToAddress = new long[1];
      pool.nextBuffer(); // init
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT);
    }

    // Fills up to but not including this docID
    private void fill(final int docID, final long nextAddress) {
      if (docID >= docToAddress.length) {
        int oldSize = docToAddress.length;
        docToAddress = ArrayUtil.grow(docToAddress, 1 + docID);
        bytesUsed.addAndGet((docToAddress.length - oldSize)
            * RamUsageEstimator.NUM_BYTES_INT);
      }
      for (int i = lastDocID + 1; i < docID; i++) {
        docToAddress[i] = nextAddress;
      }
    }

    @Override
    public void add(int docID, IndexableField value) throws IOException {
      final BytesRef bytes = value.binaryValue();
      assert bytes != null;
      assert !merge;
      if (bytes.length == 0) {
        return; // default
      }
      fill(docID, address);
      docToAddress[docID] = address;
      pool.copy(bytes);
      address += bytes.length;
      lastDocID = docID;
    }
    
    @Override
    protected void merge(DocValues readerIn, int docBase, int docCount, Bits liveDocs) throws IOException {
      merge = true;
      datOut = getOrCreateDataOut();
      boolean success = false;
      try {
        if (liveDocs == null && readerIn instanceof VarStraightReader) {
          // bulk merge since we don't have any deletes
          VarStraightReader reader = (VarStraightReader) readerIn;
          final int maxDocs = reader.maxDoc;
          if (maxDocs == 0) {
            return;
          }
          if (lastDocID+1 < docBase) {
            fill(docBase, address);
            lastDocID = docBase-1;
          }
          final long numDataBytes;
          final IndexInput cloneIdx = reader.cloneIndex();
          try {
            numDataBytes = cloneIdx.readVLong();
            final ReaderIterator iter = PackedInts.getReaderIterator(cloneIdx);
            for (int i = 0; i < maxDocs; i++) {
              long offset = iter.next();
              ++lastDocID;
              if (lastDocID >= docToAddress.length) {
                int oldSize = docToAddress.length;
                docToAddress = ArrayUtil.grow(docToAddress, 1 + lastDocID);
                bytesUsed.addAndGet((docToAddress.length - oldSize)
                    * RamUsageEstimator.NUM_BYTES_INT);
              }
              docToAddress[lastDocID] = address + offset;
            }
            address += numDataBytes; // this is the address after all addr pointers are updated
            iter.close();
          } finally {
            IOUtils.close(cloneIdx);
          }
          final IndexInput cloneData = reader.cloneData();
          try {
            datOut.copyBytes(cloneData, numDataBytes);
          } finally {
            IOUtils.close(cloneData);  
          }
        } else {
          super.merge(readerIn, docBase, docCount, liveDocs);
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(datOut);
        }
      }
    }
    
    @Override
    protected void mergeDoc(Field scratchField, Source source, int docID, int sourceDoc) throws IOException {
      assert merge;
      assert lastDocID < docID;
      source.getBytes(sourceDoc, bytesRef);
      if (bytesRef.length == 0) {
        return; // default
      }
      fill(docID, address);
      datOut.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
      docToAddress[docID] = address;
      address += bytesRef.length;
      lastDocID = docID;
    }
    

    @Override
    public void finish(int docCount) throws IOException {
      boolean success = false;
      assert (!merge && datOut == null) || (merge && datOut != null); 
      final IndexOutput datOut = getOrCreateDataOut();
      try {
        if (!merge) {
          // header is already written in getDataOut()
          pool.writePool(datOut);
        }
        success = true;
      } finally {
        if (success) {
          IOUtils.close(datOut);
        } else {
          IOUtils.closeWhileHandlingException(datOut);
        }
        pool.dropBuffersAndReset();
      }

      success = false;
      final IndexOutput idxOut = getOrCreateIndexOut();
      try {
        if (lastDocID == -1) {
          idxOut.writeVLong(0);
          final PackedInts.Writer w = PackedInts.getWriter(idxOut, docCount+1,
              PackedInts.bitsRequired(0));
          // docCount+1 so we write sentinel
          for (int i = 0; i < docCount+1; i++) {
            w.add(0);
          }
          w.finish();
        } else {
          fill(docCount, address);
          idxOut.writeVLong(address);
          final PackedInts.Writer w = PackedInts.getWriter(idxOut, docCount+1,
              PackedInts.bitsRequired(address));
          for (int i = 0; i < docCount; i++) {
            w.add(docToAddress[i]);
          }
          // write sentinel
          w.add(address);
          w.finish();
        }
        success = true;
      } finally {
        bytesUsed.addAndGet(-(docToAddress.length)
            * RamUsageEstimator.NUM_BYTES_INT);
        docToAddress = null;
        if (success) {
          IOUtils.close(idxOut);
        } else {
          IOUtils.closeWhileHandlingException(idxOut);
        }
      }
    }

    public long ramBytesUsed() {
      return bytesUsed.get();
    }
  }

  public static class VarStraightReader extends BytesReaderBase {
    final int maxDoc;

    VarStraightReader(Directory dir, String id, int maxDoc, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true, context, Type.BYTES_VAR_STRAIGHT);
      this.maxDoc = maxDoc;
    }

    @Override
    public Source load() throws IOException {
      return new VarStraightSource(cloneData(), cloneIndex());
    }

    @Override
    public Source getDirectSource()
        throws IOException {
      return new DirectVarStraightSource(cloneData(), cloneIndex(), getType());
    }
  }
  
  private static final class VarStraightSource extends BytesSourceBase {
    private final PackedInts.Reader addresses;

    public VarStraightSource(IndexInput datIn, IndexInput idxIn) throws IOException {
      super(datIn, idxIn, new PagedBytes(PAGED_BYTES_BITS), idxIn.readVLong(),
          Type.BYTES_VAR_STRAIGHT);
      addresses = PackedInts.getReader(idxIn);
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      final long address = addresses.get(docID);
      return data.fillSlice(bytesRef, address,
          (int) (addresses.get(docID + 1) - address));
    }
  }
  
  public final static class DirectVarStraightSource extends DirectSource {

    private final PackedInts.Reader index;

    DirectVarStraightSource(IndexInput data, IndexInput index, Type type)
        throws IOException {
      super(data, type);
      index.readVLong();
      this.index = PackedInts.getDirectReader(index);
    }

    @Override
    protected int position(int docID) throws IOException {
      final long offset = index.get(docID);
      data.seek(baseOffset + offset);
      // Safe to do 1+docID because we write sentinel at the end:
      final long nextOffset = index.get(1+docID);
      return (int) (nextOffset - offset);
    }
  }
}
