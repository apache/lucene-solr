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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.values.Bytes.BytesBaseSource;
import org.apache.lucene.index.values.Bytes.BytesReaderBase;
import org.apache.lucene.index.values.Bytes.BytesWriterBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ByteBlockPool.DirectTrackingAllocator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.ReaderIterator;

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
    public Writer(Directory dir, String id, AtomicLong bytesUsed)
        throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed);
      pool = new ByteBlockPool(new DirectTrackingAllocator(bytesUsed));
      docToAddress = new long[1];
      pool.nextBuffer(); // init
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT);
    }

    // Fills up to but not including this docID
    private void fill(final int docID) {
      if (docID >= docToAddress.length) {
        int oldSize = docToAddress.length;
        docToAddress = ArrayUtil.grow(docToAddress, 1 + docID);
        bytesUsed.addAndGet((docToAddress.length - oldSize)
            * RamUsageEstimator.NUM_BYTES_INT);
      }
      for (int i = lastDocID + 1; i < docID; i++) {
        docToAddress[i] = address;
      }
    }

    @Override
    public void add(int docID, BytesRef bytes) throws IOException {
      assert !merge;
      if (bytes.length == 0) {
        return; // default
      }
      fill(docID);
      docToAddress[docID] = address;
      pool.copy(bytes);
      address += bytes.length;
      lastDocID = docID;
    }
    
    @Override
    protected void merge(MergeState state) throws IOException {
      merge = true;
      datOut = getDataOut();
      boolean success = false;
      try {
        if (state.liveDocs == null && state.reader instanceof Reader) {
          // bulk merge since we don't have any deletes
          Reader reader = (Reader) state.reader;
          final int maxDocs = reader.maxDoc;
          if (maxDocs == 0) {
            return;
          }
          if (lastDocID+1 < state.docBase) {
            fill(state.docBase);
            lastDocID = state.docBase-1;
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
            IOUtils.closeSafely(true, cloneIdx);
          }
          final IndexInput cloneData = reader.cloneData();
          try {
            datOut.copyBytes(cloneData, numDataBytes);
          } finally {
            IOUtils.closeSafely(true, cloneData);  
          }
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
      assert merge;
      assert lastDocID < docID;
      if (bytesRef.length == 0) {
        return; // default
      }
      fill(docID);
      datOut.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
      docToAddress[docID] = address;
      address += bytesRef.length;
      lastDocID = docID;
    }
    

    @Override
    public void finish(int docCount) throws IOException {
      boolean success = false;
      assert (!merge && datOut == null) || (merge && datOut != null); 
      final IndexOutput datOut = getDataOut();
      try {
        if (!merge) {
          // header is already written in getDataOut()
          pool.writePool(datOut);
        }
        success = true;
      } finally {
        IOUtils.closeSafely(!success, datOut); 
        pool.dropBuffersAndReset();
      }

      success = false;
      final IndexOutput idxOut = getIndexOut();
      try {
        if (lastDocID == -1) {
          idxOut.writeVLong(0);
          final PackedInts.Writer w = PackedInts.getWriter(idxOut, docCount,
              PackedInts.bitsRequired(0));
          for (int i = 0; i < docCount; i++) {
            w.add(0);
          }
          w.finish();
        } else {
          fill(docCount);
          idxOut.writeVLong(address);
          final PackedInts.Writer w = PackedInts.getWriter(idxOut, docCount,
              PackedInts.bitsRequired(address));
          for (int i = 0; i < docCount; i++) {
            w.add(docToAddress[i]);
          }
          w.finish();
        }
        success = true;
      } finally {
        bytesUsed.addAndGet(-(docToAddress.length)
            * RamUsageEstimator.NUM_BYTES_INT);
        docToAddress = null;
        IOUtils.closeSafely(!success, idxOut);
      }
    }

    public long ramBytesUsed() {
      return bytesUsed.get();
    }
  }

  public static class Reader extends BytesReaderBase {
    private final int maxDoc;

    Reader(Directory dir, String id, int maxDoc) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true);
      this.maxDoc = maxDoc;
    }

    @Override
    public Source load() throws IOException {
      return new Source(cloneData(), cloneIndex());
    }

    private class Source extends BytesBaseSource {
      private final PackedInts.Reader addresses;

      public Source(IndexInput datIn, IndexInput idxIn) throws IOException {
        super(datIn, idxIn, new PagedBytes(PAGED_BYTES_BITS), idxIn.readVLong());
        addresses = PackedInts.getReader(idxIn);
      }

      @Override
      public BytesRef getBytes(int docID, BytesRef bytesRef) {
        final long address = addresses.get(docID);
        final int length = docID == maxDoc - 1 ? (int) (totalLengthInBytes - address)
            : (int) (addresses.get(1 + docID) - address);
        return data.fillSlice(bytesRef, address, length);
      }
      
      @Override
      public ValuesEnum getEnum(AttributeSource attrSource) throws IOException {
        return new SourceEnum(attrSource, type(), this, maxDoc()) {
          @Override
          public int advance(int target) throws IOException {
            if (target >= numDocs) {
              return pos = NO_MORE_DOCS;
            }
            source.getBytes(target, bytesRef);
            return pos = target;
          }
        };
      }

      @Override
      public int getValueCount() {
        throw new UnsupportedOperationException();
      }

      @Override
      public ValueType type() {
        return ValueType.BYTES_VAR_STRAIGHT;
      }

      @Override
      protected int maxDoc() {
        return addresses.size();
      }
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      return new VarStraightBytesEnum(source, cloneData(), cloneIndex());
    }

    private class VarStraightBytesEnum extends ValuesEnum {
      private final PackedInts.ReaderIterator addresses;
      private final IndexInput datIn;
      private final IndexInput idxIn;
      private final long fp;
      private final long totBytes;
      private int pos = -1;
      private long nextAddress;

      protected VarStraightBytesEnum(AttributeSource source, IndexInput datIn,
          IndexInput idxIn) throws IOException {
        super(source, ValueType.BYTES_VAR_STRAIGHT);
        totBytes = idxIn.readVLong();
        fp = datIn.getFilePointer();
        addresses = PackedInts.getReaderIterator(idxIn);
        this.datIn = datIn;
        this.idxIn = idxIn;
        nextAddress = addresses.next();
      }

      @Override
      public void close() throws IOException {
        datIn.close();
        idxIn.close();
      }

      @Override
      public int advance(final int target) throws IOException {
        if (target >= maxDoc) {
          return pos = NO_MORE_DOCS;
        }
        final long addr = pos+1 == target ? nextAddress : addresses.advance(target);
        if (addr == totBytes) { // empty values at the end
          bytesRef.length = 0;
          bytesRef.offset = 0;
          return pos = target;
        }
        datIn.seek(fp + addr);
        final int size = (int) (target == maxDoc - 1 ? totBytes - addr
            : (nextAddress = addresses.next()) - addr);
        if (bytesRef.bytes.length < size) {
          bytesRef.grow(size);
        }
        bytesRef.length = size;
        datIn.readBytes(bytesRef.bytes, 0, size);
        return pos = target;
      }

      @Override
      public int docID() {
        return pos;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(pos + 1);
      }
    }

    @Override
    public ValueType type() {
      return ValueType.BYTES_VAR_STRAIGHT;
    }
  }
}
