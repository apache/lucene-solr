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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ByteBlockPool.Allocator;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;
import org.apache.lucene.util.packed.PackedInts;

// Stores fixed-length byte[] by deref, ie when two docs
// have the same value, they store only 1 byte[]

class FixedDerefBytesImpl {

  static final String CODEC_NAME = "FixedDerefBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class Writer extends BytesWriterBase {
    private int size = -1;
    private int[] docToID;
    private final BytesRefHash hash = new BytesRefHash(pool);
    
    public Writer(Directory dir, String id) throws IOException {
      this(dir, id, new DirectAllocator(ByteBlockPool.BYTE_BLOCK_SIZE),
          new AtomicLong());
    }

    public Writer(Directory dir, String id, Allocator allocator,
        AtomicLong bytesUsed) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, false, false,
          new ByteBlockPool(allocator), bytesUsed);
      docToID = new int[1];
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT);
    }

    @Override
    synchronized public void add(int docID, BytesRef bytes) throws IOException {
      if(bytes.length == 0) // default value - skip it
        return;
      if (size == -1) {
        size = bytes.length;
        initDataOut();
        datOut.writeInt(size);
      } else if (bytes.length != size) {
        throw new IllegalArgumentException("expected bytes size=" + size
            + " but got " + bytes.length);
      }
      int ord = hash.add(bytes);

      if (ord >= 0) {
        // new added entry
        datOut.writeBytes(bytes.bytes, bytes.offset, bytes.length);
      } else {
        ord = (-ord)-1;
      }

      if (docID >= docToID.length) {
        int size = docToID.length;
        docToID = ArrayUtil.grow(docToID, 1 + docID);
        bytesUsed.addAndGet((docToID.length - size) * RamUsageEstimator.NUM_BYTES_INT);
      }
      docToID[docID] = 1+ord;
    }


    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    synchronized public void finish(int docCount) throws IOException {
      if (datOut == null) // no added data
        return;
      initIndexOut();
      final int count = 1+hash.size();
      idxOut.writeInt(count - 1);
      // write index
      final PackedInts.Writer w = PackedInts.getWriter(idxOut, docCount,
          PackedInts.bitsRequired(count - 1));
      final int limit = docCount > docToID.length ? docToID.length : docCount;
      for (int i = 0; i < limit; i++) {
        w.add(docToID[i]);
      }
      // fill up remaining doc with zeros
      for (int i = limit; i < docCount; i++) {
        w.add(0);
      }
      w.finish();
      hash.clear();

      super.finish(docCount);
    }
  }

  public static class Reader extends BytesReaderBase {
    private final int size;

    Reader(Directory dir, String id, int maxDoc) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true);
      try {
        size = datIn.readInt();
      } catch (IOException e) {
        throw e;
      }
    }

    @Override
    public Source load() throws IOException {
      final IndexInput index = cloneIndex();
      return new Source(cloneData(), index , size, index.readInt());
    }

    private static class Source extends BytesBaseSource {
      private final BytesRef bytesRef = new BytesRef();
      private final PackedInts.Reader index;
      private final int size;
      private final int numValues;

      protected Source(IndexInput datIn, IndexInput idxIn, int size, int numValues)
          throws IOException {
        super(datIn, idxIn, new PagedBytes(PAGED_BYTES_BITS), size * numValues);
        this.size = size;
        this.numValues = numValues;
        index = PackedInts.getReader(idxIn);
      }

      @Override
      public BytesRef getBytes(int docID) {
        final int id = (int) index.get(docID);
        if (id == 0) {
          return defaultValue;
        }
        return data.fill(bytesRef, ((id - 1) * size), size);
      }

      
      @Override
      public int getValueCount() {
        return numValues;
      }
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      return new DerefBytesEnum(source, cloneData(), cloneIndex(), CODEC_NAME,
          size);
    }

    static class DerefBytesEnum extends ValuesEnum {
      protected final IndexInput datIn;
      private final PackedInts.ReaderIterator idx;
      protected final long fp;
      private final int size;
      protected final BytesRef ref;
      private final int valueCount;
      private int pos = -1;

      public DerefBytesEnum(AttributeSource source, IndexInput datIn,
          IndexInput idxIn, String codecName, int size) throws IOException {
        this(source, datIn, idxIn, codecName, size, Values.BYTES_FIXED_DEREF);
      }

      protected DerefBytesEnum(AttributeSource source, IndexInput datIn,
          IndexInput idxIn, String codecName, int size, Values enumType)
          throws IOException {
        super(source, enumType);
        ref = attr.bytes();
        this.datIn = datIn;
        this.size = size == -1 ? 128 : size;
        idxIn.readInt();// read valueCount
        idx = PackedInts.getReaderIterator(idxIn);
        fp = datIn.getFilePointer();
        ref.grow(this.size);
        ref.length = this.size;
        ref.offset = 0;
        valueCount = idx.size();
      }

      @Override
      public int advance(int target) throws IOException {
        if (target < valueCount) {
          final long address = idx.advance(target);
          pos = idx.ord();
          if(address == 0) {
            // default is empty
            ref.length = 0;
            ref.offset = 0;
            return pos;
          }
          fill(address, ref);
          return pos;
        }
        return pos = NO_MORE_DOCS;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(pos + 1);
      }

      public void close() throws IOException {
        datIn.close();
        idx.close();
      }

      protected void fill(long address, BytesRef ref) throws IOException {
        datIn.seek(fp + ((address - 1) * size));
        datIn.readBytes(ref.bytes, 0, size);
        ref.length = size;
        ref.offset = 0;
      }

      @Override
      public int docID() {
        return pos;
      }

    }

    @Override
    public Values type() {
      return Values.BYTES_FIXED_DEREF;
    }
  }

}
