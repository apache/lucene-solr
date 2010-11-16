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
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.values.Bytes.BytesBaseSortedSource;
import org.apache.lucene.index.values.Bytes.BytesReaderBase;
import org.apache.lucene.index.values.Bytes.BytesWriterBase;
import org.apache.lucene.index.values.FixedDerefBytesImpl.Reader.DerefBytesEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ByteBlockPool.Allocator;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;
import org.apache.lucene.util.packed.PackedInts;

// Stores fixed-length byte[] by deref, ie when two docs
// have the same value, they store only 1 byte[]

class FixedSortedBytesImpl {

  static final String CODEC_NAME = "FixedSortedBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class Writer extends BytesWriterBase {
    private int size = -1;
    private int[] docToEntry;
    private final Comparator<BytesRef> comp;

    private final BytesRefHash hash = new BytesRefHash(pool);

    public Writer(Directory dir, String id, Comparator<BytesRef> comp)
        throws IOException {
      this(dir, id, comp, new DirectAllocator(ByteBlockPool.BYTE_BLOCK_SIZE),
          new AtomicLong());
    }

    public Writer(Directory dir, String id, Comparator<BytesRef> comp,
        Allocator allocator, AtomicLong bytesUsed) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, false, false,
          new ByteBlockPool(allocator), bytesUsed);
      docToEntry = new int[1];
      // docToEntry[0] = -1;
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT);
      this.comp = comp;
    }

    @Override
    synchronized public void add(int docID, BytesRef bytes) throws IOException {
      if (bytes.length == 0)
        return; // default - skip it
      if (size == -1) {
        size = bytes.length;
        initDataOut();
        datOut.writeInt(size);
      } else if (bytes.length != size) {
        throw new IllegalArgumentException("expected bytes size=" + size
            + " but got " + bytes.length);
      }
      if (docID >= docToEntry.length) {
        int[] newArray = new int[ArrayUtil.oversize(1 + docID,
            RamUsageEstimator.NUM_BYTES_INT)];
        System.arraycopy(docToEntry, 0, newArray, 0, docToEntry.length);
        // Arrays.fill(newArray, docToEntry.length, newArray.length, -1);

        bytesUsed.addAndGet((newArray.length - docToEntry.length)
            * RamUsageEstimator.NUM_BYTES_INT);
        docToEntry = newArray;
      }
      int e = hash.add(bytes);
      docToEntry[docID] = 1 + (e < 0 ? (-e) - 1 : e);
    }

    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    synchronized public void finish(int docCount) throws IOException {
      if (datOut == null)// no data added
        return;
      initIndexOut();
      final int[] sortedEntries = hash.sort(comp);
      final int count = hash.size();
      int[] address = new int[count];
      // first dump bytes data, recording address as we go
      for (int i = 0; i < count; i++) {
        final int e = sortedEntries[i];
        final BytesRef bytes = hash.get(e, new BytesRef());
        assert bytes.length == size;
        datOut.writeBytes(bytes.bytes, bytes.offset, bytes.length);
        address[e] = 1 + i;
      }

      idxOut.writeInt(count);

      // next write index
      PackedInts.Writer w = PackedInts.getWriter(idxOut, docCount, PackedInts
          .bitsRequired(count));
      final int limit;
      if (docCount > docToEntry.length) {
        limit = docToEntry.length;
      } else {
        limit = docCount;
      }
      for (int i = 0; i < limit; i++) {
        final int e = docToEntry[i];
        if (e == 0) {
          // null is encoded as zero
          w.add(0);
        } else {
          assert e > 0 && e <= count : "index must  0 > && <= " + count
              + " was: " + e;
          w.add(address[e - 1]);
        }
      }

      for (int i = limit; i < docCount; i++) {
        w.add(0);
      }
      w.finish();

      super.finish(docCount);
      bytesUsed.addAndGet((-docToEntry.length)
          * RamUsageEstimator.NUM_BYTES_INT);
      docToEntry = null;
    }
  }

  public static class Reader extends BytesReaderBase {
    private final int size;

    public Reader(Directory dir, String id, int maxDoc) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true);
      size = datIn.readInt();
    }

    @Override
    public org.apache.lucene.index.values.DocValues.Source load()
        throws IOException {
      return loadSorted(null);
    }

    @Override
    public SortedSource loadSorted(Comparator<BytesRef> comp)
        throws IOException {
      final IndexInput idxInput = cloneIndex();
      final IndexInput datInput = cloneData();
      datInput.seek(CodecUtil.headerLength(CODEC_NAME) + 4);
      idxInput.seek(CodecUtil.headerLength(CODEC_NAME));
      return new Source(datInput, idxInput, size, idxInput.readInt(), comp);
    }

    private static class Source extends BytesBaseSortedSource {

      private final PackedInts.Reader index;
      private final int numValue;
      private final int size;

      public Source(IndexInput datIn, IndexInput idxIn, int size, int numValues,
          Comparator<BytesRef> comp) throws IOException {
        super(datIn, idxIn, comp, new PagedBytes(PAGED_BYTES_BITS), size*numValues );
        this.size = size;
        this.numValue = numValues;
        index = PackedInts.getReader(idxIn);

        bytesRef.length = size;
      }

      @Override
      public int ord(int docID) {
        return (int) index.get(docID);
      }

      @Override
      public LookupResult getByValue(BytesRef bytes) {
        return binarySearch(bytes, 0, numValue - 1);
      }

      public long ramBytesUsed() {
        // TODO(simonw): move ram calcultation to PackedInts?
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
            + size
            * numValue
            + (RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + index
                .getBitsPerValue()
                * index.size());
      }

      @Override
      public int getValueCount() {
        return numValue;
      }
      @Override
      protected BytesRef deref(int ord) {
        return data.fill(bytesRef, (ord* size), size);
      }
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      // do unsorted
      return new DerefBytesEnum(source, cloneData(), cloneIndex(), CODEC_NAME,
          size);
    }

    @Override
    public Values type() {
      return Values.BYTES_FIXED_SORTED;
    }
  }
}
