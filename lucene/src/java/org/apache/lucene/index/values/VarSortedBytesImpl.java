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
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.values.Bytes.BytesBaseSortedSource;
import org.apache.lucene.index.values.Bytes.BytesReaderBase;
import org.apache.lucene.index.values.Bytes.BytesWriterBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ByteBlockPool.Allocator;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;
import org.apache.lucene.util.packed.PackedInts;

// Stores variable-length byte[] by deref, ie when two docs
// have the same value, they store only 1 byte[] and both
// docs reference that single source

class VarSortedBytesImpl {

  static final String CODEC_NAME = "VarDerefBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class Writer extends BytesWriterBase {
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
      this.comp = comp;
      docToEntry = new int[1];
      docToEntry[0] = -1;
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT);

    }

    @Override
    synchronized public void add(int docID, BytesRef bytes) throws IOException {
      if (bytes.length == 0)
        return;// default
      if (docID >= docToEntry.length) {
        int[] newArray = new int[ArrayUtil.oversize(1 + docID,
            RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(docToEntry, 0, newArray, 0, docToEntry.length);
        Arrays.fill(newArray, docToEntry.length, newArray.length, -1);
        bytesUsed.addAndGet((newArray.length - docToEntry.length)
            * RamUsageEstimator.NUM_BYTES_INT);
        docToEntry = newArray;
      }
      final int e = hash.add(bytes);
      docToEntry[docID] = e < 0 ? (-e) - 1 : e;
    }

    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    synchronized public void finish(int docCount) throws IOException {
      final int count = hash.size();
      if (count == 0)
        return;
      initIndexOut();
      initDataOut();
      int[] sortedEntries = hash.sort(comp);

      // first dump bytes data, recording index & offset as
      // we go
      long offset = 0;
      long lastOffset = 0;
      final int[] index = new int[count];
      final long[] offsets = new long[count];
      for (int i = 0; i < count; i++) {
        final int e = sortedEntries[i];
        offsets[i] = offset;
        index[e] = 1 + i;

        final BytesRef bytes = hash.get(e, new BytesRef());
        // TODO: we could prefix code...
        datOut.writeBytes(bytes.bytes, bytes.offset, bytes.length);
        lastOffset = offset;
        offset += bytes.length;
      }

      // total bytes of data
      idxOut.writeLong(offset);

      // write index -- first doc -> 1+ord
      // nocommit -- allow not -1:
      final PackedInts.Writer indexWriter = PackedInts.getWriter(idxOut,
          docCount, PackedInts.bitsRequired(count));
      final int limit = docCount > docToEntry.length ? docToEntry.length
          : docCount;
      for (int i = 0; i < limit; i++) {
        final int e = docToEntry[i];
        indexWriter.add(e == -1 ? 0 : index[e]);
      }
      for (int i = limit; i < docCount; i++) {
        indexWriter.add(0);
      }
      indexWriter.finish();

      // next ord (0-based) -> offset
      // nocommit -- allow not -1:
      PackedInts.Writer offsetWriter = PackedInts.getWriter(idxOut, count,
          PackedInts.bitsRequired(lastOffset));
      for (int i = 0; i < count; i++) {
        offsetWriter.add(offsets[i]);
      }
      offsetWriter.finish();

      super.finish(docCount);
      bytesUsed.addAndGet((-docToEntry.length)
          * RamUsageEstimator.NUM_BYTES_INT);

    }
  }

  public static class Reader extends BytesReaderBase {

    Reader(Directory dir, String id, int maxDoc) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true);
    }

    @Override
    public org.apache.lucene.index.values.DocValues.Source load()
        throws IOException {
      return loadSorted(null);
    }

    @Override
    public SortedSource loadSorted(Comparator<BytesRef> comp)
        throws IOException {
      return new Source(cloneData(), cloneIndex(), comp);
    }

    private static class Source extends BytesBaseSortedSource {
      // TODO: paged data
      private final byte[] data;
      private final BytesRef bytesRef = new BytesRef();
      private final PackedInts.Reader docToOrdIndex;
      private final PackedInts.Reader ordToOffsetIndex; // 0-based
      private final long totBytes;
      private final int valueCount;
      private final LookupResult lookupResult = new LookupResult();
      private final Comparator<BytesRef> comp;

      public Source(IndexInput datIn, IndexInput idxIn,
          Comparator<BytesRef> comp) throws IOException {
        super(datIn, idxIn);
        totBytes = idxIn.readLong();
        data = new byte[(int) totBytes];
        datIn.readBytes(data, 0, (int) totBytes);
        docToOrdIndex = PackedInts.getReader(idxIn);
        ordToOffsetIndex = PackedInts.getReader(idxIn);
        valueCount = ordToOffsetIndex.size();
        bytesRef.bytes = data;
        // default byte sort order
        this.comp = comp == null ? BytesRef.getUTF8SortedAsUnicodeComparator()
            : comp;

      }

      @Override
      public BytesRef getByOrd(int ord) {
        return ord == 0 ? defaultValue : deref(--ord);
      }

      @Override
      public int ord(int docID) {
        return (int) docToOrdIndex.get(docID);
      }

      @Override
      public LookupResult getByValue(BytesRef bytes) {
        return binarySearch(bytes, 0, valueCount - 1);
      }

      public long ramBytesUsed() {
        // TODO(simonw): move ram usage to PackedInts?
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
            + data.length
            + (RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + docToOrdIndex
                .getBitsPerValue()
                * docToOrdIndex.getBitsPerValue())
            + (RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + ordToOffsetIndex
                .getBitsPerValue()
                * ordToOffsetIndex.getBitsPerValue());
      }

      @Override
      public int getValueCount() {
        return valueCount;
      }

      // ord is 0-based
      private BytesRef deref(int ord) {
        bytesRef.offset = (int) ordToOffsetIndex.get(ord);
        final long nextOffset;
        if (ord == valueCount - 1) {
          nextOffset = totBytes;
        } else {
          nextOffset = ordToOffsetIndex.get(1 + ord);
        }
        bytesRef.length = (int) (nextOffset - bytesRef.offset);
        return bytesRef;
      }

      // TODO: share w/ FixedSortedBytesValues?
      private LookupResult binarySearch(BytesRef b, int low, int high) {

        while (low <= high) {
          int mid = (low + high) >>> 1;
          deref(mid);
          final int cmp = comp.compare(bytesRef, b);
          if (cmp < 0) {
            low = mid + 1;
          } else if (cmp > 0) {
            high = mid - 1;
          } else {
            lookupResult.ord = mid + 1;
            lookupResult.found = true;
            return lookupResult;
          }
        }
        assert comp.compare(bytesRef, b) != 0;
        lookupResult.ord = low;
        lookupResult.found = false;
        return lookupResult;
      }
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      return new VarSortedBytesEnum(source, cloneData(), cloneIndex());
    }

    private static class VarSortedBytesEnum extends ValuesEnum {

      private PackedInts.Reader docToOrdIndex;
      private PackedInts.Reader ordToOffsetIndex;
      private IndexInput idxIn;
      private IndexInput datIn;
      private final BytesRef bytesRef;
      private int valueCount;
      private long totBytes;
      private int docCount;
      private int pos = -1;
      private final long fp;

      protected VarSortedBytesEnum(AttributeSource source, IndexInput datIn,
          IndexInput idxIn) throws IOException {
        super(source, Values.BYTES_VAR_SORTED);
        bytesRef = attr.bytes();
        totBytes = idxIn.readLong();
        // keep that in memory to prevent lots of disk seeks
        docToOrdIndex = PackedInts.getReader(idxIn);
        ordToOffsetIndex = PackedInts.getReader(idxIn);
        valueCount = ordToOffsetIndex.size();
        docCount = docToOrdIndex.size();
        fp = datIn.getFilePointer();
        this.idxIn = idxIn;
        this.datIn = datIn;
      }

      @Override
      public void close() throws IOException {
        idxIn.close();
        datIn.close();
      }

      @Override
      public int advance(int target) throws IOException {
        if (target >= docCount)
          return pos = NO_MORE_DOCS;
        final int ord = (int) docToOrdIndex.get(target) - 1;
        if (ord == -1) {
          bytesRef.length = 0;
          bytesRef.offset = 0;
          return pos = target;
        }
        final long offset = ordToOffsetIndex.get(ord);
        final long nextOffset;
        if (ord == valueCount - 1) {
          nextOffset = totBytes;
        } else {
          nextOffset = ordToOffsetIndex.get(1 + ord);
        }
        final int length = (int) (nextOffset - offset);
        datIn.seek(fp + offset);
        if (bytesRef.bytes.length < length)
          bytesRef.grow(length);
        datIn.readBytes(bytesRef.bytes, 0, length);
        bytesRef.length = length;
        bytesRef.offset = 0;
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
    public Values type() {
      return Values.BYTES_VAR_SORTED;
    }
  }
}
