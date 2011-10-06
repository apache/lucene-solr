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

import org.apache.lucene.index.values.Bytes.BytesSortedSourceBase;
import org.apache.lucene.index.values.Bytes.BytesReaderBase;
import org.apache.lucene.index.values.Bytes.DerefBytesWriterBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.Direct64;
import org.apache.lucene.util.packed.PackedInts;

// Stores variable-length byte[] by deref, ie when two docs
// have the same value, they store only 1 byte[] and both
// docs reference that single source

/**
 * @lucene.experimental
 */
class VarSortedBytesImpl {

  static final String CODEC_NAME = "VarDerefBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  final static class Writer extends DerefBytesWriterBase {
    private final Comparator<BytesRef> comp;

    public Writer(Directory dir, String id, Comparator<BytesRef> comp,
        Counter bytesUsed, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context);
      this.comp = comp;
    }
    
    @Override
    protected void checkSize(BytesRef bytes) {
      // allow var bytes sizes
    }

    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    public void finishInternal(int docCount) throws IOException {
      final int count = hash.size();
      final IndexOutput datOut = getOrCreateDataOut();
      long offset = 0;
      long lastOffset = 0;
      final int[] index = new int[count+1];
      final long[] offsets = new long[count];
      final int[] sortedEntries = hash.sort(comp);
      // first dump bytes data, recording index & offset as
      // we go
      for (int i = 0; i < count; i++) {
        final int e = sortedEntries[i];
        offsets[i] = offset;
        index[e+1] = 1 + i;

        final BytesRef bytes = hash.get(e, new BytesRef());
        // TODO: we could prefix code...
        datOut.writeBytes(bytes.bytes, bytes.offset, bytes.length);
        lastOffset = offset;
        offset += bytes.length;
      }

      final IndexOutput idxOut = getOrCreateIndexOut();
      // total bytes of data
      idxOut.writeLong(offset);
      // write index -- first doc -> 1+ord
      writeIndex(idxOut, docCount, count, index, docToEntry);
      // next ord (0-based) -> offset
      PackedInts.Writer offsetWriter = PackedInts.getWriter(idxOut, count,
          PackedInts.bitsRequired(lastOffset));
      for (int i = 0; i < count; i++) {
        offsetWriter.add(offsets[i]);
      }
      offsetWriter.finish();
    }
  }

  public static class Reader extends BytesReaderBase {

    private final Comparator<BytesRef> defaultComp;

    Reader(Directory dir, String id, int maxDoc,
        Comparator<BytesRef> comparator, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true, context);
      this.defaultComp = comparator;
    }

    @Override
    public org.apache.lucene.index.values.IndexDocValues.Source load()
        throws IOException {
      return loadSorted(defaultComp);
    }

    @Override
    public SortedSource loadSorted(Comparator<BytesRef> comp)
        throws IOException {
      IndexInput indexIn = cloneIndex();
      return new Source(cloneData(), indexIn, comp, indexIn.readLong());
    }

    private static class Source extends BytesSortedSourceBase {
      private final PackedInts.Reader ordToOffsetIndex; // 0-based
      private final long totBytes;
      private final int valueCount;

      public Source(IndexInput datIn, IndexInput idxIn,
          Comparator<BytesRef> comp, long dataLength) throws IOException {
        super(datIn, idxIn, comp, dataLength, ValueType.BYTES_VAR_SORTED);
        totBytes = dataLength;
        ordToOffsetIndex = PackedInts.getReader(idxIn);
        valueCount = ordToOffsetIndex.size();
        closeIndexInput();
      }

      @Override
      public int getByValue(BytesRef bytes, BytesRef tmpRef) {
        return binarySearch(bytes, tmpRef, 0, valueCount - 1);
      }
      
      @Override
      public int getValueCount() {
        return valueCount;
      }

      // ord is 0-based
      @Override
      protected BytesRef deref(int ord, BytesRef bytesRef) {
        final long nextOffset;
        if (ord == valueCount - 1) {
          nextOffset = totBytes;
        } else {
          nextOffset = ordToOffsetIndex.get(1 + ord);
        }
        final long offset = ordToOffsetIndex.get(ord);
        data.fillSlice(bytesRef, offset, (int) (nextOffset - offset));
        return bytesRef;
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
      private int valueCount;
      private long totBytes;
      private int docCount;
      private int pos = -1;
      private final long fp;

      protected VarSortedBytesEnum(AttributeSource source, IndexInput datIn,
          IndexInput idxIn) throws IOException {
        super(source, ValueType.BYTES_VAR_SORTED);
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
        if (target >= docCount) {
          return pos = NO_MORE_DOCS;
        }
        int ord;
        while ((ord = (int) docToOrdIndex.get(target)) == 0) {
          if (++target >= docCount) {
            return pos = NO_MORE_DOCS;
          }
        }
        final long offset = ordToOffsetIndex.get(--ord);
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
        if (pos >= docCount) {
          return pos = NO_MORE_DOCS;
        }
        return advance(pos + 1);
      }
    }

    @Override
    public ValueType type() {
      return ValueType.BYTES_VAR_SORTED;
    }
  }
}
