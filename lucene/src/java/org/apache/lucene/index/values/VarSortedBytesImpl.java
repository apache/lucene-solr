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
import org.apache.lucene.index.values.IndexDocValues.SortedSource;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;

// Stores variable-length byte[] by deref, ie when two docs
// have the same value, they store only 1 byte[] and both
// docs reference that single source

/**
 * @lucene.experimental
 */
final class VarSortedBytesImpl {

  static final String CODEC_NAME = "VarDerefBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  final static class Writer extends DerefBytesWriterBase {
    private final Comparator<BytesRef> comp;

    public Writer(Directory dir, String id, Comparator<BytesRef> comp,
        Counter bytesUsed, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context);
      this.comp = comp;
      size = 0;
    }

    @Override
    protected void checkSize(BytesRef bytes) {
      // allow var bytes sizes
    }

    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    public void finishInternal(int docCount) throws IOException {
      fillDefault(docCount);
      final int count = hash.size();
      final IndexOutput datOut = getOrCreateDataOut();
      long offset = 0;
      final int[] index = new int[count];
      final long[] offsets = new long[count];
      final int[] sortedEntries = hash.sort(comp);
      // first dump bytes data, recording index & offset as
      // we go
      for (int i = 0; i < count; i++) {
        final int e = sortedEntries[i];
        offsets[i] = offset;
        index[e] = i;

        final BytesRef bytes = hash.get(e, new BytesRef());
        // TODO: we could prefix code...
        datOut.writeBytes(bytes.bytes, bytes.offset, bytes.length);
        offset += bytes.length;
      }
      final IndexOutput idxOut = getOrCreateIndexOut();
      // total bytes of data
      idxOut.writeLong(offset);
      // write index
      writeIndex(idxOut, docCount, count, index, docToEntry);
      // next ord (0-based) -> offset
      PackedInts.Writer offsetWriter = PackedInts.getWriter(idxOut, count+1,
          PackedInts.bitsRequired(offset));
      for (int i = 0; i < count; i++) {
        offsetWriter.add(offsets[i]);
      }
      offsetWriter.add(offset);
      offsetWriter.finish();
    }
  }

  public static class Reader extends BytesReaderBase {

    private final Comparator<BytesRef> comparator;

    Reader(Directory dir, String id, int maxDoc,
        IOContext context, ValueType type, Comparator<BytesRef> comparator)
        throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true, context, type);
      this.comparator = comparator;
    }

    @Override
    public org.apache.lucene.index.values.IndexDocValues.Source load()
        throws IOException {
      return new VarSortedSource(cloneData(), cloneIndex(), comparator);
    }

    @Override
    public Source getDirectSource() throws IOException {
      return new DirectSortedSource(cloneData(), cloneIndex(), comparator, type());
    }
    
  }
  private static final class VarSortedSource extends BytesSortedSourceBase {
    private final PackedInts.Reader ordToOffsetIndex; // 0-based
    private final int valueCount;

    VarSortedSource(IndexInput datIn, IndexInput idxIn,
        Comparator<BytesRef> comp) throws IOException {
      super(datIn, idxIn, comp, idxIn.readLong(), ValueType.BYTES_VAR_SORTED);
      ordToOffsetIndex = PackedInts.getReader(idxIn);
      valueCount = ordToOffsetIndex.size()-1; // the last value here is just a dummy value to get the length of the last value
      closeIndexInput();
    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      final long offset = ordToOffsetIndex.get(ord);
      final long nextOffset = ordToOffsetIndex.get(1 + ord);
      data.fillSlice(bytesRef, offset, (int) (nextOffset - offset));
      return bytesRef;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }
  }

  private static final class DirectSortedSource extends SortedSource {
    private final PackedInts.Reader docToOrdIndex;
    private final PackedInts.RandomAccessReaderIterator ordToOffsetIndex;
    private final IndexInput datIn;
    private final long basePointer;
    private final int valueCount;
    
    DirectSortedSource(IndexInput datIn, IndexInput idxIn,
        Comparator<BytesRef> comparator, ValueType type) throws IOException {
      super(type, comparator);
      idxIn.readLong();
      docToOrdIndex = PackedInts.getReader(idxIn); // read the ords in to prevent too many random disk seeks
      ordToOffsetIndex = PackedInts.getRandomAccessReaderIterator(idxIn);
      valueCount = ordToOffsetIndex.size()-1; // the last value here is just a dummy value to get the length of the last value
      basePointer = datIn.getFilePointer();
      this.datIn = datIn;
    }

    @Override
    public int ord(int docID) {
      return (int) docToOrdIndex.get(docID);
    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      try {
        final long offset = ordToOffsetIndex.get(ord);
        final long nextOffset = ordToOffsetIndex.next();
        datIn.seek(basePointer + offset);
        final int length = (int) (nextOffset - offset);
        if (bytesRef.bytes.length < length) {
          bytesRef.grow(length);
        }
        datIn.readBytes(bytesRef.bytes, 0, length);
        bytesRef.length = length;
        bytesRef.offset = 0;
        return bytesRef;
      } catch (IOException ex) {
        throw new IllegalStateException("failed", ex);

      }
    }
    
    @Override
    public int getValueCount() {
      return valueCount;
    }

  }
}
