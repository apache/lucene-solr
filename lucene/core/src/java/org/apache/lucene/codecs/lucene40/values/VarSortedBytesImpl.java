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
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.codecs.lucene40.values.Bytes.BytesReaderBase;
import org.apache.lucene.codecs.lucene40.values.Bytes.BytesSortedSourceBase;
import org.apache.lucene.codecs.lucene40.values.Bytes.DerefBytesWriterBase;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedBytesMergeUtils;
import org.apache.lucene.index.DocValues.SortedSource;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.SortedBytesMergeUtils.IndexOutputBytesRefConsumer;
import org.apache.lucene.index.SortedBytesMergeUtils.MergeContext;
import org.apache.lucene.index.SortedBytesMergeUtils.SortedSourceSlice;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
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
        Counter bytesUsed, IOContext context, boolean fasterButMoreRam) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context, fasterButMoreRam, Type.BYTES_VAR_SORTED);
      this.comp = comp;
      size = 0;
    }

    @Override
    public void merge(MergeState mergeState, DocValues[] docValues)
        throws IOException {
      boolean success = false;
      try {
        MergeContext ctx = SortedBytesMergeUtils.init(Type.BYTES_VAR_SORTED, docValues, comp, mergeState.mergedDocCount);
        final List<SortedSourceSlice> slices = SortedBytesMergeUtils.buildSlices(mergeState.docBase, mergeState.docMaps, docValues, ctx);
        IndexOutput datOut = getOrCreateDataOut();
        
        ctx.offsets = new long[1];
        final int maxOrd = SortedBytesMergeUtils.mergeRecords(ctx, new IndexOutputBytesRefConsumer(datOut), slices);
        final long[] offsets = ctx.offsets;
        maxBytes = offsets[maxOrd-1];
        final IndexOutput idxOut = getOrCreateIndexOut();
        
        idxOut.writeLong(maxBytes);
        final PackedInts.Writer offsetWriter = PackedInts.getWriter(idxOut, maxOrd+1,
            PackedInts.bitsRequired(maxBytes));
        offsetWriter.add(0);
        for (int i = 0; i < maxOrd; i++) {
          offsetWriter.add(offsets[i]);
        }
        offsetWriter.finish();
        
        final PackedInts.Writer ordsWriter = PackedInts.getWriter(idxOut, ctx.docToEntry.length,
            PackedInts.bitsRequired(maxOrd-1));
        for (SortedSourceSlice slice : slices) {
          slice.writeOrds(ordsWriter);
        }
        ordsWriter.finish();
        success = true;
      } finally {
        releaseResources();
        if (success) {
          IOUtils.close(getIndexOut(), getDataOut());
        } else {
          IOUtils.closeWhileHandlingException(getIndexOut(), getDataOut());
        }

      }
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
      final IndexOutput idxOut = getOrCreateIndexOut();
      long offset = 0;
      final int[] index = new int[count];
      final int[] sortedEntries = hash.sort(comp);
      // total bytes of data
      idxOut.writeLong(maxBytes);
      PackedInts.Writer offsetWriter = PackedInts.getWriter(idxOut, count+1,
          bitsRequired(maxBytes));
      // first dump bytes data, recording index & write offset as
      // we go
      final BytesRef spare = new BytesRef();
      for (int i = 0; i < count; i++) {
        final int e = sortedEntries[i];
        offsetWriter.add(offset);
        index[e] = i;
        final BytesRef bytes = hash.get(e, spare);
        // TODO: we could prefix code...
        datOut.writeBytes(bytes.bytes, bytes.offset, bytes.length);
        offset += bytes.length;
      }
      // write sentinel
      offsetWriter.add(offset);
      offsetWriter.finish();
      // write index
      writeIndex(idxOut, docCount, count, index, docToEntry);

    }
  }

  public static class Reader extends BytesReaderBase {

    private final Comparator<BytesRef> comparator;

    Reader(Directory dir, String id, int maxDoc,
        IOContext context, Type type, Comparator<BytesRef> comparator)
        throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true, context, type);
      this.comparator = comparator;
    }

    @Override
    public org.apache.lucene.index.DocValues.Source load()
        throws IOException {
      return new VarSortedSource(cloneData(), cloneIndex(), comparator);
    }

    @Override
    public Source getDirectSource() throws IOException {
      return new DirectSortedSource(cloneData(), cloneIndex(), comparator, getType());
    }
    
  }
  private static final class VarSortedSource extends BytesSortedSourceBase {
    private final int valueCount;

    VarSortedSource(IndexInput datIn, IndexInput idxIn,
        Comparator<BytesRef> comp) throws IOException {
      super(datIn, idxIn, comp, idxIn.readLong(), Type.BYTES_VAR_SORTED, true);
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
    private final PackedInts.Reader ordToOffsetIndex;
    private final IndexInput datIn;
    private final long basePointer;
    private final int valueCount;
    
    DirectSortedSource(IndexInput datIn, IndexInput idxIn,
        Comparator<BytesRef> comparator, Type type) throws IOException {
      super(type, comparator);
      idxIn.readLong();
      ordToOffsetIndex = PackedInts.getDirectReader(idxIn);
      valueCount = ordToOffsetIndex.size()-1; // the last value here is just a dummy value to get the length of the last value
      // advance this iterator to the end and clone the stream once it points to the docToOrdIndex header
      ordToOffsetIndex.get(valueCount);
      docToOrdIndex = PackedInts.getDirectReader((IndexInput) idxIn.clone()); // read the ords in to prevent too many random disk seeks
      basePointer = datIn.getFilePointer();
      this.datIn = datIn;
    }

    @Override
    public int ord(int docID) {
      return (int) docToOrdIndex.get(docID);
    }

    @Override
    public boolean hasPackedDocToOrd() {
      return true;
    }

    @Override
    public PackedInts.Reader getDocToOrd() {
      return docToOrdIndex;
    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      try {
        final long offset = ordToOffsetIndex.get(ord);
        // 1+ord is safe because we write a sentinel at the end
        final long nextOffset = ordToOffsetIndex.get(1+ord);
        datIn.seek(basePointer + offset);
        final int length = (int) (nextOffset - offset);
        bytesRef.grow(length);
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
