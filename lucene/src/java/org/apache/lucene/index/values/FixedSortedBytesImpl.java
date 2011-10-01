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
import org.apache.lucene.index.values.FixedDerefBytesImpl.Reader.DerefBytesEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

// Stores fixed-length byte[] by deref, ie when two docs
// have the same value, they store only 1 byte[]

/**
 * @lucene.experimental
 */
class FixedSortedBytesImpl {

  static final String CODEC_NAME = "FixedSortedBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class Writer extends DerefBytesWriterBase {
    private final Comparator<BytesRef> comp;

    public Writer(Directory dir, String id, Comparator<BytesRef> comp,
        Counter bytesUsed, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context);
      this.comp = comp;
    }

    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    public void finishInternal(int docCount) throws IOException {
      final IndexOutput datOut = getOrCreateDataOut();
      final int count = hash.size();
      final int[] address = new int[count+1]; // addr 0 is default values
      datOut.writeInt(size);
      if (size != -1) {
        final int[] sortedEntries = hash.sort(comp);
        // first dump bytes data, recording address as we go
        final BytesRef bytesRef = new BytesRef(size);
        for (int i = 0; i < count; i++) {
          final int e = sortedEntries[i];
          final BytesRef bytes = hash.get(e, bytesRef);
          assert bytes.length == size;
          datOut.writeBytes(bytes.bytes, bytes.offset, bytes.length);
          address[e + 1] = 1 + i;
        }
      }
      final IndexOutput idxOut = getOrCreateIndexOut();
      idxOut.writeInt(count);
      writeIndex(idxOut, docCount, count, address, docToEntry);
    }
  }

  public static class Reader extends BytesReaderBase {
    private final int size;
    private final int numValuesStored;

    public Reader(Directory dir, String id, int maxDoc, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true, context);
      size = datIn.readInt();
      numValuesStored = idxIn.readInt();
    }

    @Override
    public org.apache.lucene.index.values.IndexDocValues.Source load()
        throws IOException {
      return loadSorted(null);
    }

    @Override
    public SortedSource loadSorted(Comparator<BytesRef> comp)
        throws IOException {
      return new Source(cloneData(), cloneIndex(), size, numValuesStored, comp);
    }

    private static class Source extends BytesSortedSourceBase {
      private final int valueCount;
      private final int size;

      public Source(IndexInput datIn, IndexInput idxIn, int size,
          int numValues, Comparator<BytesRef> comp) throws IOException {
        super(datIn, idxIn, comp, size * numValues, ValueType.BYTES_FIXED_SORTED);
        this.size = size;
        this.valueCount = numValues;
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

      @Override
      protected BytesRef deref(int ord, BytesRef bytesRef) {
        return data.fillSlice(bytesRef, (ord * size), size);
      }
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      // do unsorted
      return new DerefBytesEnum(source, cloneData(), cloneIndex(), size);
    }

    @Override
    public ValueType type() {
      return ValueType.BYTES_FIXED_SORTED;
    }
  }
}
