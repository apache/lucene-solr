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

import org.apache.lucene.index.values.Bytes.BytesReaderBase;
import org.apache.lucene.index.values.Bytes.DerefBytesSourceBase;
import org.apache.lucene.index.values.Bytes.DerefBytesEnumBase;
import org.apache.lucene.index.values.Bytes.DerefBytesWriterBase;
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
class FixedDerefBytesImpl {

  static final String CODEC_NAME = "FixedDerefBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  public static class Writer extends DerefBytesWriterBase {
    public Writer(Directory dir, String id, Counter bytesUsed, IOContext context)
        throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context);
    }

    @Override
    protected void finishInternal(int docCount) throws IOException {
      final int numValues = hash.size();
      final IndexOutput datOut = getOrCreateDataOut();
      datOut.writeInt(size);
      if (size != -1) {
        final BytesRef bytesRef = new BytesRef(size);
        for (int i = 0; i < numValues; i++) {
          hash.get(i, bytesRef);
          datOut.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
      }
      final IndexOutput idxOut = getOrCreateIndexOut();
      idxOut.writeInt(numValues);
      writeIndex(idxOut, docCount, numValues, docToEntry);
    }
  }

  public static class Reader extends BytesReaderBase {
    private final int size;
    private final int numValuesStored;
    Reader(Directory dir, String id, int maxDoc, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true, context);
      size = datIn.readInt();
      numValuesStored = idxIn.readInt();
    }

    @Override
    public Source load() throws IOException {
      return new Source(cloneData(), cloneIndex(), size, numValuesStored);
    }

    private static final class Source extends DerefBytesSourceBase {
      private final int size;

      protected Source(IndexInput datIn, IndexInput idxIn, int size, long numValues) throws IOException {
        super(datIn, idxIn, size * numValues, ValueType.BYTES_FIXED_DEREF);
        this.size = size;
      }

      @Override
      public BytesRef getBytes(int docID, BytesRef bytesRef) {
        final int id = (int) addresses.get(docID);
        if (id == 0) {
          bytesRef.length = 0;
          return bytesRef;
        }
        return data.fillSlice(bytesRef, ((id - 1) * size), size);
      }

    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
        return new DerefBytesEnum(source, cloneData(), cloneIndex(), size);
    }

    final static class DerefBytesEnum extends DerefBytesEnumBase {

      public DerefBytesEnum(AttributeSource source, IndexInput datIn,
          IndexInput idxIn, int size) throws IOException {
        super(source, datIn, idxIn, size, ValueType.BYTES_FIXED_DEREF);
      }

      protected void fill(long address, BytesRef ref) throws IOException {
        datIn.seek(fp + ((address - 1) * size));
        datIn.readBytes(ref.bytes, 0, size);
        ref.length = size;
        ref.offset = 0;
      }
    }

    @Override
    public ValueType type() {
      return ValueType.BYTES_FIXED_DEREF;
    }
  }

}
