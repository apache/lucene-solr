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
import org.apache.lucene.codecs.lucene40.values.Bytes.DerefBytesWriterBase;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;

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
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context, Type.BYTES_FIXED_DEREF);
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

  public static class FixedDerefReader extends BytesReaderBase {
    private final int size;
    private final int numValuesStored;
    FixedDerefReader(Directory dir, String id, int maxDoc, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true, context, Type.BYTES_FIXED_DEREF);
      size = datIn.readInt();
      numValuesStored = idxIn.readInt();
    }

    @Override
    public Source load() throws IOException {
      return new FixedDerefSource(cloneData(), cloneIndex(), size, numValuesStored);
    }

    @Override
    public Source getDirectSource()
        throws IOException {
      return new DirectFixedDerefSource(cloneData(), cloneIndex(), size, getType());
    }

    @Override
    public int getValueSize() {
      return size;
    }
    
  }
  
  static final class FixedDerefSource extends BytesSourceBase {
    private final int size;
    private final PackedInts.Reader addresses;

    protected FixedDerefSource(IndexInput datIn, IndexInput idxIn, int size, long numValues) throws IOException {
      super(datIn, idxIn, new PagedBytes(PAGED_BYTES_BITS), size * numValues,
          Type.BYTES_FIXED_DEREF);
      this.size = size;
      addresses = PackedInts.getReader(idxIn);
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      final int id = (int) addresses.get(docID);
      return data.fillSlice(bytesRef, (id * size), size);
    }

  }
  
  final static class DirectFixedDerefSource extends DirectSource {
    private final PackedInts.Reader index;
    private final int size;

    DirectFixedDerefSource(IndexInput data, IndexInput index, int size, Type type)
        throws IOException {
      super(data, type);
      this.size = size;
      this.index = PackedInts.getDirectReader(index);
    }

    @Override
    protected int position(int docID) throws IOException {
      data.seek(baseOffset + index.get(docID) * size);
      return size;
    }
  }

}
