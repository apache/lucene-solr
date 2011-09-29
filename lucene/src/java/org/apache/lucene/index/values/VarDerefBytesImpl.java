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

// Stores variable-length byte[] by deref, ie when two docs
// have the same value, they store only 1 byte[] and both
// docs reference that single source

/**
 * @lucene.experimental
 */
class VarDerefBytesImpl {

  static final String CODEC_NAME = "VarDerefBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  /*
   * TODO: if impls like this are merged we are bound to the amount of memory we
   * can store into a BytesRefHash and therefore how much memory a ByteBlockPool
   * can address. This is currently limited to 2GB. While we could extend that
   * and use 64bit for addressing this still limits us to the existing main
   * memory as all distinct bytes will be loaded up into main memory. We could
   * move the byte[] writing to #finish(int) and store the bytes in sorted
   * order and merge them in a streamed fashion. 
   */
  static class Writer extends DerefBytesWriterBase {
    public Writer(Directory dir, String id, Counter bytesUsed, IOContext context)
        throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context);
    }
    
    @Override
    protected void checkSize(BytesRef bytes) {
      // allow var bytes sizes
    }

    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    public void finishInternal(int docCount) throws IOException {
      final int size = hash.size();
      final long[] addresses = new long[size+1];
      final IndexOutput datOut = getOrCreateDataOut();
      int addr = 1;
      final BytesRef bytesRef = new BytesRef();
      for (int i = 0; i < size; i++) {
        hash.get(i, bytesRef);
        addresses[i+1] = addr;
        addr += writePrefixLength(datOut, bytesRef) + bytesRef.length;
        datOut.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
      }

      final IndexOutput idxOut = getOrCreateIndexOut();
      // write the max address to read directly on source load
      idxOut.writeLong(addr - 1);
      writeIndex(idxOut, docCount, addresses[size], addresses, docToEntry);
    }
  }

  public static class Reader extends BytesReaderBase {
    private final long totalBytes;
    Reader(Directory dir, String id, int maxDoc, IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true, context);
      totalBytes = idxIn.readLong();
    }

    @Override
    public Source load() throws IOException {
      return new Source(cloneData(), cloneIndex(), totalBytes);
    }

    private final static class Source extends DerefBytesSourceBase {

      public Source(IndexInput datIn, IndexInput idxIn, long totalBytes)
          throws IOException {
        super(datIn, idxIn, totalBytes, ValueType.BYTES_VAR_DEREF);
      }

      @Override
      public BytesRef getBytes(int docID, BytesRef bytesRef) {
        long address = addresses.get(docID);
        bytesRef.length = 0;
        return address == 0 ? bytesRef : data.fillSliceWithPrefix(bytesRef,
            --address);
      }
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      return new VarDerefBytesEnum(source, cloneData(), cloneIndex());
    }

    final static class VarDerefBytesEnum extends DerefBytesEnumBase {
      
      public VarDerefBytesEnum(AttributeSource source, IndexInput datIn,
          IndexInput idxIn) throws IOException {
        super(source, datIn, idxIn, -1, ValueType.BYTES_VAR_DEREF);
      }

      @Override
      protected void fill(long address, BytesRef ref) throws IOException {
        datIn.seek(fp + --address);
        final byte sizeByte = datIn.readByte();
        final int size;
        if ((sizeByte & 128) == 0) {
          // length is 1 byte
          size = sizeByte;
        } else {
          size = ((sizeByte & 0x7f) << 8) | ((datIn.readByte() & 0xff));
        }
        if (ref.bytes.length < size) {
          ref.grow(size);
        }
        ref.length = size;
        ref.offset = 0;
        datIn.readBytes(ref.bytes, 0, size);
      }
    }

    @Override
    public ValueType type() {
      return ValueType.BYTES_VAR_DEREF;
    }
  }
}
