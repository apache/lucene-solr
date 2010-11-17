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
import org.apache.lucene.index.values.FixedDerefBytesImpl.Reader.DerefBytesEnum;
import org.apache.lucene.store.DataOutput;
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
import org.apache.lucene.util.BytesRefHash.ParallelArrayBase;
import org.apache.lucene.util.BytesRefHash.ParallelBytesStartArray;
import org.apache.lucene.util.packed.PackedInts;

// Stores variable-length byte[] by deref, ie when two docs
// have the same value, they store only 1 byte[] and both
// docs reference that single source

class VarDerefBytesImpl {

  static final String CODEC_NAME = "VarDerefBytes";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  
 

  private static class AddressParallelArray extends ParallelArrayBase<AddressParallelArray> {
    final int[] address;
    
    AddressParallelArray(int size, AtomicLong bytesUsed) {
      super(size, bytesUsed);
      address = new int[size]; 
    }
    @Override
    protected int bytesPerEntry() {
      return RamUsageEstimator.NUM_BYTES_INT + super.bytesPerEntry();
    }

    @Override
    protected void copyTo(AddressParallelArray toArray, int numToCopy) {
      super.copyTo(toArray, numToCopy);
      System.arraycopy(address, 0, toArray.address, 0, size);
      
    }

    @Override
    public AddressParallelArray newInstance(int size) {
      return new AddressParallelArray(size, bytesUsed);
    }
    
  }


  static class Writer extends BytesWriterBase {
    private int[] docToAddress;
    private int address = 1;
    
    private final ParallelBytesStartArray<AddressParallelArray> array = new ParallelBytesStartArray<AddressParallelArray>(new AddressParallelArray(0, bytesUsed));
    private final BytesRefHash hash  = new BytesRefHash(pool, 16, array) ;

    public Writer(Directory dir, String id) throws IOException  {
      this(dir, id, new DirectAllocator(ByteBlockPool.BYTE_BLOCK_SIZE),
          new AtomicLong());
    }
    public Writer(Directory dir, String id, Allocator allocator, AtomicLong bytesUsed) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, false, false, new ByteBlockPool(allocator), bytesUsed);
      docToAddress = new int[1];
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT);
    }

    @Override
    synchronized public void add(int docID, BytesRef bytes) throws IOException {
      if(bytes.length == 0)
        return; // default
      if(datOut == null)
        initDataOut();
      final int e = hash.add(bytes);

      if (docID >= docToAddress.length) {
        final int oldSize = docToAddress.length;
        docToAddress = ArrayUtil.grow(docToAddress, 1+docID);
        bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT * (docToAddress.length - oldSize));
      }
      final int docAddress;
      if (e >= 0) {
        docAddress = array.array.address[e] = address;
        address += writePrefixLength(datOut, bytes);
        datOut.writeBytes(bytes.bytes, bytes.offset, bytes.length);
        address += bytes.length;
      } else {
        docAddress = array.array.address[(-e)-1];
      }
      docToAddress[docID] = docAddress;
    }
    
    private static int writePrefixLength(DataOutput datOut, BytesRef bytes) throws IOException{
      if (bytes.length < 128) {
        datOut.writeByte((byte) bytes.length);
        return 1;
      } else {
        datOut.writeByte((byte) (0x80 | (bytes.length >> 8)));
        datOut.writeByte((byte) (bytes.length & 0xff));
        return 2;
      }
    }
    
    public long ramBytesUsed() {
      return bytesUsed.get();
    }

    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    synchronized public void finish(int docCount) throws IOException {
      if(datOut == null)
        return;
      initIndexOut();
      idxOut.writeInt(address-1);

      // write index
      // TODO(simonw): -- allow forcing fixed array (not -1)
      // TODO(simonw): check the address calculation / make it more intuitive
      final PackedInts.Writer w = PackedInts.getWriter(idxOut, docCount, PackedInts.bitsRequired(address-1));
      final int limit;
      if (docCount > docToAddress.length) {
        limit = docToAddress.length;
      } else {
        limit = docCount;
      }
      for(int i=0;i<limit;i++) {
        w.add(docToAddress[i]);
      }
      for(int i=limit;i<docCount;i++) {
        w.add(0);
      }
      w.finish();
      hash.clear(true);
      super.finish(docCount);
    }
  }

  public static class Reader extends BytesReaderBase {

    Reader(Directory dir, String id, int maxDoc)
      throws IOException {
      super(dir, id, CODEC_NAME, VERSION_START, true);
    }

    @Override
    public Source load() throws IOException {
      final IndexInput data = cloneData();
      final IndexInput index = cloneIndex();
      data.seek(CodecUtil.headerLength(CODEC_NAME));
      index.seek(CodecUtil.headerLength(CODEC_NAME));
      final long totalBytes = index.readInt(); // should be long
      return new Source(data,index, totalBytes);
    }

    private static class Source extends BytesBaseSource {
      private final BytesRef bytesRef = new BytesRef();
      private final PackedInts.Reader index;

      public Source(IndexInput datIn, IndexInput idxIn, long totalBytes) throws IOException {
        super(datIn, idxIn, new PagedBytes(PAGED_BYTES_BITS), totalBytes);
        index = PackedInts.getReader(idxIn);
      }

      @Override
      public BytesRef getBytes(int docID) {
        long address =  index.get(docID);
        if (address == 0) {
          assert defaultValue.length == 0: " default value manipulated";
          return defaultValue;
        } else {
          data.fillUsingLengthPrefix2(bytesRef, --address);
          return bytesRef;
        }
      }
      
      @Override
      public int getValueCount() {
        return index.size();
      }
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      return new VarDerefBytesEnum(source, cloneData(), cloneIndex(), CODEC_NAME);
    }
    
    static class VarDerefBytesEnum extends DerefBytesEnum {

      public VarDerefBytesEnum(AttributeSource source, IndexInput datIn, IndexInput idxIn,
          String codecName) throws IOException {
        super(source, datIn, idxIn, codecName, -1, Values.BYTES_VAR_DEREF);
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
          size = ((sizeByte & 0x7f)<<8) | ((datIn.readByte() & 0xff));
        }
        if(ref.bytes.length < size)
          ref.grow(size);
        ref.length = size;
        ref.offset = 0;
        datIn.readBytes(ref.bytes, 0, size);
      }
    }
    
    @Override
    public Values type() {
      return Values.BYTES_VAR_DEREF;
    }
  }
}
