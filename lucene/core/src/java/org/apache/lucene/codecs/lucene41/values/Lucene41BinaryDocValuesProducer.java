package org.apache.lucene.codecs.lucene41.values;

/*
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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;

public class Lucene41BinaryDocValuesProducer extends DocValues {
  
  private final IndexInput dataIn;
  private final IndexInput indexIn;
  private final int valueLength;
  private final long dataFilePointer;
  private long bytesToRead;
  
  public Lucene41BinaryDocValuesProducer(IndexInput dataIn, IndexInput indexIn)
      throws IOException {
    this.dataIn = dataIn;
    CodecUtil.checkHeader(dataIn, Lucene41BinaryDocValuesConsumer.CODEC_NAME,
        Lucene41BinaryDocValuesConsumer.VERSION_START,
        Lucene41BinaryDocValuesConsumer.VERSION_START);
    valueLength = dataIn.readInt();
    dataFilePointer = dataIn.getFilePointer();
    CodecUtil.checkHeader(indexIn, Lucene41BinaryDocValuesConsumer.CODEC_NAME,
        Lucene41BinaryDocValuesConsumer.VERSION_START,
        Lucene41BinaryDocValuesConsumer.VERSION_START);
    bytesToRead = indexIn.readLong();
    if (valueLength == Lucene41BinaryDocValuesConsumer.VALUE_SIZE_VAR) {
      this.indexIn = indexIn;
    } else {
      indexIn.close();
      this.indexIn = null;
    }
    
  }
  
  @Override
  protected Source loadSource() throws IOException {
    if (valueLength == Lucene41BinaryDocValuesConsumer.VALUE_SIZE_VAR) {
      assert indexIn != null;
      return new VarStraightSource(dataIn.clone(), indexIn.clone(), bytesToRead);
    } else {
      assert indexIn == null;
      return new FixedStraightSource(dataIn.clone(), valueLength, bytesToRead);
    }
  }
  
  @Override
  protected Source loadDirectSource() throws IOException {
    if (valueLength == Lucene41BinaryDocValuesConsumer.VALUE_SIZE_VAR) {
      assert indexIn != null;
      return new DirectVarStraightSource(dataIn.clone(), indexIn.clone(),
          dataFilePointer);
    } else {
      assert indexIn == null;
      return new DirectFixedStraightSource(dataIn.clone(), valueLength,
          dataFilePointer);
    }
  }
  
  @Override
  public Type getType() {
    return valueLength == Lucene41BinaryDocValuesConsumer.VALUE_SIZE_VAR ? Type.BYTES_VAR_STRAIGHT
        : Type.BYTES_FIXED_STRAIGHT;
  }
  
  @Override
  public void close() throws IOException {
    super.close();
    IOUtils.close(dataIn, indexIn);
  }
  
  static abstract class BytesSourceBase extends Source {
    private final PagedBytes pagedBytes;
    protected final IndexInput datIn;
    protected final IndexInput idxIn;
    protected final static int PAGED_BYTES_BITS = 15;
    protected final PagedBytes.Reader data;
    protected final long totalLengthInBytes;
    
    protected BytesSourceBase(IndexInput datIn, IndexInput idxIn,
        PagedBytes pagedBytes, long bytesToRead, Type type) throws IOException {
      super(type);
      assert bytesToRead <= datIn.length() : " file size is less than the expected size diff: "
          + (bytesToRead - datIn.length()) + " pos: " + datIn.getFilePointer();
      this.datIn = datIn;
      this.totalLengthInBytes = bytesToRead;
      this.pagedBytes = pagedBytes;
      this.pagedBytes.copy(datIn, bytesToRead);
      data = pagedBytes.freeze(true);
      this.idxIn = idxIn;
    }
  }
  
  public final static class DirectVarStraightSource extends Source {
    
    private final PackedInts.Reader index;
    private final IndexInput data;
    private final long baseOffset;
    
    DirectVarStraightSource(IndexInput data, IndexInput index,
        long dataFilePointer) throws IOException {
      super(Type.BYTES_VAR_STRAIGHT);
      this.data = data;
      baseOffset = dataFilePointer;
      this.index = PackedInts.getDirectReader(index); // nocommit read without
                                                      // header
    }
    
    private final int position(int docID) throws IOException {
      final long offset = index.get(docID);
      data.seek(baseOffset + offset);
      // Safe to do 1+docID because we write sentinel at the end:
      final long nextOffset = index.get(1 + docID);
      return (int) (nextOffset - offset);
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      try {
        final int sizeToRead = position(docID);
        ref.offset = 0;
        ref.grow(sizeToRead);
        data.readBytes(ref.bytes, 0, sizeToRead);
        ref.length = sizeToRead;
        return ref;
      } catch (IOException ex) {
        throw new IllegalStateException("failed to get value for docID: "
            + docID, ex);
      }
    }
  }
  
  private static final class VarStraightSource extends BytesSourceBase {
    private final PackedInts.Reader addresses;
    
    public VarStraightSource(IndexInput datIn, IndexInput idxIn,
        long bytesToRead) throws IOException {
      super(datIn, idxIn, new PagedBytes(PAGED_BYTES_BITS), bytesToRead,
          Type.BYTES_VAR_STRAIGHT);
      addresses = PackedInts.getReader(idxIn); // nocommit read without header
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      final long address = addresses.get(docID);
      return data.fillSlice(bytesRef, address,
          (int) (addresses.get(docID + 1) - address));
    }
  }
  
  private final static class FixedStraightSource extends BytesSourceBase {
    private final int size;
    
    public FixedStraightSource(IndexInput datIn, int size, long bytesToRead)
        throws IOException {
      super(datIn, null, new PagedBytes(PAGED_BYTES_BITS), bytesToRead,
          Type.BYTES_FIXED_STRAIGHT);
      this.size = size;
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      return data.fillSlice(bytesRef, size * ((long) docID), size);
    }
  }
  
  public final static class DirectFixedStraightSource extends Source {
    private final int size;
    private IndexInput data;
    private long baseOffset;
    
    DirectFixedStraightSource(IndexInput input, int size, long dataFilePointer) {
      super(Type.BYTES_FIXED_STRAIGHT);
      this.size = size;
      this.data = input;
      baseOffset = dataFilePointer;
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      try {
        data.seek(baseOffset + size * ((long) docID));
        ref.offset = 0;
        ref.grow(size);
        data.readBytes(ref.bytes, 0, size);
        ref.length = size;
        return ref;
      } catch (IOException ex) {
        throw new IllegalStateException("failed to get value for docID: "
            + docID, ex);
      }
    }
  }
}
