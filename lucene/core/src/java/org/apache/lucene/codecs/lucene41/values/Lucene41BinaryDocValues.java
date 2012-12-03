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
import org.apache.lucene.codecs.lucene41.values.Lucene41DocValuesProducer.DocValuesFactory;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.codecs.lucene41.values.Lucene41BinaryDocValuesConsumer.*;

public final class Lucene41BinaryDocValues extends BinaryDocValues {
  private final PackedInts.Reader index;
  private final IndexInput data;
  private final long baseOffset;
  private final int size;
  private int maxLength;
  private final DocValuesFactory<BinaryDocValues> factory;
  
  public Lucene41BinaryDocValues(IndexInput dataIn, long dataOffset, int size,
      int maxLength, PackedInts.Reader index, DocValuesFactory<BinaryDocValues> factory) throws IOException {
    this.data = dataIn;
    
    this.size = size;
    this.maxLength = maxLength;
    this.baseOffset = dataOffset;
    this.index = index;
    this.factory = factory;
  }
  
  public void get(int docId, BytesRef result) {
    try {
      final long offset;
      final int length;
      if (index == null) {
        offset = size * ((long) docId);
        length = size;
      } else {
        offset = index.get(docId);
        data.seek(baseOffset + offset);
        // Safe to do 1+docID because we write sentinel at the end:
        final long nextOffset = index.get(1 + docId);
        length = (int) (nextOffset - offset);
      }
      result.offset = 0;
      result.grow(length);
      data.readBytes(result.bytes, 0, length);
      result.length = length;
    } catch (IOException ex) {
      throw new IllegalStateException(
          "failed to get value for docID: " + docId, ex);
    }
  }
  
  @Override
  public int size() {
    return size;
  }
  
  @Override
  public boolean isFixedLength() {
    return index == null;
  }
  
  @Override
  public int maxLength() {
    return maxLength;
  }

  public static final class Factory extends DocValuesFactory<BinaryDocValues> {
    private final IndexInput datIn;
    private final IndexInput indexIn;
    private final PackedInts.Header indexHeader;
    private int size;
    private int maxLength;
    private long baseOffset;
    private final int valueCount;
    
    public Factory(Directory dir,
        SegmentInfo segmentInfo, FieldInfo field, IOContext context
        ) throws IOException {
      boolean success = false;
      this.valueCount = segmentInfo.getDocCount();
      IndexInput datIn = null;
      IndexInput indexIn = null;
      try {
        datIn = dir.openInput(Lucene41DocValuesConsumer.getDocValuesFileName(
            segmentInfo, field, Lucene41DocValuesConsumer.DATA_EXTENSION),
            context);
        CodecUtil.checkHeader(datIn, CODEC_NAME, VERSION_START, VERSION_START);
        
        this.size = datIn.readInt();
        this.maxLength = datIn.readInt();
        this.baseOffset = datIn.getFilePointer();
        
        if (size == VALUE_SIZE_VAR) {
          indexIn = dir.openInput(Lucene41DocValuesConsumer
              .getDocValuesFileName(segmentInfo, field,
                  Lucene41DocValuesConsumer.INDEX_EXTENSION), context);
          CodecUtil.checkHeader(indexIn, CODEC_NAME, VERSION_START,
              VERSION_START);
          indexHeader = PackedInts.readHeader(indexIn);
        } else {
          indexIn = null;
          indexHeader = null;
        }
        this.indexIn = indexIn;
        this.datIn = datIn;
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(datIn, indexIn);
        }
      }
    }
    
    public BinaryDocValues getDirect() throws IOException {
      return new Lucene41BinaryDocValues(datIn.clone(), this.baseOffset, size,
          maxLength,
          indexHeader == null ? null : PackedInts.getDirectReaderNoHeader(
              indexIn.clone(), indexHeader), this);
    }
    
    public BinaryDocValues getInMemory() throws IOException {
      // nocommit simple in memory impl
      PackedInts.Reader indexReader = indexHeader == null ? null : PackedInts
          .getReaderNoHeader(indexIn.clone(), indexHeader);
      PagedBytes bytes = new PagedBytes(15);
      bytes.copy(datIn.clone(), indexReader == null ? size * valueCount
          : indexReader.get(indexReader.size() - 1));
      bytes.freeze(true);
      return new Lucene41BinaryDocValues(bytes.getDataInput(), 0, size,
          maxLength, indexReader, null);
    }
    
    @Override
    public void close() throws IOException {
      IOUtils.close(datIn, indexIn);
    }
  }
}
