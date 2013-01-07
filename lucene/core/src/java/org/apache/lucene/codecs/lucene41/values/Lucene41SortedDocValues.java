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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.codecs.lucene41.values.Lucene41SortedDocValuesConsumer.*;

public class Lucene41SortedDocValues extends SortedDocValues {
  private final PackedInts.Reader docToOrdIndex;
  private final PackedInts.Reader ordToOffsetIndex;
  private final IndexInput data;
  private final long baseOffset;
  private final int valueCount;
  private int size;
  private int maxLength;
  private final DocValuesFactory<SortedDocValues> factory;
  
  public Lucene41SortedDocValues(IndexInput dataIn, long dataOffset, int size,
      int maxLength, int valueCount, PackedInts.Reader index, PackedInts.Reader offsets, DocValuesFactory<SortedDocValues> factory)
      throws IOException {
    this.data = dataIn;
    this.size = size;
    this.maxLength = maxLength;
    this.baseOffset = dataOffset;
    this.valueCount = valueCount;
    this.docToOrdIndex = index;
    ordToOffsetIndex = offsets;
    this.factory = factory;
    
  }
  
  @Override
  public int getOrd(int docID) {
    return (int) docToOrdIndex.get(docID);
  }
  
  @Override
  public void lookupOrd(int ord, BytesRef result) {
    try {
      assert ord < valueCount;
      final long offset;
      final int length;
      if (ordToOffsetIndex != null) {
        offset = ordToOffsetIndex.get(ord);
        
        // 1+ord is safe because we write a sentinel at the end
        final long nextOffset = ordToOffsetIndex.get(1 + ord);
        assert offset <= nextOffset : "offset: " + offset + " nextOffset: " + nextOffset + " ord: " + ord + " numValues: " + valueCount;
        length = (int) (nextOffset - offset);
      } else {
        length = size;
        offset = size * ord;
      }
      data.seek(baseOffset + offset);
      result.offset = 0;
      result.grow(length);
      data.readBytes(result.bytes, 0, length);
      result.length = length;
    } catch (IOException ex) {
      throw new IllegalStateException("failed", ex);
    }
  }
  
  @Override
  public int getValueCount() {
    return valueCount;
  }
  
  @Override
  public int size() {
    return size;
  }
  
  @Override
  public boolean isFixedLength() {
    return ordToOffsetIndex == null;
  }
  
  @Override
  public int maxLength() {
    return maxLength;
  }
  
  public static final class Factory extends
      DocValuesFactory<SortedDocValues> {
    private final IndexInput datIn;
    private final IndexInput offsetIn;
    private final PackedInts.Header offsetHeader;
    private final IndexInput indexIn;
    private final PackedInts.Header indexHeader;
    private final int size;
    private final int maxLength;
    private final long baseOffset;
    private final int valueCount;
    
    public Factory(Directory dir,
        SegmentInfo segmentInfo, FieldInfo field, IOContext context)
        throws IOException {
      boolean success = false;
      IndexInput datIn = null;
      IndexInput indexIn = null;
      IndexInput offsetIn = null;
      try {
        datIn = dir.openInput(Lucene41DocValuesConsumer.getDocValuesFileName(
            segmentInfo, field, Lucene41DocValuesConsumer.DATA_EXTENSION),
            context);
        CodecUtil.checkHeader(datIn, CODEC_NAME, VERSION_START, VERSION_START);
        indexIn = dir.openInput(Lucene41DocValuesConsumer.getDocValuesFileName(
            segmentInfo, field, Lucene41DocValuesConsumer.INDEX_EXTENSION),
            context);
        CodecUtil
            .checkHeader(indexIn, CODEC_NAME, VERSION_START, VERSION_START);
        indexHeader = PackedInts.readHeader(indexIn);
        this.size = datIn.readInt();
        this.maxLength = datIn.readInt();
        this.valueCount = datIn.readInt();
        this.baseOffset = datIn.getFilePointer();
        
        //if (size == Lucene41BinaryDocValuesConsumer.VALUE_SIZE_VAR) {
        if (size == -1) {
          offsetIn = dir.openInput(Lucene41DocValuesConsumer
              .getDocValuesFileName(segmentInfo, field,
                  Lucene41DocValuesConsumer.OFFSET_EXTENSION), context);
          CodecUtil.checkHeader(offsetIn, CODEC_NAME, VERSION_START,
              VERSION_START);
          this.offsetHeader = PackedInts.readHeader(offsetIn);
        } else {
          offsetIn = null;
          this.offsetHeader = null;
        }
        this.offsetIn = offsetIn;
        this.indexIn = indexIn;
        this.datIn = datIn;
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(datIn, indexIn, offsetIn);
        }
      }
    }
    
    public SortedDocValues getDirect() throws IOException {
      return new Lucene41SortedDocValues(datIn.clone(), this.baseOffset, size,
          maxLength, valueCount, PackedInts.getDirectReaderNoHeader(indexIn.clone(),
              indexHeader), offsetHeader == null ? null
              : PackedInts.getDirectReaderNoHeader(offsetIn.clone(),
                  offsetHeader), this);
    }
    
    public Lucene41SortedDocValues getInMemory() throws IOException {
      // nocommit simple in memory impl
      PackedInts.Reader offsetReader = offsetHeader == null ? null : PackedInts
          .getReaderNoHeader(offsetIn.clone(), offsetHeader);
      PackedInts.Reader indexReader = PackedInts.getReaderNoHeader(
          indexIn.clone(), indexHeader);
      PagedBytes bytes = new PagedBytes(15);
      bytes.copy(
          datIn.clone(),
          offsetReader == null ? size * indexReader.size() : offsetReader
              .get(offsetReader.size() - 1));
      bytes.freeze(true);
      return new Lucene41SortedDocValues(bytes.getDataInput(), 0, size,
          maxLength, valueCount, indexReader, offsetReader, this);
    }
    
    @Override
    public void close() throws IOException {
      IOUtils.close(datIn, indexIn, offsetIn);
    }
  }
  
}
