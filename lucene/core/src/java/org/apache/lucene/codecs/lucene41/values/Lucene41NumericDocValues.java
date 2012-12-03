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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Reader;

public class Lucene41NumericDocValues extends NumericDocValues {
  
  private final long minValue;
  private final Reader values;
  private final long maxValue;
  private final DocValuesFactory<NumericDocValues> factory;
  
  public Lucene41NumericDocValues(PackedInts.Reader reader, long minValue,
      long maxValue, DocValuesFactory<NumericDocValues> factory) {
    this.values = reader;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.factory = factory;
  }
  
  @Override
  public long get(int docID) {
    assert docID >= 0;
    return values.get(docID) + minValue;
  }
  
  public static final class Factory extends DocValuesFactory<NumericDocValues> {
    private final IndexInput datIn;
    private final PackedInts.Header header;
    private final long minValue;
    private final long maxValue;
    
    public Factory(Directory dir, SegmentInfo segmentInfo, FieldInfo field,
        IOContext context) throws IOException {
      this.datIn = dir.openInput(Lucene41DocValuesConsumer
          .getDocValuesFileName(segmentInfo, field,
              Lucene41DocValuesConsumer.DATA_EXTENSION), context);
      boolean success = false;
      try {
        CodecUtil.checkHeader(datIn,
            Lucene41NumericDocValuesConsumer.CODEC_NAME,
            Lucene41NumericDocValuesConsumer.VERSION_START,
            Lucene41NumericDocValuesConsumer.VERSION_START);
        minValue = datIn.readLong();
        maxValue = datIn.readLong();
        this.header = PackedInts.readHeader(datIn);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(datIn);
        }
      }
    }
    
    public NumericDocValues getDirect() throws IOException {
      IndexInput input = datIn.clone();
      return new Lucene41NumericDocValues(PackedInts.getDirectReaderNoHeader(
          input, header), minValue, maxValue, this);
    }
    
    public NumericDocValues getInMemory() throws IOException {
      IndexInput input = datIn.clone();
      return new Lucene41NumericDocValues(PackedInts.getReaderNoHeader(input,
          header), minValue, maxValue, null);
    }
    
    @Override
    public void close() throws IOException {
      IOUtils.close(datIn);
    }
  }
  
  @Override
  public long minValue() {
    return minValue;
  }
  
  @Override
  public long maxValue() {
    return maxValue;
  }
  
  @Override
  public int size() {
    return values.size();
  }
}
