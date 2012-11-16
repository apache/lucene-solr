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
import org.apache.lucene.codecs.DocValuesArraySource;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;



public class Lucene41NumericDocValuesProducer extends DocValues {
  
  private final IndexInput datIn;
  private final long minValue;
  
  public Lucene41NumericDocValuesProducer(IndexInput input, int numDocs)
      throws IOException {
    datIn = input;
    boolean success = false;
    try {
      CodecUtil.checkHeader(datIn, Lucene41NumericDocValuesConsumer.CODEC_NAME,
          Lucene41NumericDocValuesConsumer.VERSION_START,
          Lucene41NumericDocValuesConsumer.VERSION_START);
      minValue = datIn.readLong();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(datIn);
      }
    }
  }
  
  /**
   * Loads the actual values. You may call this more than once, eg if you
   * already previously loaded but then discarded the Source.
   */
  @Override
  protected Source loadSource() throws IOException {
    boolean success = false;
    final Source source;
    IndexInput input = null;
    try {
      input = datIn.clone();
      source = new PackedIntsSource(input, false, minValue);
      success = true;
      return source;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(input, datIn);
      }
    }
  }
  
  @Override
  public void close() throws IOException {
    super.close();
    datIn.close();
  }
  
  @Override
  public Type getType() {
    return Type.VAR_INTS;
  }
  
  @Override
  protected Source loadDirectSource() throws IOException {
    return new PackedIntsSource(datIn.clone(), true, minValue);
  }
  
  static class PackedIntsSource extends Source {
    private final PackedInts.Reader values;
    private final long minValue;
    
    public PackedIntsSource(IndexInput dataIn, boolean direct, long minValue)
        throws IOException {
      super(Type.VAR_INTS);
      this.minValue = minValue;
      values = direct ? PackedInts.getDirectReader(dataIn) : PackedInts
          .getReader(dataIn);
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      ref.grow(8);
      DocValuesArraySource.copyLong(ref, getInt(docID));
      return ref;
    }
    
    @Override
    public long getInt(int docID) {
      assert docID >= 0;
      return values.get(docID) + minValue;
    }
  }
  
}
