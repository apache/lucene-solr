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
import org.apache.lucene.codecs.SortedDocValuesConsumer;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.AppendingLongBuffer;
import org.apache.lucene.util.packed.AppendingLongBuffer.Iterator;
import org.apache.lucene.util.packed.PackedInts;

public class Lucene41SortedDocValuesConsumer extends SortedDocValuesConsumer {
  static final int VERSION_START = -1;
  static final String CODEC_NAME = "Lucene41Sorted";
  static final int VALUE_SIZE_VAR = -1;
  private final IndexOutput data;
  private final IndexOutput index;
  private final IndexOutput offsets;
  private final AppendingLongBuffer offsetBuffer;
  private final PackedInts.Writer ords;
  private long offset = 0;
  private final int valueCount;
  
  public Lucene41SortedDocValuesConsumer(IndexOutput dataOut,
      IndexOutput indexOut, IndexOutput offsetOut, int valueCount, int maxLength)
      throws IOException {
    int size;
    if (offsetOut != null) {
      size = VALUE_SIZE_VAR;
      offsetBuffer = new AppendingLongBuffer();  
      this.offsets = offsetOut;
      CodecUtil.writeHeader(offsetOut, CODEC_NAME, VERSION_START);
    } else {
      size = maxLength;
      offsetBuffer = null;
      this.offsets = null;
    }
    CodecUtil.writeHeader(dataOut, CODEC_NAME, VERSION_START);
    dataOut.writeInt(size);
    dataOut.writeInt(maxLength);
    CodecUtil.writeHeader(indexOut, CODEC_NAME, VERSION_START);
    this.data = dataOut;
    this.index = indexOut;
    this.valueCount = valueCount;
    ords = PackedInts.getWriter(index, valueCount,
        PackedInts.bitsRequired(valueCount-1), PackedInts.DEFAULT);
  }
  
  @Override
  public void addValue(BytesRef value) throws IOException {
    data.writeBytes(value.bytes, value.offset, value.length);
    if (offsetBuffer != null) {
      offsetBuffer.add(offset);
      offset += value.length;
    }
  }
  
  @Override
  public void addDoc(int ord) throws IOException {
    ords.add(ord);
  }
  
  @Override
  public void finish() throws IOException {
    try {
      ords.finish();
      
      if (offsetBuffer != null) {
        
        final int bitsRequired = PackedInts.bitsRequired(offset);
        Iterator iterator = offsetBuffer.iterator();
        PackedInts.Writer writer = PackedInts.getWriter(offsets, valueCount+1,
            bitsRequired, PackedInts.DEFAULT);
        while (iterator.hasNext()) {
          writer.add(iterator.next());
        }
        writer.add(offset); // total # bytes
        writer.finish();
      }
    } finally {
      IOUtils.close(data, index, offsets);
    }
  }
  
}
