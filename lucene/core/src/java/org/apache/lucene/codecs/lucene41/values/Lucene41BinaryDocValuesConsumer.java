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

import org.apache.lucene.codecs.BinaryDocValuesConsumer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.AppendingLongBuffer;
import org.apache.lucene.util.packed.AppendingLongBuffer.Iterator;
import org.apache.lucene.util.packed.PackedInts;

public class Lucene41BinaryDocValuesConsumer extends BinaryDocValuesConsumer {
  
  private final IndexOutput dataOut;
  private final IndexOutput indexOut;
  // nocommit: remove buffering!
  private final AppendingLongBuffer buffer;
  private long offset = 0;
  private long prevOffset = 0;
  static final int VERSION_START = -1;
  static final String CODEC_NAME = "Lucene41Binary";
  static final int VALUE_SIZE_VAR = -1;

  public Lucene41BinaryDocValuesConsumer(IndexOutput dataOut, IndexOutput indexOut, boolean fixedLength, int maxLength) throws IOException {
    this.dataOut = dataOut;
    this.indexOut = indexOut;
    CodecUtil.writeHeader(dataOut, CODEC_NAME, VERSION_START);
    dataOut.writeInt(fixedLength ? maxLength: VALUE_SIZE_VAR);
    dataOut.writeInt(maxLength);
    CodecUtil.writeHeader(indexOut, CODEC_NAME, VERSION_START);
    buffer = fixedLength ? null : new AppendingLongBuffer();
    
  }
  
  @Override
  public void finish() throws IOException {
    try {
      if (buffer != null) {
        Iterator iterator = buffer.iterator();
        PackedInts.Writer writer = PackedInts.getWriter(indexOut,
            buffer.size() + 1, PackedInts.bitsRequired(offset),
            PackedInts.FASTEST);
        long previous = 0;
        while (iterator.hasNext()) {
          long next = iterator.next() + previous;
          previous = next;
          writer.add(next);
        }
        writer.add(offset);
        writer.finish();
      }
    } finally {
      IOUtils.close(indexOut, dataOut);
    }
  }

  @Override
  public void add(BytesRef value) throws IOException {
    dataOut.writeBytes(value.bytes, value.offset, value.length);
    if (buffer != null) {
      buffer.add(offset-prevOffset);
      prevOffset = offset;
    }
    offset += value.length;
  }
  
}
