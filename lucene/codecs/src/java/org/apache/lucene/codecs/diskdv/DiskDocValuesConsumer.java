package org.apache.lucene.codecs.diskdv;

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
import java.util.Iterator;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.FormatAndBits;

// nocommit fix exception handling (make sure tests find problems first)
class DiskDocValuesConsumer extends DocValuesConsumer {
  final IndexOutput data, meta;
  final int maxDoc;
  
  DiskDocValuesConsumer(SegmentWriteState state) throws IOException {
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "ddvd");
    data = state.directory.createOutput(dataName, state.context);
    CodecUtil.writeHeader(data, DiskDocValuesFormat.DATA_CODEC, 
                                DiskDocValuesFormat.VERSION_CURRENT);
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "ddvm");
    meta = state.directory.createOutput(metaName, state.context);
    CodecUtil.writeHeader(meta, DiskDocValuesFormat.METADATA_CODEC, 
                                DiskDocValuesFormat.VERSION_CURRENT);
    maxDoc = state.segmentInfo.getDocCount();
  }
  
  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    meta.writeVInt(field.number);
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    int count = 0;
    for(Number nv : values) {
      long v = nv.longValue();
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
      count++;
    }
    meta.writeLong(minValue);
    long delta = maxValue - minValue;
    final int bitsPerValue;
    if (delta < 0) {
      bitsPerValue = 64;
    } else {
      bitsPerValue = PackedInts.bitsRequired(delta);
    }
    FormatAndBits formatAndBits = PackedInts.fastestFormatAndBits(count, bitsPerValue, PackedInts.COMPACT);
    
    // nocommit: refactor this crap in PackedInts.java
    // e.g. Header.load()/save() or something rather than how it works now.
    CodecUtil.writeHeader(meta, PackedInts.CODEC_NAME, PackedInts.VERSION_CURRENT);
    meta.writeVInt(bitsPerValue);
    meta.writeVInt(count);
    meta.writeVInt(formatAndBits.format.getId());
    
    meta.writeLong(data.getFilePointer());
    
    final PackedInts.Writer writer = PackedInts.getWriterNoHeader(data, formatAndBits.format, count, formatAndBits.bitsPerValue, 0);
    for(Number nv : values) {
      writer.add(nv.longValue() - minValue);
    }
    writer.finish();
  }

  @Override
  public void addBinaryField(FieldInfo field, final Iterable<BytesRef> values) throws IOException {
    // write the byte[] data
    meta.writeVInt(field.number);
    int minLength = Integer.MAX_VALUE;
    int maxLength = Integer.MIN_VALUE;
    final long startFP = data.getFilePointer();
    int count = 0;
    for(BytesRef v : values) {
      minLength = Math.min(minLength, v.length);
      maxLength = Math.max(maxLength, v.length);
      data.writeBytes(v.bytes, v.offset, v.length);
      count++;
    }
    meta.writeVInt(minLength);
    meta.writeVInt(maxLength);
    meta.writeVInt(count);
    meta.writeLong(startFP);
    
    // if minLength == maxLength, its a fixed-length byte[], we are done (the addresses are implicit)
    // otherwise, we need to record the length fields...
    // TODO: make this more efficient. this is just as inefficient as 4.0 codec.... we can do much better.
    if (minLength != maxLength) {
      addNumericField(field, new Iterable<Number>() {
        @Override
        public Iterator<Number> iterator() {
          final Iterator<BytesRef> inner = values.iterator();
          return new Iterator<Number>() {
            long addr = 0;

            @Override
            public boolean hasNext() {
              return inner.hasNext();
            }

            @Override
            public Number next() {
              BytesRef b = inner.next();
              addr += b.length;
              return Long.valueOf(addr);
            }

            @Override
            public void remove() { throw new UnsupportedOperationException(); } 
          };
        }
      });
    }
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    addBinaryField(field, values);
    addNumericField(field, docToOrd);
  }
  
  @Override
  public void close() throws IOException {
    // nocommit: just write this to a RAMfile or something and flush it here, with #fields first.
    // this meta is a tiny file so this hurts nobody
    meta.writeVInt(-1);
    IOUtils.close(data, meta);
  }
}
