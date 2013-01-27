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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

class DiskDocValuesConsumer extends DocValuesConsumer {

  static final int BLOCK_SIZE = 16384;

  final IndexOutput data, meta;
  final int maxDoc;
  
  DiskDocValuesConsumer(SegmentWriteState state) throws IOException {
    boolean success = false;
    try {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "ddvd");
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeHeader(data, DiskDocValuesFormat.DATA_CODEC, 
                                  DiskDocValuesFormat.VERSION_CURRENT);
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "ddvm");
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeHeader(meta, DiskDocValuesFormat.METADATA_CODEC, 
                                  DiskDocValuesFormat.VERSION_CURRENT);
      maxDoc = state.segmentInfo.getDocCount();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }
  
  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    int count = 0;
    for (@SuppressWarnings("unused") Number nv : values) {
      ++count;
    }

    meta.writeVInt(field.number);
    meta.writeVInt(PackedInts.VERSION_CURRENT);
    meta.writeLong(data.getFilePointer());
    meta.writeVInt(count);
    meta.writeVInt(BLOCK_SIZE);

    final BlockPackedWriter writer = new BlockPackedWriter(data, BLOCK_SIZE);
    for (Number nv : values) {
      writer.add(nv.longValue());
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
    if (minLength != maxLength) {
      meta.writeLong(data.getFilePointer());
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeVInt(BLOCK_SIZE);

      final MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(data, BLOCK_SIZE);
      long addr = 0;
      for (BytesRef v : values) {
        addr += v.length;
        writer.add(addr);
      }
      writer.finish();
    }
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    addBinaryField(field, values);
    addNumericField(field, docToOrd);
  }
  
  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeVInt(-1); // write EOF marker
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
    }
  }
}
