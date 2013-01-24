package org.apache.lucene.codecs.lucene40;

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
import org.apache.lucene.codecs.lucene40.Lucene40FieldInfosReader.LegacyDocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

class Lucene40DocValuesWriter extends DocValuesConsumer {
  private final Directory dir;
  private final SegmentWriteState state;
  private final String legacyKey;

  // note: intentionally ignores seg suffix
  // String filename = IndexFileNames.segmentFileName(state.segmentInfo.name, "dv", IndexFileNames.COMPOUND_FILE_EXTENSION);
  Lucene40DocValuesWriter(SegmentWriteState state, String filename, String legacyKey) throws IOException {
    this.state = state;
    this.legacyKey = legacyKey;
    this.dir = new CompoundFileDirectory(state.directory, filename, state.context, true);
  }
  
  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    // examine the values to determine best type to use
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    for (Number n : values) {
      long v = n.longValue();
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
    }
    
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
    IndexOutput data = dir.createOutput(fileName, state.context);
    boolean success = false;
    try {
      if (minValue >= Byte.MIN_VALUE && maxValue <= Byte.MAX_VALUE && PackedInts.bitsRequired(maxValue-minValue) > 4) {
        // fits in a byte[], would be more than 4bpv, just write byte[]
        addBytesField(field, data, values);
      } else if (minValue >= Short.MIN_VALUE && maxValue <= Short.MAX_VALUE && PackedInts.bitsRequired(maxValue-minValue) > 8) {
        // fits in a short[], would be more than 8bpv, just write short[]
        addShortsField(field, data, values);
      } else if (minValue >= Integer.MIN_VALUE && maxValue <= Integer.MAX_VALUE && PackedInts.bitsRequired(maxValue-minValue) > 16) {
        // fits in a int[], would be more than 16bpv, just write int[]
        addIntsField(field, data, values);
      } else {
        addVarIntsField(field, data, values, minValue, maxValue);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data);
      } else {
        IOUtils.closeWhileHandlingException(data);
      }
    }
  }

  private void addBytesField(FieldInfo field, IndexOutput output, Iterable<Number> values) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.FIXED_INTS_8.name());
    CodecUtil.writeHeader(output, 
                          Lucene40DocValuesFormat.INTS_CODEC_NAME, 
                          Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    output.writeInt(1); // size
    for (Number n : values) {
      output.writeByte(n.byteValue());
    }
  }
  
  private void addShortsField(FieldInfo field, IndexOutput output, Iterable<Number> values) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.FIXED_INTS_16.name());
    CodecUtil.writeHeader(output, 
                          Lucene40DocValuesFormat.INTS_CODEC_NAME, 
                          Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    output.writeInt(2); // size
    for (Number n : values) {
      output.writeShort(n.shortValue());
    }
  }
  
  private void addIntsField(FieldInfo field, IndexOutput output, Iterable<Number> values) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.FIXED_INTS_32.name());
    CodecUtil.writeHeader(output, 
                          Lucene40DocValuesFormat.INTS_CODEC_NAME, 
                          Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    output.writeInt(4); // size
    for (Number n : values) {
      output.writeInt(n.intValue());
    }
  }
  
  private void addVarIntsField(FieldInfo field, IndexOutput output, Iterable<Number> values, long minValue, long maxValue) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.VAR_INTS.name());
    
    CodecUtil.writeHeader(output, 
                          Lucene40DocValuesFormat.VAR_INTS_CODEC_NAME, 
                          Lucene40DocValuesFormat.VAR_INTS_VERSION_CURRENT);
    
    final long delta = maxValue - minValue;
    
    if (delta < 0) {
      // writes longs
      output.writeByte(Lucene40DocValuesFormat.VAR_INTS_FIXED_64);
      for (Number n : values) {
        output.writeLong(n.longValue());
      }
    } else {
      // writes packed ints
      output.writeByte(Lucene40DocValuesFormat.VAR_INTS_PACKED);
      output.writeLong(minValue);
      output.writeLong(0 - minValue); // default value (representation of 0)
      PackedInts.Writer writer = PackedInts.getWriter(output, 
                                                      state.segmentInfo.getDocCount(),
                                                      PackedInts.bitsRequired(delta), 
                                                      PackedInts.DEFAULT);
      for (Number n : values) {
        writer.add(n.longValue() - minValue);
      }
      writer.finish();
    }
  }

  @Override
  public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
    // examine the values to determine best type to use
    // TODO: would be cool to write the deref types in this impersonator too
    int minLength = Integer.MAX_VALUE;
    int maxLength = Integer.MIN_VALUE;
    for (BytesRef b : values) {
      minLength = Math.min(minLength, b.length);
      maxLength = Math.max(maxLength, b.length);
    }
    
    if (minLength == maxLength) {
      // fixed byte[]
      String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
      IndexOutput data = dir.createOutput(fileName, state.context);
      boolean success = false;
      try {
        addFixedStraightBytesField(field, data, values, minLength);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(data);
        } else {
          IOUtils.closeWhileHandlingException(data);
        }
      }
    } else {
      // variable byte[]
      boolean success = false;
      IndexOutput data = null;
      IndexOutput index = null;
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
      String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "idx");
      try {
        data = dir.createOutput(dataName, state.context);
        index = dir.createOutput(indexName, state.context);
        addVarStraightBytesField(field, data, index, values);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(data, index);
        } else {
          IOUtils.closeWhileHandlingException(data, index);
        }
      }
    }
  }
  
  private void addFixedStraightBytesField(FieldInfo field, IndexOutput output, Iterable<BytesRef> values, int length) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.BYTES_FIXED_STRAIGHT.name());

    CodecUtil.writeHeader(output, 
                          Lucene40DocValuesFormat.BYTES_FIXED_STRAIGHT_CODEC_NAME,
                          Lucene40DocValuesFormat.BYTES_FIXED_STRAIGHT_VERSION_CURRENT);
    
    output.writeInt(length);
    for (BytesRef v : values) {
      output.writeBytes(v.bytes, v.offset, v.length);
    }
  }
  
  // NOTE: 4.0 file format docs are crazy/wrong here...
  private void addVarStraightBytesField(FieldInfo field, IndexOutput data, IndexOutput index, Iterable<BytesRef> values) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.BYTES_VAR_STRAIGHT.name());
    
    CodecUtil.writeHeader(data, 
                          Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_CODEC_NAME_DAT,
                          Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_CURRENT);
    
    CodecUtil.writeHeader(index, 
                          Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_CODEC_NAME_IDX,
                          Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_CURRENT);
    
    final long startPos = data.getFilePointer();
    
    for (BytesRef v : values) {
      data.writeBytes(v.bytes, v.offset, v.length);
    }
    
    final long maxAddress = data.getFilePointer() - startPos;
    index.writeVLong(maxAddress);
    
    final int maxDoc = state.segmentInfo.getDocCount();
    assert maxDoc != Integer.MAX_VALUE; // unsupported by the 4.0 impl
    
    final PackedInts.Writer w = PackedInts.getWriter(index, maxDoc+1, PackedInts.bitsRequired(maxAddress), PackedInts.DEFAULT);
    long currentPosition = 0;
    for (BytesRef v : values) {
      w.add(currentPosition);
      currentPosition += v.length;
    }
    // write sentinel
    assert currentPosition == maxAddress;
    w.add(currentPosition);
    w.finish();
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    assert false;
  }  
  
  @Override
  public void close() throws IOException {
    dir.close();
  }
}
