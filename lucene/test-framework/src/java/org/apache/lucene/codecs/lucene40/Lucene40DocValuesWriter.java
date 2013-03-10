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
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

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
  private final static String segmentSuffix = "dv";

  // note: intentionally ignores seg suffix
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
    
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
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
    HashSet<BytesRef> uniqueValues = new HashSet<BytesRef>();
    int minLength = Integer.MAX_VALUE;
    int maxLength = Integer.MIN_VALUE;
    for (BytesRef b : values) {
      minLength = Math.min(minLength, b.length);
      maxLength = Math.max(maxLength, b.length);
      if (uniqueValues != null) {
        if (uniqueValues.add(BytesRef.deepCopyOf(b))) {
          if (uniqueValues.size() > 256) {
            uniqueValues = null;
          }
        }
      }
    }
    
    int maxDoc = state.segmentInfo.getDocCount();
    final boolean fixed = minLength == maxLength;
    final boolean dedup = uniqueValues != null && uniqueValues.size() * 2 < maxDoc;
    
    if (dedup) {
      // we will deduplicate and deref values
      boolean success = false;
      IndexOutput data = null;
      IndexOutput index = null;
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
      String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
      try {
        data = dir.createOutput(dataName, state.context);
        index = dir.createOutput(indexName, state.context);
        if (fixed) {
          addFixedDerefBytesField(field, data, index, values, minLength);
        } else {
          addVarDerefBytesField(field, data, index, values);
        }
        success = true;
      } finally {
        if (success) {
          IOUtils.close(data, index);
        } else {
          IOUtils.closeWhileHandlingException(data, index);
        }
      }
    } else {
      // we dont deduplicate, just write values straight
      if (fixed) {
        // fixed byte[]
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
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
        String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
        String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
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
    
    /* values */
    
    final long startPos = data.getFilePointer();
    
    for (BytesRef v : values) {
      data.writeBytes(v.bytes, v.offset, v.length);
    }
    
    /* addresses */
    
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
  
  private void addFixedDerefBytesField(FieldInfo field, IndexOutput data, IndexOutput index, Iterable<BytesRef> values, int length) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.BYTES_FIXED_DEREF.name());

    CodecUtil.writeHeader(data, 
                          Lucene40DocValuesFormat.BYTES_FIXED_DEREF_CODEC_NAME_DAT,
                          Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_CURRENT);
    
    CodecUtil.writeHeader(index, 
                          Lucene40DocValuesFormat.BYTES_FIXED_DEREF_CODEC_NAME_IDX,
                          Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_CURRENT);
    
    // deduplicate
    TreeSet<BytesRef> dictionary = new TreeSet<BytesRef>();
    for (BytesRef v : values) {
      dictionary.add(BytesRef.deepCopyOf(v));
    }
    
    /* values */
    data.writeInt(length);
    for (BytesRef v : dictionary) {
      data.writeBytes(v.bytes, v.offset, v.length);
    }
    
    /* ordinals */
    int valueCount = dictionary.size();
    assert valueCount > 0;
    index.writeInt(valueCount);
    final int maxDoc = state.segmentInfo.getDocCount();
    final PackedInts.Writer w = PackedInts.getWriter(index, maxDoc, PackedInts.bitsRequired(valueCount-1), PackedInts.DEFAULT);

    for (BytesRef v : values) {
      int ord = dictionary.headSet(v).size();
      w.add(ord);
    }
    w.finish();
  }
  
  private void addVarDerefBytesField(FieldInfo field, IndexOutput data, IndexOutput index, Iterable<BytesRef> values) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.BYTES_VAR_DEREF.name());

    CodecUtil.writeHeader(data, 
                          Lucene40DocValuesFormat.BYTES_VAR_DEREF_CODEC_NAME_DAT,
                          Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_CURRENT);
    
    CodecUtil.writeHeader(index, 
                          Lucene40DocValuesFormat.BYTES_VAR_DEREF_CODEC_NAME_IDX,
                          Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_CURRENT);
    
    // deduplicate
    TreeSet<BytesRef> dictionary = new TreeSet<BytesRef>();
    for (BytesRef v : values) {
      dictionary.add(BytesRef.deepCopyOf(v));
    }
    
    /* values */
    long startPosition = data.getFilePointer();
    long currentAddress = 0;
    HashMap<BytesRef,Long> valueToAddress = new HashMap<BytesRef,Long>();
    for (BytesRef v : dictionary) {
      currentAddress = data.getFilePointer() - startPosition;
      valueToAddress.put(v, currentAddress);
      writeVShort(data, v.length);
      data.writeBytes(v.bytes, v.offset, v.length);
    }
    
    /* ordinals */
    long totalBytes = data.getFilePointer() - startPosition;
    index.writeLong(totalBytes);
    final int maxDoc = state.segmentInfo.getDocCount();
    final PackedInts.Writer w = PackedInts.getWriter(index, maxDoc, PackedInts.bitsRequired(currentAddress), PackedInts.DEFAULT);

    for (BytesRef v : values) {
      w.add(valueToAddress.get(v));
    }
    w.finish();
  }
  
  // the little vint encoding used for var-deref
  private static void writeVShort(IndexOutput o, int i) throws IOException {
    assert i >= 0 && i <= Short.MAX_VALUE;
    if (i < 128) {
      o.writeByte((byte)i);
    } else {
      o.writeByte((byte) (0x80 | (i >> 8)));
      o.writeByte((byte) (i & 0xff));
    }
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    // examine the values to determine best type to use
    int minLength = Integer.MAX_VALUE;
    int maxLength = Integer.MIN_VALUE;
    for (BytesRef b : values) {
      minLength = Math.min(minLength, b.length);
      maxLength = Math.max(maxLength, b.length);
    }
    
    boolean success = false;
    IndexOutput data = null;
    IndexOutput index = null;
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
    String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
    
    try {
      data = dir.createOutput(dataName, state.context);
      index = dir.createOutput(indexName, state.context);
      if (minLength == maxLength) {
        // fixed byte[]
        addFixedSortedBytesField(field, data, index, values, docToOrd, minLength);
      } else {
        // var byte[]
        addVarSortedBytesField(field, data, index, values, docToOrd);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, index);
      } else {
        IOUtils.closeWhileHandlingException(data, index);
      }
    }
  }
  
  private void addFixedSortedBytesField(FieldInfo field, IndexOutput data, IndexOutput index, Iterable<BytesRef> values, Iterable<Number> docToOrd, int length) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.BYTES_FIXED_SORTED.name());

    CodecUtil.writeHeader(data, 
                          Lucene40DocValuesFormat.BYTES_FIXED_SORTED_CODEC_NAME_DAT,
                          Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_CURRENT);
    
    CodecUtil.writeHeader(index, 
                          Lucene40DocValuesFormat.BYTES_FIXED_SORTED_CODEC_NAME_IDX,
                          Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_CURRENT);
    
    /* values */
    
    data.writeInt(length);
    int valueCount = 0;
    for (BytesRef v : values) {
      data.writeBytes(v.bytes, v.offset, v.length);
      valueCount++;
    }
    
    /* ordinals */
    
    index.writeInt(valueCount);
    int maxDoc = state.segmentInfo.getDocCount();
    assert valueCount > 0;
    final PackedInts.Writer w = PackedInts.getWriter(index, maxDoc, PackedInts.bitsRequired(valueCount-1), PackedInts.DEFAULT);
    for (Number n : docToOrd) {
      w.add(n.longValue());
    }
    w.finish();
  }
  
  private void addVarSortedBytesField(FieldInfo field, IndexOutput data, IndexOutput index, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    field.putAttribute(legacyKey, LegacyDocValuesType.BYTES_VAR_SORTED.name());
    
    CodecUtil.writeHeader(data, 
                          Lucene40DocValuesFormat.BYTES_VAR_SORTED_CODEC_NAME_DAT,
                          Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_CURRENT);

    CodecUtil.writeHeader(index, 
                          Lucene40DocValuesFormat.BYTES_VAR_SORTED_CODEC_NAME_IDX,
                          Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_CURRENT);

    /* values */
    
    final long startPos = data.getFilePointer();
    
    int valueCount = 0;
    for (BytesRef v : values) {
      data.writeBytes(v.bytes, v.offset, v.length);
      valueCount++;
    }
    
    /* addresses */
    
    final long maxAddress = data.getFilePointer() - startPos;
    index.writeLong(maxAddress);
    
    assert valueCount != Integer.MAX_VALUE; // unsupported by the 4.0 impl
    
    final PackedInts.Writer w = PackedInts.getWriter(index, valueCount+1, PackedInts.bitsRequired(maxAddress), PackedInts.DEFAULT);
    long currentPosition = 0;
    for (BytesRef v : values) {
      w.add(currentPosition);
      currentPosition += v.length;
    }
    // write sentinel
    assert currentPosition == maxAddress;
    w.add(currentPosition);
    w.finish();
    
    /* ordinals */
    
    final int maxDoc = state.segmentInfo.getDocCount();
    assert valueCount > 0;
    final PackedInts.Writer ords = PackedInts.getWriter(index, maxDoc, PackedInts.bitsRequired(valueCount-1), PackedInts.DEFAULT);
    for (Number n : docToOrd) {
      ords.add(n.longValue());
    }
    ords.finish();
  }
  
  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
    throw new UnsupportedOperationException("Lucene 4.0 does not support SortedSet docvalues");
  }

  @Override
  public void close() throws IOException {
    dir.close();
  }
}
