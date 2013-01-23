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
    // TODO: examine the values: and simulate all the possibilities.
    // e.g. if all values fit in a byte, write a fixed_8 etc.
    field.putAttribute(legacyKey, LegacyDocValuesType.VAR_INTS.name());
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
    IndexOutput data = dir.createOutput(fileName, state.context);
    boolean success = false;
    try {
      addVarIntsField(data, values);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data);
      } else {
        IOUtils.closeWhileHandlingException(data);
      }
    }
  }

  @Override
  public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
    assert false;
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    assert false;
  }
  
  @Override
  public void close() throws IOException {
    dir.close();
  }
  
  private void addVarIntsField(IndexOutput output, Iterable<Number> values) throws IOException {
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    for (Number n : values) {
      long v = n.longValue();
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
    }
    
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
}
