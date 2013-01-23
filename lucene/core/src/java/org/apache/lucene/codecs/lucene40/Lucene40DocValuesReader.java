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
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene40.Lucene40FieldInfosReader.LegacyDocValuesType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

class Lucene40DocValuesReader extends DocValuesProducer {
  private final Directory dir;
  private final SegmentReadState state;
  private final String legacyKey;

  // ram instances we have already loaded
  private final Map<Integer,NumericDocValues> numericInstances = 
      new HashMap<Integer,NumericDocValues>();
  private final Map<Integer,BinaryDocValues> binaryInstances = 
      new HashMap<Integer,BinaryDocValues>();
  private final Map<Integer,SortedDocValues> sortedInstances = 
      new HashMap<Integer,SortedDocValues>();
  
  Lucene40DocValuesReader(SegmentReadState state, String filename, String legacyKey) throws IOException {
    this.state = state;
    this.legacyKey = legacyKey;
    this.dir = new CompoundFileDirectory(state.directory, filename, state.context, false);
  }
  
  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericDocValues instance = numericInstances.get(field.number);
    if (instance == null) {
      switch(LegacyDocValuesType.valueOf(field.getAttribute(legacyKey))) {
        case VAR_INTS:
          instance = loadVarIntsField(field);
          break;
        case FIXED_INTS_8:
          instance = loadByteField(field);
          break;
        case FIXED_INTS_16:
          instance = loadShortField(field);
          break;
        case FIXED_INTS_32:
          instance = loadIntField(field);
          break;
        case FIXED_INTS_64:
          instance = loadLongField(field);
          break;
        case FLOAT_32:
          instance = loadFloatField(field);
          break;
        case FLOAT_64:
          instance = loadDoubleField(field);
          break;
        default: 
          throw new AssertionError();
      }
      numericInstances.put(field.number, instance);
    }
    return instance;
  }
  
  private NumericDocValues loadVarIntsField(FieldInfo field) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
    IndexInput input = dir.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40DocValuesFormat.VAR_INTS_CODEC_NAME, 
                                   Lucene40DocValuesFormat.VAR_INTS_VERSION_START, 
                                   Lucene40DocValuesFormat.VAR_INTS_VERSION_CURRENT);
      byte header = input.readByte();
      if (header == Lucene40DocValuesFormat.VAR_INTS_FIXED_64) {
        int maxDoc = state.segmentInfo.getDocCount();
        final long values[] = new long[maxDoc];
        for (int i = 0; i < values.length; i++) {
          values[i] = input.readLong();
        }
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            return values[docID];
          }
        };
      } else if (header == Lucene40DocValuesFormat.VAR_INTS_PACKED) {
        final long minValue = input.readLong();
        final long defaultValue = input.readLong();
        final PackedInts.Reader reader = PackedInts.getReader(input);
        return new NumericDocValues() {
          @Override
          public long get(int docID) {
            final long value = reader.get(docID);
            if (value == defaultValue) {
              return 0;
            } else {
              return minValue + value;
            }
          }
        };
      } else {
        throw new CorruptIndexException("invalid VAR_INTS header byte: " + header + " (resource=" + input + ")");
      }
    } finally {
      if (success) {
        IOUtils.close(input);
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }
  
  private NumericDocValues loadByteField(FieldInfo field) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
    IndexInput input = dir.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME, 
                                   Lucene40DocValuesFormat.INTS_VERSION_START, 
                                   Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
      input.readInt();
      int maxDoc = state.segmentInfo.getDocCount();
      final byte values[] = new byte[maxDoc];
      input.readBytes(values, 0, values.length);
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return values[docID];
        }
      };
    } finally {
      if (success) {
        IOUtils.close(input);
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }
  
  private NumericDocValues loadShortField(FieldInfo field) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
    IndexInput input = dir.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME, 
                                   Lucene40DocValuesFormat.INTS_VERSION_START, 
                                   Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
      input.readInt();
      int maxDoc = state.segmentInfo.getDocCount();
      final short values[] = new short[maxDoc];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readShort();
      }
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return values[docID];
        }
      };
    } finally {
      if (success) {
        IOUtils.close(input);
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }
  
  private NumericDocValues loadIntField(FieldInfo field) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
    IndexInput input = dir.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME, 
                                   Lucene40DocValuesFormat.INTS_VERSION_START, 
                                   Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
      input.readInt();
      int maxDoc = state.segmentInfo.getDocCount();
      final int values[] = new int[maxDoc];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readInt();
      }
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return values[docID];
        }
      };
    } finally {
      if (success) {
        IOUtils.close(input);
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }
  
  private NumericDocValues loadLongField(FieldInfo field) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
    IndexInput input = dir.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME, 
                                   Lucene40DocValuesFormat.INTS_VERSION_START, 
                                   Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
      input.readInt();
      int maxDoc = state.segmentInfo.getDocCount();
      final long values[] = new long[maxDoc];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readLong();
      }
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return values[docID];
        }
      };
    } finally {
      if (success) {
        IOUtils.close(input);
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }
  
  private NumericDocValues loadFloatField(FieldInfo field) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
    IndexInput input = dir.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40DocValuesFormat.FLOATS_CODEC_NAME, 
                                   Lucene40DocValuesFormat.FLOATS_VERSION_START, 
                                   Lucene40DocValuesFormat.FLOATS_VERSION_CURRENT);
      input.readInt();
      int maxDoc = state.segmentInfo.getDocCount();
      final int values[] = new int[maxDoc];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readInt();
      }
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return values[docID];
        }
      };
    } finally {
      if (success) {
        IOUtils.close(input);
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }
  
  private NumericDocValues loadDoubleField(FieldInfo field) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, Integer.toString(field.number), "dat");
    IndexInput input = dir.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40DocValuesFormat.FLOATS_CODEC_NAME, 
                                   Lucene40DocValuesFormat.FLOATS_VERSION_START, 
                                   Lucene40DocValuesFormat.FLOATS_VERSION_CURRENT);
      input.readInt();
      int maxDoc = state.segmentInfo.getDocCount();
      final long values[] = new long[maxDoc];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readLong();
      }
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return values[docID];
        }
      };
    } finally {
      if (success) {
        IOUtils.close(input);
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }

  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo field) throws IOException {
    throw new AssertionError();
  }

  @Override
  public synchronized SortedDocValues getSorted(FieldInfo field) throws IOException {
    throw new AssertionError();
  }
  
  @Override
  public void close() throws IOException {
    dir.close();
  }
}
