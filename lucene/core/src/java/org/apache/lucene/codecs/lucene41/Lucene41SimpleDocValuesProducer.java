package org.apache.lucene.codecs.lucene41;

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
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.PackedInts;

class Lucene41SimpleDocValuesProducer extends DocValuesProducer {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<Integer,NumericEntry> numerics;
  private final Map<Integer,BinaryEntry> binaries;
  private final Map<Integer,FSTEntry> fsts;
  private final IndexInput data;
  
  // ram instances we have already loaded
  private final Map<Integer,NumericDocValues> numericInstances = 
      new HashMap<Integer,NumericDocValues>();
  
  // if this thing needs some TL state then we might put something
  // else in this map.
  private final Map<Integer,BinaryDocValues> binaryInstances =
      new HashMap<Integer,BinaryDocValues>();
  
  private final Map<Integer,FST<Long>> fstInstances =
      new HashMap<Integer,FST<Long>>();
    
  Lucene41SimpleDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    IndexInput in = state.directory.openInput(metaName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(in, metaCodec, 
                                Lucene41SimpleDocValuesConsumer.VERSION_START,
                                Lucene41SimpleDocValuesConsumer.VERSION_START);
      numerics = new HashMap<Integer,NumericEntry>();
      binaries = new HashMap<Integer,BinaryEntry>();
      fsts = new HashMap<Integer,FSTEntry>();
      readFields(in, state.fieldInfos);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }
    
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    data = state.directory.openInput(dataName, state.context);
    CodecUtil.checkHeader(data, dataCodec, 
                                Lucene41SimpleDocValuesConsumer.VERSION_START,
                                Lucene41SimpleDocValuesConsumer.VERSION_START);
  }
  
  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      int fieldType = meta.readByte();
      if (fieldType == Lucene41SimpleDocValuesConsumer.NUMBER) {
        NumericEntry entry = new NumericEntry();
        entry.offset = meta.readLong();
        entry.tableized = meta.readByte() != 0;
        numerics.put(fieldNumber, entry);
      } else if (fieldType == Lucene41SimpleDocValuesConsumer.BYTES) {
        BinaryEntry entry = new BinaryEntry();
        entry.offset = meta.readLong();
        entry.numBytes = meta.readLong();
        entry.minLength = meta.readVInt();
        entry.maxLength = meta.readVInt();
        binaries.put(fieldNumber, entry);
      } else if (fieldType == Lucene41SimpleDocValuesConsumer.FST) {
        FSTEntry entry = new FSTEntry();
        entry.offset = meta.readLong();
        entry.numOrds = meta.readVInt();
        fsts.put(fieldNumber, entry);
      } else {
        throw new CorruptIndexException("invalid entry type: " + fieldType + ", input=" + meta);
      }
      fieldNumber = meta.readVInt();
    }
  }

  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericDocValues instance = numericInstances.get(field.number);
    if (instance == null) {
      instance = loadNumeric(field);
      numericInstances.put(field.number, instance);
    }
    return instance;
  }
  
  private NumericDocValues loadNumeric(FieldInfo field) throws IOException {
    NumericEntry entry = numerics.get(field.number);
    data.seek(entry.offset);
    if (entry.tableized) {
      int size = data.readVInt();
      final long decode[] = new long[size];
      for (int i = 0; i < decode.length; i++) {
        decode[i] = data.readLong();
      }
      final long minValue = data.readLong();
      assert minValue == 0;
      final PackedInts.Reader reader = PackedInts.getReader(data);
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return decode[(int)reader.get(docID)];
        }
      };
    } else {
      final long minValue = data.readLong();
      final PackedInts.Reader reader = PackedInts.getReader(data);
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return minValue + reader.get(docID);
        }
      };
    }
  }

  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryDocValues instance = binaryInstances.get(field.number);
    if (instance == null) {
      instance = loadBinary(field);
      binaryInstances.put(field.number, instance);
    }
    return instance;
  }
  
  private BinaryDocValues loadBinary(FieldInfo field) throws IOException {
    BinaryEntry entry = binaries.get(field.number);
    data.seek(entry.offset);
    assert entry.numBytes < Integer.MAX_VALUE; // nocommit
    final byte[] bytes = new byte[(int)entry.numBytes];
    data.readBytes(bytes, 0, bytes.length);
    if (entry.minLength == entry.maxLength) {
      final int fixedLength = entry.minLength;
      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          result.bytes = bytes;
          result.offset = docID * fixedLength;
          result.length = fixedLength;
        }
      };
    } else {
      final NumericDocValues addresses = getNumeric(field);
      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          int startAddress = docID == 0 ? 0 : (int) addresses.get(docID-1);
          int endAddress = (int)addresses.get(docID); 
          result.bytes = bytes;
          result.offset = startAddress;
          result.length = endAddress - startAddress;
        }
      };
    }
  }
  
  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    final FSTEntry entry = fsts.get(field.number);
    FST<Long> instance;
    synchronized(this) {
      instance = fstInstances.get(field.number);
      if (instance == null) {
        data.seek(entry.offset);
        instance = new FST<Long>(data, PositiveIntOutputs.getSingleton(true));
        fstInstances.put(field.number, instance);
      }
    }
    final NumericDocValues docToOrd = getNumeric(field);
    final FST<Long> fst = instance;
    
    // per-thread resources
    final BytesReader in = fst.getBytesReader(0);
    final Arc<Long> firstArc = new Arc<Long>();
    final Arc<Long> scratchArc = new Arc<Long>();
    final IntsRef scratchInts = new IntsRef();
    final BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum<Long>(fst); 
    
    return new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return (int) docToOrd.get(docID);
      }

      @Override
      public void lookupOrd(int ord, BytesRef result) {
        try {
          in.setPosition(0);
          fst.getFirstArc(firstArc);
          Util.toBytesRef(Util.getByOutput(fst, ord, in, firstArc, scratchArc, scratchInts), result);
        } catch (IOException bogus) {
          throw new RuntimeException(bogus);
        }
      }

      @Override
      public int lookupTerm(BytesRef key, BytesRef spare) {
        try {
          InputOutput<Long> o = fstEnum.seekCeil(key);
          if (o == null) {
            return -getValueCount()-1;
          } else if (o.input.equals(key)) {
            return o.output.intValue();
          } else {
            return (int) -o.output-1;
          }
        } catch (IOException bogus) {
          throw new RuntimeException(bogus);
        }
      }

      @Override
      public int getValueCount() {
        return entry.numOrds;
      }
    };
  }
  
  @Override
  public void close() throws IOException {
    data.close();
  }
  
  static class NumericEntry {
    long offset;
    boolean tableized;
  }
  
  static class BinaryEntry {
    long offset;
    long numBytes;
    int minLength;
    int maxLength;
  }
  
  static class FSTEntry {
    long offset;
    int numOrds;
  }
}
