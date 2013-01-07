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

import org.apache.lucene.codecs.BinaryDocValuesConsumer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NumericDocValuesConsumer;
import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDVProducer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.codecs.SortedDocValuesConsumer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.FormatAndBits;

public class Lucene41SimpleDocValuesFormat extends SimpleDocValuesFormat {

  public Lucene41SimpleDocValuesFormat() {
    super("Lucene41");
  }

  @Override
  public SimpleDVConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene41SimpleDocValuesConsumer(state);
  }

  @Override
  public SimpleDVProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Lucene41SimpleDocValuesProducer(state);
  }
  
  static class Lucene41SimpleDocValuesConsumer extends SimpleDVConsumer {
    final IndexOutput data, meta;
    final int maxDoc;
    
    Lucene41SimpleDocValuesConsumer(SegmentWriteState state) throws IOException {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "dvd");
      data = state.directory.createOutput(dataName, state.context);
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "dvm");
      meta = state.directory.createOutput(metaName, state.context);
      maxDoc = state.segmentInfo.getDocCount();
    }
    
    @Override
    public NumericDocValuesConsumer addNumericField(FieldInfo field, final long minValue, long maxValue) throws IOException {
      meta.writeVInt(field.number);
      meta.writeLong(minValue);
      meta.writeLong(maxValue);
      long delta = maxValue - minValue;
      final int bitsPerValue;
      if (delta < 0) {
        bitsPerValue = 64;
      } else {
        bitsPerValue = PackedInts.bitsRequired(delta);
      }
      FormatAndBits formatAndBits = PackedInts.fastestFormatAndBits(maxDoc, bitsPerValue, PackedInts.COMPACT);
      
      // nocommit: refactor this crap in PackedInts.java
      // e.g. Header.load()/save() or something rather than how it works now.
      CodecUtil.writeHeader(meta, PackedInts.CODEC_NAME, PackedInts.VERSION_CURRENT);
      meta.writeVInt(bitsPerValue);
      meta.writeVInt(maxDoc);
      meta.writeVInt(formatAndBits.format.getId());
      
      meta.writeLong(data.getFilePointer());
      
      final PackedInts.Writer writer = PackedInts.getWriterNoHeader(data, formatAndBits.format, maxDoc, formatAndBits.bitsPerValue, 0);
      return new NumericDocValuesConsumer() {
        @Override
        public void add(long value) throws IOException {
          writer.add(value - minValue);
        }

        @Override
        public void finish() throws IOException {
          writer.finish();
        }
      };
    }

    @Override
    public BinaryDocValuesConsumer addBinaryField(FieldInfo field, boolean fixedLength, int maxLength) throws IOException {
      meta.writeVInt(field.number);
      meta.writeByte(fixedLength ? (byte)1 : 0);
      meta.writeVInt(maxLength);
      meta.writeLong(data.getFilePointer());
      if (fixedLength) {
        return new BinaryDocValuesConsumer() {
          @Override
          public void add(BytesRef value) throws IOException {
            data.writeBytes(value.bytes, value.offset, value.length);
          }

          @Override
          public void finish() throws IOException {}
        };
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public SortedDocValuesConsumer addSortedField(FieldInfo field, int valueCount, boolean fixedLength, int maxLength) throws IOException {
      return null;
    }
    
    @Override
    public void close() throws IOException {
      // nocommit: just write this to a RAMfile or something and flush it here, with #fields first.
      // this meta is a tiny file so this hurts nobody
      meta.writeVInt(-1);
      IOUtils.close(data, meta);
    }
  }
  
  static class NumericEntry {
    long offset;
    
    long minValue;
    long maxValue;
    PackedInts.Header header;
  }
  
  static class BinaryEntry {
    long offset;

    boolean fixedLength;
    int maxLength;
  }
  
  static class Lucene41SimpleDocValuesProducer extends SimpleDVProducer {
    private final Map<Integer,NumericEntry> numerics;
    private final Map<Integer,BinaryEntry> binaries;
    private final IndexInput data;
    private final int maxDoc;
    
    Lucene41SimpleDocValuesProducer(SegmentReadState state) throws IOException {
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "dvm");
      // slurpy slurp
      IndexInput in = state.directory.openInput(metaName, state.context);
      boolean success = false;
      try {
        numerics = new HashMap<Integer,NumericEntry>();
        binaries = new HashMap<Integer,BinaryEntry>();
        readFields(numerics, binaries, in, state.fieldInfos);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(in);
        } else {
          IOUtils.closeWhileHandlingException(in);
        }
      }
      
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "dvd");
      data = state.directory.openInput(dataName, state.context);
      maxDoc = state.segmentInfo.getDocCount();
    }
    
    // used by clone()
    Lucene41SimpleDocValuesProducer(IndexInput data, Map<Integer,NumericEntry> numerics, Map<Integer,BinaryEntry> binaries, int maxDoc) {
      this.data = data;
      this.numerics = numerics;
      this.binaries = binaries;
      this.maxDoc = maxDoc;
    }
    
    static void readFields(Map<Integer,NumericEntry> numerics, Map<Integer,BinaryEntry> binaries, IndexInput meta, FieldInfos infos) throws IOException {
      int fieldNumber = meta.readVInt();
      while (fieldNumber != -1) {
        DocValues.Type type = infos.fieldInfo(fieldNumber).getDocValuesType();
        if (DocValues.isNumber(type) || DocValues.isFloat(type)) {
          numerics.put(fieldNumber, readNumericField(meta));
        } else if (DocValues.isBytes(type)) {
          BinaryEntry b = readBinaryField(meta);
          binaries.put(fieldNumber, b);
          if (!b.fixedLength) {
            throw new AssertionError();
            // here we will read a numerics entry for the field, too.
            // it contains the addresses as ints.
          }
        }
        fieldNumber = meta.readVInt();
      }
    }
    
    static NumericEntry readNumericField(IndexInput meta) throws IOException {
      NumericEntry entry = new NumericEntry();
      entry.minValue = meta.readLong();
      entry.maxValue = meta.readLong();
      entry.header = PackedInts.readHeader(meta);
      entry.offset = meta.readLong();
      return entry;
    }
    
    static BinaryEntry readBinaryField(IndexInput meta) throws IOException {
      BinaryEntry entry = new BinaryEntry();
      entry.fixedLength = meta.readByte() != 0;
      entry.maxLength = meta.readVInt();
      entry.offset = meta.readLong();
      return entry;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
      final NumericEntry entry = numerics.get(field.number);
      final PackedInts.Reader reader = PackedInts.getDirectReaderNoHeader(data, entry.header);
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return entry.minValue + reader.get(docID);
        }

        @Override
        public long minValue() {
          return entry.minValue;
        }

        @Override
        public long maxValue() {
          return entry.maxValue;
        }

        @Override
        public int size() {
          return maxDoc;
        }
      };
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
      final BinaryEntry entry = binaries.get(field.number);
      assert entry.fixedLength;
      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          long address = entry.offset + docID * (long)entry.maxLength;
          try {
            data.seek(address);
            if (result.length < entry.maxLength) {
              result.offset = 0;
              result.bytes = new byte[entry.maxLength];
            }
            data.readBytes(result.bytes, result.offset, entry.maxLength);
            result.length = entry.maxLength;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public boolean isFixedLength() {
          return entry.fixedLength;
        }

        @Override
        public int maxLength() {
          return entry.maxLength;
        }
        
        @Override
        public int size() {
          return maxDoc;
        }
      };
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
      return null;
    }

    @Override
    public SimpleDVProducer clone() {
      return new Lucene41SimpleDocValuesProducer(data.clone(), numerics, binaries, maxDoc);
    }
    
    @Override
    public void close() throws IOException {
      data.close();
    }
  }
}
