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
import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDVProducer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
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
    public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
      meta.writeVInt(field.number);
      long minValue = Long.MAX_VALUE;
      long maxValue = Long.MIN_VALUE;
      for(Number nv : values) {
        long v = nv.longValue();
        minValue = Math.min(minValue, v);
        maxValue = Math.max(maxValue, v);
      }
      meta.writeLong(minValue);
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
      for(Number nv : values) {
        writer.add(nv.longValue() - minValue);
      }
      writer.finish();
    }

    @Override
    public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
      meta.writeVInt(field.number);
      // nocommit handle var length too!!
      int length = -1;
      for(BytesRef v : values) {
        if (length == -1) {
          length = v.length;
        } else if (length != v.length) {
          throw new UnsupportedOperationException();
        }
      }
      // nocommit don't hardwire fixedLength to 1:
      meta.writeByte((byte) 1);
      meta.writeVInt(length);
      meta.writeLong(data.getFilePointer());
      for(BytesRef value : values) {
        data.writeBytes(value.bytes, value.offset, value.length);
      }
    }

    @Override
    public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
      // nocommit todo
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
