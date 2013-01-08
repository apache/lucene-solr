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
import java.util.Iterator;
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
                return addr; // nocommit don't box
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
  
  static class NumericEntry {
    long offset;
    
    long minValue;
    PackedInts.Header header;
  }
  
  static class BinaryEntry {
    long offset;

    int count;
    int minLength;
    int maxLength;
  }
  
  static class Lucene41SimpleDocValuesProducer extends SimpleDVProducer {
    private final Map<Integer,NumericEntry> numerics;
    private final Map<Integer,NumericEntry> ords;
    private final Map<Integer,BinaryEntry> binaries;
    private final IndexInput data;
    
    Lucene41SimpleDocValuesProducer(SegmentReadState state) throws IOException {
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "dvm");
      // slurpy slurp
      IndexInput in = state.directory.openInput(metaName, state.context);
      boolean success = false;
      try {
        numerics = new HashMap<Integer,NumericEntry>();
        ords = new HashMap<Integer,NumericEntry>();
        binaries = new HashMap<Integer,BinaryEntry>();
        readFields(numerics, ords, binaries, in, state.fieldInfos);
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
    }
    
    static void readFields(Map<Integer,NumericEntry> numerics, Map<Integer,NumericEntry> ords, Map<Integer,BinaryEntry> binaries, IndexInput meta, FieldInfos infos) throws IOException {
      int fieldNumber = meta.readVInt();
      while (fieldNumber != -1) {
        DocValues.Type type = infos.fieldInfo(fieldNumber).getDocValuesType();
        if (DocValues.isNumber(type) || DocValues.isFloat(type)) {
          numerics.put(fieldNumber, readNumericField(meta));
        } else if (DocValues.isBytes(type)) {
          BinaryEntry b = readBinaryField(meta);
          binaries.put(fieldNumber, b);
          if (b.minLength != b.maxLength) {
            fieldNumber = meta.readVInt(); // waste
            // variable length byte[]: read addresses as a numeric dv field
            numerics.put(fieldNumber, readNumericField(meta));
          }
        } else if (DocValues.isSortedBytes(type)) {
          BinaryEntry b = readBinaryField(meta);
          binaries.put(fieldNumber, b);
          if (b.minLength != b.maxLength) {
            fieldNumber = meta.readVInt(); // waste
            // variable length byte[]: read addresses as a numeric dv field
            numerics.put(fieldNumber, readNumericField(meta));
          }
          // sorted byte[]: read ords as a numeric dv field
          fieldNumber = meta.readVInt(); // waste
          ords.put(fieldNumber, readNumericField(meta));
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
      entry.minLength = meta.readVInt();
      entry.maxLength = meta.readVInt();
      entry.count = meta.readVInt();
      entry.offset = meta.readLong();
      return entry;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
      // nocommit: user can currently get back a numericDV of the addresses...
      NumericEntry entry = numerics.get(field.number);
      return getNumeric(field, entry);
    }
    
    private NumericDocValues getNumeric(FieldInfo field, final NumericEntry entry) throws IOException {
      // nocommit: what are we doing with clone?!
      final IndexInput data = this.data.clone();
      data.seek(entry.offset);
      final PackedInts.Reader reader = PackedInts.getDirectReaderNoHeader(data, entry.header);
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return entry.minValue + reader.get(docID);
        }

        @Override
        public int size() {
          return entry.header.getValueCount();
        }
      };
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
      BinaryEntry bytes = binaries.get(field.number);
      if (bytes.minLength == bytes.maxLength) {
        return getFixedBinary(field, bytes);
      } else {
        return getVariableBinary(field, bytes);
      }
    }
    
    private BinaryDocValues getFixedBinary(FieldInfo field, final BinaryEntry bytes) {
      // nocommit: what are we doing with clone?!
      final IndexInput data = this.data.clone();
      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          long address = bytes.offset + docID * (long)bytes.maxLength;
          try {
            data.seek(address);
            if (result.length < bytes.maxLength) {
              result.offset = 0;
              result.bytes = new byte[bytes.maxLength];
            }
            data.readBytes(result.bytes, result.offset, bytes.maxLength);
            result.length = bytes.maxLength;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public int size() {
          return bytes.count;
        }
      };
    }
    
    private BinaryDocValues getVariableBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
      // nocommit: what are we doing with clone?!
      final IndexInput data = this.data.clone();
      final NumericDocValues addresses = getNumeric(field);
      return new BinaryDocValues() {
        @Override
        public void get(int docID, BytesRef result) {
          long startAddress = docID == 0 ? bytes.offset : bytes.offset + addresses.get(docID-1);
          long endAddress = bytes.offset + addresses.get(docID);
          int length = (int) (endAddress - startAddress);
          try {
            data.seek(startAddress);
            if (result.length < length) {
              result.offset = 0;
              result.bytes = new byte[length];
            }
            data.readBytes(result.bytes, result.offset, length);
            result.length = length;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public int size() {
          return bytes.count;
        }
      };
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
      final BinaryDocValues binary = getBinary(field);
      final NumericDocValues ordinals = getNumeric(field, ords.get(field.number));
      return new SortedDocValues() {

        @Override
        public int getOrd(int docID) {
          return (int) ordinals.get(docID);
        }

        @Override
        public void lookupOrd(int ord, BytesRef result) {
          binary.get(ord, result);
        }

        @Override
        public int getValueCount() {
          return ordinals.size();
        }

        @Override
        public int size() {
          return binary.size();
        }
      };
    }

    @Override
    public void close() throws IOException {
      data.close();
    }
  }
}
