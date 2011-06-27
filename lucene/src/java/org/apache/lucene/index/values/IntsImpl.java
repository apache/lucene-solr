package org.apache.lucene.index.values;

/**
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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.values.IndexDocValuesArray;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.index.values.IndexDocValues.SourceEnum;
import org.apache.lucene.index.values.IndexDocValuesArray.ByteValues;
import org.apache.lucene.index.values.IndexDocValuesArray.IntValues;
import org.apache.lucene.index.values.IndexDocValuesArray.LongValues;
import org.apache.lucene.index.values.IndexDocValuesArray.ShortValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Stores ints packed and fixed with fixed-bit precision.
 * 
 * @lucene.experimental
 * */
class IntsImpl {

  private static final String CODEC_NAME = "Ints";
  private static final byte PACKED = 0x00;
  private static final byte FIXED_64 = 0x01;
  private static final byte FIXED_32 = 0x02;
  private static final byte FIXED_16 = 0x03;
  private static final byte FIXED_8 = 0x04;
  
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class IntsWriter extends Writer {

    // TODO: optimize merging here!!
    private LongsRef intsRef;
    private final IndexDocValuesArray array;
    private long minValue;
    private long maxValue;
    private boolean started;
    private final String id;
    private int lastDocId = -1;
    private final Directory dir;
    private final byte typeOrd;
    

    protected IntsWriter(Directory dir, String id, AtomicLong bytesUsed,
        ValueType valueType) throws IOException {
      super(bytesUsed);
      this.dir = dir;
      this.id = id;
      switch (valueType) {
      case FIXED_INTS_16:
        array= new ShortValues(bytesUsed);
        typeOrd = FIXED_16;
        break;
      case FIXED_INTS_32:
        array = new IntValues(bytesUsed);
        typeOrd = FIXED_32;
        break;
      case FIXED_INTS_64:
        array = new LongValues(bytesUsed);
        typeOrd = FIXED_64;
        break;
      case FIXED_INTS_8:
        array = new ByteValues(bytesUsed);
        typeOrd = FIXED_8;
        break;
      case VAR_INTS:
        array = new LongValues(bytesUsed);
        typeOrd = PACKED;
        break;
      default:
        throw new IllegalStateException("unknown type " + valueType);
      }
    }
    
    @Override
    public void add(int docID, long v) throws IOException {
      assert lastDocId < docID;
      if (!started) {
        started = true;
        minValue = maxValue = v;
      } else {
        if (v < minValue) {
          minValue = v;
        } else if (v > maxValue) {
          maxValue = v;
        }
      }
      lastDocId = docID;
      array.set(docID, v);
    }

    @Override
    public void finish(int docCount) throws IOException {
      IndexOutput datOut = null;
      boolean success = false;
      try {
        datOut = dir.createOutput(IndexFileNames.segmentFileName(id, "",
            DATA_EXTENSION));
        CodecUtil.writeHeader(datOut, CODEC_NAME, VERSION_CURRENT);
        if (!started) {
          minValue = maxValue = 0;
        }
        byte headerType = typeOrd;
        if (typeOrd == PACKED) {
          final long delta = maxValue - minValue;
          // if we exceed the range of positive longs we must switch to fixed ints
          if (delta <= ( maxValue >= 0 && minValue <= 0 ? Long.MAX_VALUE : Long.MAX_VALUE -1) &&  delta >= 0) {
            writePackedInts(datOut, docCount);
            return;
          } 
          headerType = FIXED_64;
        }
        datOut.writeByte(headerType);
        array.write(datOut, docCount);
        success = true;
      } finally {
        IOUtils.closeSafely(!success, datOut);
        array.clear();
      }
    }

    private void writePackedInts(IndexOutput datOut, int docCount) throws IOException {
      datOut.writeByte(PACKED);
      datOut.writeLong(minValue);
      assert array.type() == ValueType.FIXED_INTS_64;
      final long[] docToValue = (long[])array.getArray();
      // write a default value to recognize docs without a value for that
      // field
      final long defaultValue = maxValue >= 0 && minValue <= 0 ? 0 - minValue
          : ++maxValue - minValue;
      datOut.writeLong(defaultValue);
      PackedInts.Writer w = PackedInts.getWriter(datOut, docCount,
          PackedInts.bitsRequired(maxValue - minValue));
      final int limit = docToValue.length > docCount ? docCount
          : docToValue.length;
      for (int i = 0; i < limit; i++) {
        w.add(docToValue[i] == 0 ? defaultValue : docToValue[i] - minValue);
      }
      for (int i = limit; i < docCount; i++) {
        w.add(defaultValue);
      }

      w.finish();
    }

    @Override
    protected void mergeDoc(int docID) throws IOException {
      add(docID, intsRef.get());
    }

    @Override
    protected void setNextEnum(ValuesEnum valuesEnum) {
      intsRef = valuesEnum.getInt();
    }

    @Override
    public void add(int docID, PerDocFieldValues docValues) throws IOException {
      add(docID, docValues.getInt());
    }

    @Override
    public void files(Collection<String> files) throws IOException {
      files.add(IndexFileNames.segmentFileName(id, "", DATA_EXTENSION));
    }
  }

  /**
   * Opens all necessary files, but does not read any data in until you call
   * {@link #load}.
   */
  static class IntsReader extends IndexDocValues {
    private final IndexInput datIn;
    private final byte type;

    protected IntsReader(Directory dir, String id) throws IOException {
      datIn = dir.openInput(IndexFileNames.segmentFileName(id, "",
          Writer.DATA_EXTENSION));
      boolean success = false;
      try {
        CodecUtil.checkHeader(datIn, CODEC_NAME, VERSION_START, VERSION_START);
        type = datIn.readByte();
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeSafely(true, datIn);
        }
      }
    }

    /**
     * Loads the actual values. You may call this more than once, eg if you
     * already previously loaded but then discarded the Source.
     */
    @Override
    public Source load() throws IOException {
      boolean success = false;
      final Source source;
      IndexInput input = null;
      try {
        input = (IndexInput) datIn.clone();
        input.seek(CodecUtil.headerLength(CODEC_NAME) + 1);
        source  = loadFixedSource(type, input);
        success = true;
        return source;
      } finally {
        if (!success) {
          IOUtils.closeSafely(true, input, datIn);
        }
      }
    }

    @Override
    public void close() throws IOException {
      super.close();
      datIn.close();
    }

    @Override
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      final IndexInput input = (IndexInput) datIn.clone();
      boolean success = false;
      try {
        input.seek(CodecUtil.headerLength(CODEC_NAME) + 1);
        final ValuesEnum inst = directEnum(type, source, input);
        success = true;
        return inst;
      } finally {
        if (!success) {
          IOUtils.closeSafely(true, input);
        }
      }
    }

    @Override
    public ValueType type() {
      return ValueType.VAR_INTS;
    }
  }
  
  private static ValuesEnum directEnum(byte ord, AttributeSource attrSource, IndexInput input) throws IOException {
    switch (ord) {
    case FIXED_16:
      return new ShortValues((AtomicLong)null).getDirectEnum(attrSource, input);
    case FIXED_32:
      return new IntValues((AtomicLong)null).getDirectEnum(attrSource, input);
    case FIXED_64:
      return new LongValues((AtomicLong)null).getDirectEnum(attrSource, input);
    case FIXED_8:
      return new ByteValues((AtomicLong)null).getDirectEnum(attrSource, input);
    case PACKED:
      return new PackedIntsEnumImpl(attrSource, input);
    default:
      throw new IllegalStateException("unknown type ordinal " + ord);
    }
  }
  
  private static IndexDocValues.Source loadFixedSource(byte ord, IndexInput input) throws IOException {
    switch (ord) {
    case FIXED_16:
      return new ShortValues(input);
    case FIXED_32:
      return new IntValues(input);
    case FIXED_64:
      return new LongValues(input);
    case FIXED_8:
      return new ByteValues(input);
    case PACKED:
      return new PackedIntsSource(input);
    default:
      throw new IllegalStateException("unknown type ordinal " + ord);
    }
  }
  
  static class PackedIntsSource extends Source {
    private final long minValue;
    private final long defaultValue;
    private final PackedInts.Reader values;

    public PackedIntsSource(IndexInput dataIn) throws IOException {
      
      minValue = dataIn.readLong();
      defaultValue = dataIn.readLong();
      values = PackedInts.getReader(dataIn);
    }

    @Override
    public long getInt(int docID) {
      // TODO -- can we somehow avoid 2X method calls
      // on each get? must push minValue down, and make
      // PackedInts implement Ints.Source
      assert docID >= 0;
      final long value = values.get(docID);
      return value == defaultValue ? 0 : minValue + value;
    }

    @Override
    public ValuesEnum getEnum(AttributeSource attrSource) throws IOException {
      return new SourceEnum(attrSource, type(), this, values.size()) {
        @Override
        public int advance(int target) throws IOException {
          if (target >= numDocs)
            return pos = NO_MORE_DOCS;
          intsRef.ints[intsRef.offset] = source.getInt(target);
          return pos = target;
        }
      };
    }

    @Override
    public ValueType type() {
      return ValueType.VAR_INTS;
    }
  }


  private static final class PackedIntsEnumImpl extends ValuesEnum {
    private final PackedInts.ReaderIterator ints;
    private long minValue;
    private final IndexInput dataIn;
    private final long defaultValue;
    private final int maxDoc;
    private int pos = -1;

    private PackedIntsEnumImpl(AttributeSource source, IndexInput dataIn)
        throws IOException {
      super(source, ValueType.VAR_INTS);
      intsRef.offset = 0;
      this.dataIn = dataIn;
      minValue = dataIn.readLong();
      defaultValue = dataIn.readLong();
      this.ints = PackedInts.getReaderIterator(dataIn);
      maxDoc = ints.size();
    }

    @Override
    public void close() throws IOException {
      ints.close();
      dataIn.close();
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return pos = NO_MORE_DOCS;
      }
      final long val = ints.advance(target);
      intsRef.ints[intsRef.offset] = val == defaultValue ? 0 : minValue + val;
      return pos = target;
    }

    @Override
    public int docID() {
      return pos;
    }

    @Override
    public int nextDoc() throws IOException {
      if (pos >= maxDoc) {
        return pos = NO_MORE_DOCS;
      }
      return advance(pos + 1);
    }
  }

 

}