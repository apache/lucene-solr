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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Stores ints packed with fixed-bit precision.
 * 
 * @lucene.experimental
 * */
class PackedIntsImpl {

  private static final String CODEC_NAME = "Ints";
  private static final byte PACKED = 0x00;
  private static final byte FIXED = 0x01;

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class IntsWriter extends Writer {

    // TODO: can we bulkcopy this on a merge?
    private LongsRef intsRef;
    private long[] docToValue;
    private long minValue;
    private long maxValue;
    private boolean started;
    private final String id;
    private int lastDocId = -1;
    private IndexOutput datOut;

    protected IntsWriter(Directory dir, String id, AtomicLong bytesUsed)
        throws IOException {
      super(bytesUsed);
      datOut = dir.createOutput(IndexFileNames.segmentFileName(id, "",
          DATA_EXTENSION));
      boolean success = false;
      try {
        CodecUtil.writeHeader(datOut, CODEC_NAME, VERSION_CURRENT);
        this.id = id;
        docToValue = new long[1];
        bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_LONG); // TODO the
                                                               // bitset
                                                               // needs memory
                                                               // too
        success = true;
      } finally {
        if (!success) {
          datOut.close();
        }
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

      if (docID >= docToValue.length) {
        final long len = docToValue.length;
        docToValue = ArrayUtil.grow(docToValue, 1 + docID);
        bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_LONG
            * ((docToValue.length) - len));
      }
      docToValue[docID] = v;
    }

    @Override
    public void finish(int docCount) throws IOException {
      try {
        if (!started) {
          minValue = maxValue = 0;
        }
        // if we exceed the range of positive longs we must switch to fixed ints
        if ((maxValue - minValue) < (((long)1) << 63) && (maxValue - minValue) > 0) {
          writePackedInts(docCount);
        } else {
          writeFixedInts(docCount);
        }

      } finally {
        datOut.close();
        bytesUsed
            .addAndGet(-(RamUsageEstimator.NUM_BYTES_LONG * docToValue.length));
        docToValue = null;
      }
    }

    private void writeFixedInts(int docCount) throws IOException {
      datOut.writeByte(FIXED);
      datOut.writeInt(docCount);
      for (int i = 0; i < docToValue.length; i++) {
        datOut.writeLong(docToValue[i]); // write full array - we use 0 as default
      }
      for (int i = docToValue.length; i < docCount; i++) {
        datOut.writeLong(0); // fill with defaults values
      }
    }

    private void writePackedInts(int docCount) throws IOException {
      datOut.writeByte(PACKED);
      // TODO -- long can't work right since it's signed
      datOut.writeLong(minValue);
      // write a default value to recognize docs without a value for that
      // field
      final long defaultValue = maxValue>= 0 && minValue <=0 ? 0-minValue : ++maxValue-minValue;
      datOut.writeLong(defaultValue);
      PackedInts.Writer w = PackedInts.getWriter(datOut, docCount,
          PackedInts.bitsRequired(maxValue-minValue));
      final int limit = docToValue.length > docCount ? docCount : docToValue.length;
      for (int i = 0; i < limit; i++) {
        w.add(docToValue[i] == 0 ? defaultValue : docToValue[i] - minValue);
      }
      for (int i = limit; i < docCount; i++) {
        w.add(defaultValue);
      }
      
      w.finish();
    }

    @Override
    protected void add(int docID) throws IOException {
      add(docID, intsRef.get());
    }

    @Override
    protected void setNextEnum(DocValuesEnum valuesEnum) {
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
  static class IntsReader extends DocValues {
    private final IndexInput datIn;
    private final boolean packed;

    protected IntsReader(Directory dir, String id) throws IOException {
      datIn = dir.openInput(IndexFileNames.segmentFileName(id, "",
          Writer.DATA_EXTENSION));
      boolean success = false;
      try {
        CodecUtil.checkHeader(datIn, CODEC_NAME, VERSION_START, VERSION_START);
        packed = PACKED == datIn.readByte();
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
      final IndexInput input = (IndexInput) datIn.clone();
      boolean success = false;
      try {
        final Source source = packed ? new PackedIntsSource(input)
            : new FixedIntsSource(input);
        success = true;
        return source;
      } finally {
        if (!success) {
          IOUtils.closeSafely(true, datIn);
        }
      }
    }
    
    private static class FixedIntsSource extends Source {
      private final long[] values;
      public FixedIntsSource(IndexInput dataIn) throws IOException {
        dataIn.seek(CodecUtil.headerLength(CODEC_NAME) + 1);
        final int numDocs = dataIn.readInt();
        values = new long[numDocs];
        for (int i = 0; i < values.length; i++) {
          values[i] = dataIn.readLong();
        }
      }
      
      @Override
      public long getInt(int docID) {
        assert docID >= 0 && docID < values.length;
        return values[docID];
      }

      @Override
      public ValueType type() {
        return ValueType.INTS;
      }

      @Override
      public DocValuesEnum getEnum(AttributeSource attrSource)
          throws IOException {
        return new SourceEnum(attrSource, type(), this, values.length) {
          
          @Override
          public int advance(int target) throws IOException {
            if (target >= numDocs)
              return pos = NO_MORE_DOCS;
            intsRef.ints[intsRef.offset] = values[target];
            return pos = target;
          }
        };
      }
      
    }

    private static class PackedIntsSource extends Source {
      private final long minValue;
      private final long defaultValue;
      private final PackedInts.Reader values;

      public PackedIntsSource(IndexInput dataIn) throws IOException {
        dataIn.seek(CodecUtil.headerLength(CODEC_NAME) + 1);
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
      public DocValuesEnum getEnum(AttributeSource attrSource)
          throws IOException {
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
        return ValueType.INTS;
      }
    }

    @Override
    public void close() throws IOException {
      super.close();
      datIn.close();
    }

    @Override
    public DocValuesEnum getEnum(AttributeSource source) throws IOException {
      final IndexInput input = (IndexInput) datIn.clone();
      boolean success = false;
      try {
        DocValuesEnum inst = packed ? new PackedIntsEnumImpl(source, input)
            : new FixedIntsEnumImpl(source, input);
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
      return ValueType.INTS;
    }

  }

  private static final class PackedIntsEnumImpl extends DocValuesEnum {
    private final PackedInts.ReaderIterator ints;
    private long minValue;
    private final IndexInput dataIn;
    private final long defaultValue;
    private final int maxDoc;
    private int pos = -1;

    private PackedIntsEnumImpl(AttributeSource source, IndexInput dataIn)
        throws IOException {
      super(source, ValueType.INTS);
      intsRef.offset = 0;
      this.dataIn = dataIn;
      dataIn.seek(CodecUtil.headerLength(CODEC_NAME) + 1);
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
  
  private static final class FixedIntsEnumImpl extends DocValuesEnum {
    private final IndexInput dataIn;
    private final int maxDoc;
    private int pos = -1;

    private FixedIntsEnumImpl(AttributeSource source, IndexInput dataIn)
        throws IOException {
      super(source, ValueType.INTS);
      intsRef.offset = 0;
      this.dataIn = dataIn;
      dataIn.seek(CodecUtil.headerLength(CODEC_NAME) + 1);
      maxDoc = dataIn.readInt();
    }

    @Override
    public void close() throws IOException {
      dataIn.close();
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return pos = NO_MORE_DOCS;
      }
      assert target > pos;
      if (target > pos+1) {
        dataIn.seek(dataIn.getFilePointer() + ((target - pos - 1) * 8));
      }
      intsRef.ints[intsRef.offset] = dataIn.readLong();
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