package org.apache.lucene.index.values;

import java.io.IOException;

import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.index.values.IndexDocValues.SourceEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * @lucene.experimental
 */
abstract class IndexDocValuesArray extends Source {

  private final Counter bytesUsed;
  private final int bytesPerValue;
  private int size = 0;
  private final ValueType type;
  protected int maxDocID = -1;

  IndexDocValuesArray(Counter bytesUsed, int bytesPerValue, ValueType type) {
    this.bytesUsed = bytesUsed;
    this.bytesPerValue = bytesPerValue;
    this.type = type;
  }

  void set(int docId, long value) {
    if (docId >= size) {
      adjustSize(grow(docId + 1));
    }
    if (docId > maxDocID) {
      maxDocID = docId;
    }
    setInternal(docId, value);
  }

  protected final void adjustSize(int newSize) {
    bytesUsed.addAndGet(bytesPerValue * (newSize - size));
    size = newSize;
  }

  void clear() {
    adjustSize(0);
    maxDocID = -1;
    size = 0;
  }
  
  protected abstract void writeDirect(IndexOutput out, long value) throws IOException;
  
  protected abstract void writeDefaults(IndexOutput out, int num) throws IOException;

  protected abstract void setInternal(int docId, long value);

  protected abstract int grow(int numDocs);

  abstract void write(IndexOutput output, int numDocs) throws IOException;

  @Override
  public final int getValueCount() {
    return maxDocID + 1;
  }

  @Override
  public final ValueType type() {
    return type;
  }

  @Override
  public ValuesEnum getEnum(AttributeSource attrSource) throws IOException {
    return new SourceEnum(attrSource, type(), this, maxDocID + 1) {

      @Override
      public int advance(int target) throws IOException {
        if (target >= numDocs) {
          return pos = NO_MORE_DOCS;
        }
        intsRef.ints[intsRef.offset] = IndexDocValuesArray.this.getInt(target);
        return pos = target;
      }
    };
  }

  abstract ValuesEnum getDirectEnum(AttributeSource attrSource, IndexInput input, int maxDoc)
      throws IOException;

  @Override
  public final boolean hasArray() {
    return true;
  }

  final static class ByteValues extends IndexDocValuesArray {
    private byte[] values;

    ByteValues(Counter bytesUsed) {
      super(bytesUsed, 1, ValueType.FIXED_INTS_8);
      values = new byte[0];
    }

    ByteValues(IndexInput input, int numDocs) throws IOException {
      super(Counter.newCounter(), 1, ValueType.FIXED_INTS_8);
      values = new byte[numDocs];
      adjustSize(numDocs);
      input.readBytes(values, 0, values.length, false);
      maxDocID = numDocs - 1;
    }

    @Override
    public byte[] getArray() {
      return values;
    }

    @Override
    public long getInt(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }

    @Override
    protected void setInternal(int docId, long value) {
      values[docId] = (byte) (0xFFL & value);
    }

    @Override
    protected int grow(int numDocs) {
      values = ArrayUtil.grow(values, numDocs);
      return values.length;
    }

    @Override
    void write(IndexOutput output, int numDocs) throws IOException {
      assert maxDocID + 1 <= numDocs;
      output.writeBytes(values, 0, maxDocID + 1);
      writeDefaults(output,  numDocs - (maxDocID+1));
    }

    @Override
    ValuesEnum getDirectEnum(AttributeSource attrSource, IndexInput input, int maxDoc)
        throws IOException {
      return new FixedIntsEnumImpl(attrSource, input, type(), maxDoc) {
        @Override
        protected void fillNext(LongsRef ref, IndexInput dataIn)
            throws IOException {
          ref.ints[ref.offset] = dataIn.readByte();
        }
      };
    }

    @Override
    void clear() {
      super.clear();
      values = new byte[0];
    }

    @Override
    protected void writeDefaults(IndexOutput out, int num) throws IOException {
      final byte zero = 0;
      for (int i = 0; i < num; i++) {
        out.writeByte(zero);
      }
    }

    @Override
    protected void writeDirect(IndexOutput out, long value) throws IOException {
      out.writeByte((byte) (0xFFL & value));
    }
  };

  final static class ShortValues extends IndexDocValuesArray {
    private short[] values;

    ShortValues(Counter bytesUsed) {
      super(bytesUsed, RamUsageEstimator.NUM_BYTES_SHORT,
          ValueType.FIXED_INTS_16);
      values = new short[0];
    }

    ShortValues(IndexInput input, int numDocs) throws IOException {
      super(Counter.newCounter(), RamUsageEstimator.NUM_BYTES_SHORT,
          ValueType.FIXED_INTS_16);
      values = new short[numDocs];
      adjustSize(numDocs);
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readShort();
      }
      maxDocID = numDocs - 1;
    }

    @Override
    public short[] getArray() {
      return values;
    }

    @Override
    public long getInt(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }

    @Override
    protected void setInternal(int docId, long value) {
      values[docId] = (short) (0xFFFFL & value);
    }

    @Override
    protected int grow(int numDocs) {
      values = ArrayUtil.grow(values, numDocs);
      return values.length;
    }

    @Override
    void write(IndexOutput output, int numDocs) throws IOException {
      assert maxDocID + 1 <= numDocs;
      for (int i = 0; i < maxDocID + 1; i++) {
        output.writeShort(values[i]);
      }
      writeDefaults(output,  numDocs - (maxDocID+1));
    }

    @Override
    ValuesEnum getDirectEnum(AttributeSource attrSource, IndexInput input, int maxDoc)
        throws IOException {
      return new FixedIntsEnumImpl(attrSource, input, type(), maxDoc) {
        @Override
        protected void fillNext(LongsRef ref, IndexInput dataIn)
            throws IOException {
          ref.ints[ref.offset] = dataIn.readShort();
        }
      };
    }

    @Override
    void clear() {
      super.clear();
      values = new short[0];
    }

    @Override
    protected void writeDefaults(IndexOutput out, int num) throws IOException {
      final short zero = 0;
      for (int i = 0; i < num; i++) {
        out.writeShort(zero);
      }
    }
    
    @Override
    protected void writeDirect(IndexOutput out, long value) throws IOException {
      out.writeShort((short) (0xFFFFL & value));
    }

  };

  final static class IntValues extends IndexDocValuesArray {
    private int[] values;

    IntValues(Counter bytesUsed) {
      super(bytesUsed, RamUsageEstimator.NUM_BYTES_INT, ValueType.FIXED_INTS_32);
      values = new int[0];
    }

    IntValues(IndexInput input, int numDocs) throws IOException {
      super(Counter.newCounter(), RamUsageEstimator.NUM_BYTES_INT,
          ValueType.FIXED_INTS_32);
      values = new int[numDocs];
      adjustSize(numDocs);
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readInt();
      }
      maxDocID = numDocs - 1;
    }

    @Override
    public int[] getArray() {
      return values;
    }

    @Override
    public long getInt(int docID) {
      assert docID >= 0 && docID < values.length;
      return 0xFFFFFFFF & values[docID];
    }

    @Override
    protected void setInternal(int docId, long value) {
      values[docId] = (int) (0xFFFFFFFF & value);
    }

    @Override
    protected int grow(int numDocs) {
      values = ArrayUtil.grow(values, numDocs);
      return values.length;
    }

    @Override
    void write(IndexOutput output, int numDocs) throws IOException {
      assert maxDocID + 1 <= numDocs;
      for (int i = 0; i < maxDocID + 1; i++) {
        output.writeInt(values[i]);
      }
      writeDefaults(output,  numDocs - (maxDocID+1));
    }

    @Override
    ValuesEnum getDirectEnum(AttributeSource attrSource, IndexInput input, int maxDoc)
        throws IOException {
      return new FixedIntsEnumImpl(attrSource, input, type(), maxDoc) {
        @Override
        protected void fillNext(LongsRef ref, IndexInput dataIn)
            throws IOException {
          ref.ints[ref.offset] = dataIn.readInt();
        }
      };
    }

    @Override
    void clear() {
      super.clear();
      values = new int[0];
    }

    @Override
    protected void writeDefaults(IndexOutput out, int num) throws IOException {
      for (int i = 0; i < num; i++) {
        out.writeInt(0);
      }
    }
    
    @Override
    protected void writeDirect(IndexOutput out, long value) throws IOException {
      out.writeInt((int) (0xFFFFFFFFL & value));
    }

  };

  final static class LongValues extends IndexDocValuesArray {
    private long[] values;

    LongValues(Counter bytesUsed) {
      super(bytesUsed, RamUsageEstimator.NUM_BYTES_LONG,
          ValueType.FIXED_INTS_64);
      values = new long[0];
    }

    LongValues(IndexInput input, int numDocs) throws IOException {
      super(Counter.newCounter(), RamUsageEstimator.NUM_BYTES_LONG,
          ValueType.FIXED_INTS_64);
      values = new long[numDocs];
      adjustSize(numDocs);
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readLong();
      }
      maxDocID = numDocs - 1;
    }

    @Override
    public long[] getArray() {
      return values;
    }

    @Override
    public long getInt(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }

    @Override
    protected void setInternal(int docId, long value) {
      values[docId] = value;
    }

    @Override
    protected int grow(int numDocs) {
      values = ArrayUtil.grow(values, numDocs);
      return values.length;
    }

    @Override
    void write(IndexOutput output, int numDocs) throws IOException {
      assert maxDocID + 1 <= numDocs;
      for (int i = 0; i < maxDocID + 1; i++) {
        output.writeLong(values[i]);
      }
      writeDefaults(output, numDocs - (maxDocID+1));
     
    }

    @Override
    ValuesEnum getDirectEnum(AttributeSource attrSource, IndexInput input, int maxDoc)
        throws IOException {
      return new FixedIntsEnumImpl(attrSource, input, type(), maxDoc) {
        @Override
        protected void fillNext(LongsRef ref, IndexInput dataIn)
            throws IOException {
          ref.ints[ref.offset] = dataIn.readLong();
        }
      };
    }

    @Override
    void clear() {
      super.clear();
      values = new long[0];
    }

    @Override
    protected void writeDefaults(IndexOutput out, int num) throws IOException {
      for (int i = 0; i < num; i++) {
        out.writeLong(0l);
      }
    }
    @Override
    protected void writeDirect(IndexOutput out, long value) throws IOException {
      out.writeLong(value);
    }
  };

  private abstract static class FixedIntsEnumImpl extends ValuesEnum {
    private final IndexInput dataIn;
    private final int maxDoc;
    private final int sizeInByte;
    private int pos = -1;

    private FixedIntsEnumImpl(AttributeSource source, IndexInput dataIn,
        ValueType type, int maxDoc) throws IOException {
      super(source, type);
      switch (type) {
      case FIXED_INTS_16:
        sizeInByte = 2;
        break;
      case FIXED_INTS_32:
        sizeInByte = 4;
        break;
      case FIXED_INTS_64:
        sizeInByte = 8;
        break;
      case FIXED_INTS_8:
        sizeInByte = 1;
        break;
      default:
        throw new IllegalStateException("type " + type
            + " is not a fixed int type");
      }
      intsRef.offset = 0;
      this.dataIn = dataIn;
      this.maxDoc = maxDoc;

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
      if (target > pos + 1) {
        dataIn
            .seek(dataIn.getFilePointer() + ((target - pos - 1) * sizeInByte));
      }
      fillNext(intsRef, dataIn);
      return pos = target;
    }

    protected abstract void fillNext(LongsRef ref, IndexInput input)
        throws IOException;

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
