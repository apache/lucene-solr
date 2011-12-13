package org.apache.lucene.index.codecs.lucene40.values;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
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
abstract class DocValuesArray extends Source {

  static final Map<Type, DocValuesArray> TEMPLATES;

  static {
    EnumMap<Type, DocValuesArray> templates = new EnumMap<Type, DocValuesArray>(
        Type.class);
    templates.put(Type.FIXED_INTS_16, new ShortValues());
    templates.put(Type.FIXED_INTS_32, new IntValues());
    templates.put(Type.FIXED_INTS_64, new LongValues());
    templates.put(Type.FIXED_INTS_8, new ByteValues());
    templates.put(Type.FLOAT_32, new FloatValues());
    templates.put(Type.FLOAT_64, new DoubleValues());
    TEMPLATES = Collections.unmodifiableMap(templates);
  }

  protected final int bytesPerValue;

  DocValuesArray(int bytesPerValue, Type type) {
    super(type);
    this.bytesPerValue = bytesPerValue;
  }

  public abstract DocValuesArray newFromInput(IndexInput input, int numDocs)
      throws IOException;

  @Override
  public final boolean hasArray() {
    return true;
  }

  void toBytes(long value, BytesRef bytesRef) {
    BytesRefUtils.copyLong(bytesRef, value);
  }

  void toBytes(double value, BytesRef bytesRef) {
    BytesRefUtils.copyLong(bytesRef, Double.doubleToRawLongBits(value));
  }

  final static class ByteValues extends DocValuesArray {
    private final byte[] values;

    ByteValues() {
      super(1, Type.FIXED_INTS_8);
      values = new byte[0];
    }

    private ByteValues(IndexInput input, int numDocs) throws IOException {
      super(1, Type.FIXED_INTS_8);
      values = new byte[numDocs];
      input.readBytes(values, 0, values.length, false);
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
    public DocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new ByteValues(input, numDocs);
    }

    void toBytes(long value, BytesRef bytesRef) {
      bytesRef.bytes[0] = (byte) (0xFFL & value);
    }

  };

  final static class ShortValues extends DocValuesArray {
    private final short[] values;

    ShortValues() {
      super(RamUsageEstimator.NUM_BYTES_SHORT, Type.FIXED_INTS_16);
      values = new short[0];
    }

    private ShortValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_SHORT, Type.FIXED_INTS_16);
      values = new short[numDocs];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readShort();
      }
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
    public DocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new ShortValues(input, numDocs);
    }

    void toBytes(long value, BytesRef bytesRef) {
      BytesRefUtils.copyShort(bytesRef, (short) (0xFFFFL & value));
    }

  };

  final static class IntValues extends DocValuesArray {
    private final int[] values;

    IntValues() {
      super(RamUsageEstimator.NUM_BYTES_INT, Type.FIXED_INTS_32);
      values = new int[0];
    }

    private IntValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_INT, Type.FIXED_INTS_32);
      values = new int[numDocs];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readInt();
      }
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
    public DocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new IntValues(input, numDocs);
    }

    void toBytes(long value, BytesRef bytesRef) {
      BytesRefUtils.copyInt(bytesRef, (int) (0xFFFFFFFF & value));
    }

  };

  final static class LongValues extends DocValuesArray {
    private final long[] values;

    LongValues() {
      super(RamUsageEstimator.NUM_BYTES_LONG, Type.FIXED_INTS_64);
      values = new long[0];
    }

    private LongValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_LONG, Type.FIXED_INTS_64);
      values = new long[numDocs];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readLong();
      }
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
    public DocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new LongValues(input, numDocs);
    }

  };

  final static class FloatValues extends DocValuesArray {
    private final float[] values;

    FloatValues() {
      super(RamUsageEstimator.NUM_BYTES_FLOAT, Type.FLOAT_32);
      values = new float[0];
    }

    private FloatValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_FLOAT, Type.FLOAT_32);
      values = new float[numDocs];
      /*
       * we always read BIG_ENDIAN here since the writer serialized plain bytes
       * we can simply read the ints / longs back in using readInt / readLong
       */
      for (int i = 0; i < values.length; i++) {
        values[i] = Float.intBitsToFloat(input.readInt());
      }
    }

    @Override
    public float[] getArray() {
      return values;
    }

    @Override
    public double getFloat(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }
    
    @Override
    void toBytes(double value, BytesRef bytesRef) {
      BytesRefUtils.copyInt(bytesRef, Float.floatToRawIntBits((float)value));

    }

    @Override
    public DocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new FloatValues(input, numDocs);
    }
  };

  final static class DoubleValues extends DocValuesArray {
    private final double[] values;

    DoubleValues() {
      super(RamUsageEstimator.NUM_BYTES_DOUBLE, Type.FLOAT_64);
      values = new double[0];
    }

    private DoubleValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_DOUBLE, Type.FLOAT_64);
      values = new double[numDocs];
      /*
       * we always read BIG_ENDIAN here since the writer serialized plain bytes
       * we can simply read the ints / longs back in using readInt / readLong
       */
      for (int i = 0; i < values.length; i++) {
        values[i] = Double.longBitsToDouble(input.readLong());
      }
    }

    @Override
    public double[] getArray() {
      return values;
    }

    @Override
    public double getFloat(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }

    @Override
    public DocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new DoubleValues(input, numDocs);
    }

  };

}
