package org.apache.lucene.codecs;

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
 * @lucene.internal
 */
public abstract class DocValuesArraySource extends Source {

  private static final Map<Type, DocValuesArraySource> TEMPLATES;

  static {
    EnumMap<Type, DocValuesArraySource> templates = new EnumMap<Type, DocValuesArraySource>(
        Type.class);
    templates.put(Type.FIXED_INTS_16, new ShortValues());
    templates.put(Type.FIXED_INTS_32, new IntValues());
    templates.put(Type.FIXED_INTS_64, new LongValues());
    templates.put(Type.FIXED_INTS_8, new ByteValues());
    templates.put(Type.FLOAT_32, new FloatValues());
    templates.put(Type.FLOAT_64, new DoubleValues());
    TEMPLATES = Collections.unmodifiableMap(templates);
  }
  
  public static DocValuesArraySource forType(Type type) {
    return TEMPLATES.get(type);
  }

  protected final int bytesPerValue;

  DocValuesArraySource(int bytesPerValue, Type type) {
    super(type);
    this.bytesPerValue = bytesPerValue;
  }

  @Override
  public abstract BytesRef getBytes(int docID, BytesRef ref);

  
  public abstract DocValuesArraySource newFromInput(IndexInput input, int numDocs)
      throws IOException;
  
  public abstract DocValuesArraySource newFromArray(Object array);

  @Override
  public final boolean hasArray() {
    return true;
  }

  public void toBytes(long value, BytesRef bytesRef) {
    copyLong(bytesRef, value);
  }

  public void toBytes(double value, BytesRef bytesRef) {
    copyLong(bytesRef, Double.doubleToRawLongBits(value));
  }

  final static class ByteValues extends DocValuesArraySource {
    private final byte[] values;
    
    ByteValues() {
      super(1, Type.FIXED_INTS_8);
      values = new byte[0];
    }
    private ByteValues(byte[] array) {
      super(1, Type.FIXED_INTS_8);
      values = array;
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
    public DocValuesArraySource newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new ByteValues(input, numDocs);
    }
    
    @Override
    public DocValuesArraySource newFromArray(Object array) {
      assert array instanceof byte[];
      return new ByteValues((byte[]) array);
    }

    public void toBytes(long value, BytesRef bytesRef) {
      if (bytesRef.bytes.length == 0) {
        bytesRef.bytes = new byte[1];
      }
      bytesRef.bytes[0] = (byte) (0xFFL & value);
      bytesRef.offset = 0;
      bytesRef.length = 1;
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      toBytes(getInt(docID), ref);
      return ref;
    }

  };

  final static class ShortValues extends DocValuesArraySource {
    private final short[] values;

    ShortValues() {
      super(RamUsageEstimator.NUM_BYTES_SHORT, Type.FIXED_INTS_16);
      values = new short[0];
    }
    
    private ShortValues(short[] array) {
      super(RamUsageEstimator.NUM_BYTES_SHORT, Type.FIXED_INTS_16);
      values = array;
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
    public DocValuesArraySource newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new ShortValues(input, numDocs);
    }

    public void toBytes(long value, BytesRef bytesRef) {
      copyShort(bytesRef, (short) (0xFFFFL & value));
    }

    @Override
    public DocValuesArraySource newFromArray(Object array) {
      assert array instanceof short[];
      return new ShortValues((short[]) array);
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      toBytes(getInt(docID), ref);
      return ref;
    }

  };

  final static class IntValues extends DocValuesArraySource {
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

    private IntValues(int[] array) {
      super(RamUsageEstimator.NUM_BYTES_INT, Type.FIXED_INTS_32);
      values = array;
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
    public DocValuesArraySource newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new IntValues(input, numDocs);
    }

    public void toBytes(long value, BytesRef bytesRef) {
      copyInt(bytesRef, (int) (0xFFFFFFFF & value));
    }

    @Override
    public DocValuesArraySource newFromArray(Object array) {
      assert array instanceof int[];
      return new IntValues((int[]) array);
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      toBytes(getInt(docID), ref);
      return ref;
    }

  };

  final static class LongValues extends DocValuesArraySource {
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

    private LongValues(long[] array) {
      super(RamUsageEstimator.NUM_BYTES_LONG, Type.FIXED_INTS_64);
      values = array;
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
    public DocValuesArraySource newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new LongValues(input, numDocs);
    }

    @Override
    public DocValuesArraySource newFromArray(Object array) {
      assert array instanceof long[];
      return new LongValues((long[])array);
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      toBytes(getInt(docID), ref);
      return ref;
    }

  };

  final static class FloatValues extends DocValuesArraySource {
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

    private FloatValues(float[] array) {
      super(RamUsageEstimator.NUM_BYTES_FLOAT, Type.FLOAT_32);
      values = array;
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
    public void toBytes(double value, BytesRef bytesRef) {
      copyInt(bytesRef, Float.floatToRawIntBits((float)value));

    }

    @Override
    public DocValuesArraySource newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new FloatValues(input, numDocs);
    }

    @Override
    public DocValuesArraySource newFromArray(Object array) {
      assert array instanceof float[];
      return new FloatValues((float[]) array);
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      toBytes(getFloat(docID), ref);
      return ref;
    }
  };

  final static class DoubleValues extends DocValuesArraySource {
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

    private DoubleValues(double[] array) {
      super(RamUsageEstimator.NUM_BYTES_DOUBLE, Type.FLOAT_64);
      values = array;
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
    public DocValuesArraySource newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new DoubleValues(input, numDocs);
    }

    @Override
    public DocValuesArraySource newFromArray(Object array) {
      assert array instanceof double[];
      return new DoubleValues((double[]) array);
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      toBytes(getFloat(docID), ref);
      return ref;
    }

  };
  
  /**
   * Copies the given long value and encodes it as 8 byte Big-Endian.
   * <p>
   * NOTE: this method resets the offset to 0, length to 8 and resizes the
   * reference array if needed.
   */
  public static void copyLong(BytesRef ref, long value) {
    if (ref.bytes.length < 8) {
      ref.bytes = new byte[8];
    }
    copyInternal(ref, (int) (value >> 32), ref.offset = 0);
    copyInternal(ref, (int) value, 4);
    ref.length = 8;
  }

  /**
   * Copies the given int value and encodes it as 4 byte Big-Endian.
   * <p>
   * NOTE: this method resets the offset to 0, length to 4 and resizes the
   * reference array if needed.
   */
  public static void copyInt(BytesRef ref, int value) {
    if (ref.bytes.length < 4) {
      ref.bytes = new byte[4];
    }
    copyInternal(ref, value, ref.offset = 0);
    ref.length = 4;
    
  }

  /**
   * Copies the given short value and encodes it as a 2 byte Big-Endian.
   * <p>
   * NOTE: this method resets the offset to 0, length to 2 and resizes the
   * reference array if needed.
   */
  public static void copyShort(BytesRef ref, short value) {
    if (ref.bytes.length < 2) {
      ref.bytes = new byte[2];
    }
    ref.offset = 0;
    ref.bytes[ref.offset] = (byte) (value >> 8);
    ref.bytes[ref.offset + 1] = (byte) (value);
    ref.length = 2;
  }

  private static void copyInternal(BytesRef ref, int value, int startOffset) {
    ref.bytes[startOffset] = (byte) (value >> 24);
    ref.bytes[startOffset + 1] = (byte) (value >> 16);
    ref.bytes[startOffset + 2] = (byte) (value >> 8);
    ref.bytes[startOffset + 3] = (byte) (value);
  }

  /**
   * Converts 2 consecutive bytes from the current offset to a short. Bytes are
   * interpreted as Big-Endian (most significant bit first)
   * <p>
   * NOTE: this method does <b>NOT</b> check the bounds of the referenced array.
   */
  public static short asShort(BytesRef b) {
    return (short) (0xFFFF & ((b.bytes[b.offset] & 0xFF) << 8) | (b.bytes[b.offset + 1] & 0xFF));
  }

  /**
   * Converts 4 consecutive bytes from the current offset to an int. Bytes are
   * interpreted as Big-Endian (most significant bit first)
   * <p>
   * NOTE: this method does <b>NOT</b> check the bounds of the referenced array.
   */
  public static int asInt(BytesRef b) {
    return asIntInternal(b, b.offset);
  }

  /**
   * Converts 8 consecutive bytes from the current offset to a long. Bytes are
   * interpreted as Big-Endian (most significant bit first)
   * <p>
   * NOTE: this method does <b>NOT</b> check the bounds of the referenced array.
   */
  public static long asLong(BytesRef b) {
    return (((long) asIntInternal(b, b.offset) << 32) | asIntInternal(b,
        b.offset + 4) & 0xFFFFFFFFL);
  }

  private static int asIntInternal(BytesRef b, int pos) {
    return ((b.bytes[pos++] & 0xFF) << 24) | ((b.bytes[pos++] & 0xFF) << 16)
        | ((b.bytes[pos++] & 0xFF) << 8) | (b.bytes[pos] & 0xFF);
  }


}