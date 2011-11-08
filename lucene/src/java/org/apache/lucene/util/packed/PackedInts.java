package org.apache.lucene.util.packed;

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

import java.io.Closeable;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.Constants;

import java.io.IOException;

/**
 * Simplistic compression for array of unsigned long values.
 * Each value is >= 0 and <= a specified maximum value.  The
 * values are stored as packed ints, with each value
 * consuming a fixed number of bits.
 *
 * @lucene.internal
 */

public class PackedInts {

  private final static String CODEC_NAME = "PackedInts";
  private final static int VERSION_START = 0;
  private final static int VERSION_CURRENT = VERSION_START;

  /**
   * A read-only random access array of positive integers.
   * @lucene.internal
   */
  public static interface Reader {
    /**
     * @param index the position of the wanted value.
     * @return the value at the stated index.
     */
    long get(int index);

    /**
     * @return the number of bits used to store any given value.
     *         Note: This does not imply that memory usage is
     *         {@code bitsPerValue * #values} as implementations are free to
     *         use non-space-optimal packing of bits.
     */
    int getBitsPerValue();

    /**
     * @return the number of values.
     */
    int size();

    /**
     * Expert: if the bit-width of this reader matches one of
     * java's native types, returns the underlying array
     * (ie, byte[], short[], int[], long[]); else, returns
     * null.  Note that when accessing the array you must
     * upgrade the type (bitwise AND with all ones), to
     * interpret the full value as unsigned.  Ie,
     * bytes[idx]&0xFF, shorts[idx]&0xFFFF, etc.
     */
    Object getArray();

    /**
     * Returns true if this implementation is backed by a
     * native java array.
     *
     * @see #getArray
     */
    boolean hasArray();
  }

  /**
   * Run-once iterator interface, to decode previously saved PackedInts.
   */
  public static interface ReaderIterator extends Closeable {
    /** Returns next value */
    long next() throws IOException;
    /** Returns number of bits per value */
    int getBitsPerValue();
    /** Returns number of values */
    int size();
    /** Returns the current position */
    int ord();
    /** Skips to the given ordinal and returns its value.
     * @return the value at the given position
     * @throws IOException if reading the value throws an IOException*/
    long advance(int ord) throws IOException;
  }
  
  /**
   * A packed integer array that can be modified.
   * @lucene.internal
   */
  public static interface Mutable extends Reader {
    /**
     * Set the value at the given index in the array.
     * @param index where the value should be positioned.
     * @param value a value conforming to the constraints set by the array.
     */
    void set(int index, long value);

    /**
     * Sets all values to 0.
     */
    
    void clear();
  }

  /**
   * A simple base for Readers that keeps track of valueCount and bitsPerValue.
   * @lucene.internal
   */
  public static abstract class ReaderImpl implements Reader {
    protected final int bitsPerValue;
    protected final int valueCount;

    protected ReaderImpl(int valueCount, int bitsPerValue) {
      this.bitsPerValue = bitsPerValue;
      assert bitsPerValue > 0 && bitsPerValue <= 64 : "bitsPerValue=" + bitsPerValue;
      this.valueCount = valueCount;
    }

    public int getBitsPerValue() {
      return bitsPerValue;
    }
    
    public int size() {
      return valueCount;
    }

    public long getMaxValue() { // Convenience method
      return maxValue(bitsPerValue);
    }

    public Object getArray() {
      return null;
    }

    public boolean hasArray() {
      return false;
    }
  }

  /** A write-once Writer.
   * @lucene.internal
   */
  public static abstract class Writer {
    protected final DataOutput out;
    protected final int bitsPerValue;
    protected final int valueCount;

    protected Writer(DataOutput out, int valueCount, int bitsPerValue)
      throws IOException {
      assert bitsPerValue <= 64;

      this.out = out;
      this.valueCount = valueCount;
      this.bitsPerValue = bitsPerValue;
      CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
      out.writeVInt(bitsPerValue);
      out.writeVInt(valueCount);
    }

    public abstract void add(long v) throws IOException;
    public abstract void finish() throws IOException;
  }

  /**
   * Retrieve PackedInt data from the DataInput and return a packed int
   * structure based on it.
   * @param in positioned at the beginning of a stored packed int structure.
   * @return a read only random access capable array of positive integers.
   * @throws IOException if the structure could not be retrieved.
   * @lucene.internal
   */
  public static Reader getReader(DataInput in) throws IOException {
    CodecUtil.checkHeader(in, CODEC_NAME, VERSION_START, VERSION_START);
    final int bitsPerValue = in.readVInt();
    assert bitsPerValue > 0 && bitsPerValue <= 64: "bitsPerValue=" + bitsPerValue;
    final int valueCount = in.readVInt();

    switch (bitsPerValue) {
    case 8:
      return new Direct8(in, valueCount);
    case 16:
      return new Direct16(in, valueCount);
    case 32:
      return new Direct32(in, valueCount);
    case 64:
      return new Direct64(in, valueCount);
    default:
      if (Constants.JRE_IS_64BIT || bitsPerValue >= 32) {
        return new Packed64(in, valueCount, bitsPerValue);
      } else {
        return new Packed32(in, valueCount, bitsPerValue);
      }
    }
  }

  /**
   * Retrieve PackedInts as a {@link ReaderIterator}
   * @param in positioned at the beginning of a stored packed int structure.
   * @return an iterator to access the values
   * @throws IOException if the structure could not be retrieved.
   * @lucene.internal
   */
  public static ReaderIterator getReaderIterator(IndexInput in) throws IOException {
    CodecUtil.checkHeader(in, CODEC_NAME, VERSION_START, VERSION_START);
    final int bitsPerValue = in.readVInt();
    assert bitsPerValue > 0 && bitsPerValue <= 64: "bitsPerValue=" + bitsPerValue;
    final int valueCount = in.readVInt();
    return new PackedReaderIterator(bitsPerValue, valueCount, in);
  }
  
  /**
   * Retrieve PackedInts.Reader that does not load values
   * into RAM but rather accesses all values via the
   * provided IndexInput.
   * @param in positioned at the beginning of a stored packed int structure.
   * @return an Reader to access the values
   * @throws IOException if the structure could not be retrieved.
   * @lucene.internal
   */
  public static Reader getDirectReader(IndexInput in) throws IOException {
    CodecUtil.checkHeader(in, CODEC_NAME, VERSION_START, VERSION_START);
    final int bitsPerValue = in.readVInt();
    assert bitsPerValue > 0 && bitsPerValue <= 64: "bitsPerValue=" + bitsPerValue;
    final int valueCount = in.readVInt();
    return new DirectReader(bitsPerValue, valueCount, in);
  }
  
  /**
   * Create a packed integer array with the given amount of values initialized
   * to 0. the valueCount and the bitsPerValue cannot be changed after creation.
   * All Mutables known by this factory are kept fully in RAM.
   * @param valueCount   the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   * @return a mutable packed integer array.
   * @throws java.io.IOException if the Mutable could not be created. With the
   *         current implementations, this never happens, but the method
   *         signature allows for future persistence-backed Mutables.
   * @lucene.internal
   */
  public static Mutable getMutable(
         int valueCount, int bitsPerValue) {
    switch (bitsPerValue) {
    case 8:
      return new Direct8(valueCount);
    case 16:
      return new Direct16(valueCount);
    case 32:
      return new Direct32(valueCount);
    case 64:
      return new Direct64(valueCount);
    default:
      if (Constants.JRE_IS_64BIT || bitsPerValue >= 32) {
        return new Packed64(valueCount, bitsPerValue);
      } else {
        return new Packed32(valueCount, bitsPerValue);
      }
    }
  }

  /**
   * Create a packed integer array writer for the given number of values at the
   * given bits/value. Writers append to the given IndexOutput and has very
   * low memory overhead.
   * @param out          the destination for the produced bits.
   * @param valueCount   the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   * @return a Writer ready for receiving values.
   * @throws IOException if bits could not be written to out.
   * @lucene.internal
   */
  public static Writer getWriter(DataOutput out, int valueCount, int bitsPerValue)
    throws IOException {
    return new PackedWriter(out, valueCount, bitsPerValue);
  }

  /** Returns how many bits are required to hold values up
   *  to and including maxValue
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @lucene.internal
   */
  public static int bitsRequired(long maxValue) {
    // Very high long values does not translate well to double, so we do an
    // explicit check for the edge cases
    if (maxValue > 0x3FFFFFFFFFFFFFFFL) {
      return 63;
    } if (maxValue > 0x1FFFFFFFFFFFFFFFL) {
      return 62;
    }
    return Math.max(1, (int) Math.ceil(Math.log(1+maxValue)/Math.log(2.0)));
  }

  /**
   * Calculates the maximum unsigned long that can be expressed with the given
   * number of bits.
   * @param bitsPerValue the number of bits available for any given value.
   * @return the maximum value for the given bits.
   * @lucene.internal
   */
  public static long maxValue(int bitsPerValue) {
    return bitsPerValue == 64 ? Long.MAX_VALUE : ~(~0L << bitsPerValue);
  }

  /** Rounds bitsPerValue up to 8, 16, 32 or 64. */
  public static int getNextFixedSize(int bitsPerValue) {
    if (bitsPerValue <= 8) {
      return 8;
    } else if (bitsPerValue <= 16) {
      return 16;
    } else if (bitsPerValue <= 32) {
      return 32;
    } else {
      return 64;
    }
  }

  /** Possibly wastes some storage in exchange for faster lookups */
  public static int getRoundedFixedSize(int bitsPerValue) {
    if (bitsPerValue > 58 || (bitsPerValue < 32 && bitsPerValue > 29)) { // 10% space-waste is ok
      return getNextFixedSize(bitsPerValue);
    } else {
      return bitsPerValue;
    }
  }
}
