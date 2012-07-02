package org.apache.lucene.util.packed;

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

import java.io.Closeable;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

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

  /**
   * At most 700% memory overhead, always select a direct implementation.
   */
  public static final float FASTEST = 7f;

  /**
   * At most 50% memory overhead, always select a reasonably fast implementation.
   */
  public static final float FAST = 0.5f;

  /**
   * At most 20% memory overhead.
   */
  public static final float DEFAULT = 0.2f;

  /**
   * No memory overhead at all, but the returned implementation may be slow.
   */
  public static final float COMPACT = 0f;

  /**
   * Default amount of memory to use for bulk operations.
   */
  public static final int DEFAULT_BUFFER_SIZE = 1024; // 1K

  final static String CODEC_NAME = "PackedInts";
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  static final int PACKED = 0;
  static final int PACKED_SINGLE_BLOCK = 1;

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
     * Bulk get: read at least one and at most <code>len</code> longs starting
     * from <code>index</code> into <code>arr[off:off+len]</code> and return
     * the actual number of values that have been read.
     */
    int get(int index, long[] arr, int off, int len);

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
     * Return the in-memory size in bytes.
     */
    long ramBytesUsed();

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

  static abstract class ReaderIteratorImpl implements ReaderIterator {

    protected final IndexInput in;
    protected final int bitsPerValue;
    protected final int valueCount;

    protected ReaderIteratorImpl(int valueCount, int bitsPerValue, IndexInput in) {
      this.in = in;
      this.bitsPerValue = bitsPerValue;
      this.valueCount = valueCount;
    }

    @Override
    public int getBitsPerValue() {
      return bitsPerValue;
    }

    @Override
    public int size() {
      return valueCount;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
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
     * Bulk set: set at least one and at most <code>len</code> longs starting
     * at <code>off</code> in <code>arr</code> into this mutable, starting at
     * <code>index</code>. Returns the actual number of values that have been
     * set.
     */
    int set(int index, long[] arr, int off, int len);

    /**
     * Fill the mutable from <code>fromIndex</code> (inclusive) to
     * <code>toIndex</code> (exclusive) with <code>val</code>.
     */
    void fill(int fromIndex, int toIndex, long val);

    /**
     * Sets all values to 0.
     */
    void clear();

    /**
     * Save this mutable into <code>out</code>. Instantiating a reader from
     * the generated data will return a reader with the same number of bits
     * per value.
     */
    void save(DataOutput out) throws IOException;

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

    public Object getArray() {
      return null;
    }

    public boolean hasArray() {
      return false;
    }

    public int get(int index, long[] arr, int off, int len) {
      assert len > 0 : "len must be > 0 (got " + len + ")";
      assert index >= 0 && index < valueCount;
      assert off + len <= arr.length;

      final int gets = Math.min(valueCount - index, len);
      for (int i = index, o = off, end = index + gets; i < end; ++i, ++o) {
        arr[o] = get(i);
      }
      return gets;
    }

  }

  public static abstract class MutableImpl extends ReaderImpl implements Mutable {

    protected MutableImpl(int valueCount, int bitsPerValue) {
      super(valueCount, bitsPerValue);
    }

    public int set(int index, long[] arr, int off, int len) {
      assert len > 0 : "len must be > 0 (got " + len + ")";
      assert index >= 0 && index < valueCount;
      len = Math.min(len, valueCount - index);
      assert off + len <= arr.length;

      for (int i = index, o = off, end = index + len; i < end; ++i, ++o) {
        set(i, arr[o]);
      }
      return len;
    }

    public void fill(int fromIndex, int toIndex, long val) {
      assert val <= maxValue(bitsPerValue);
      assert fromIndex <= toIndex;
      for (int i = fromIndex; i < toIndex; ++i) {
        set(i, val);
      }
    }

    protected int getFormat() {
      return PACKED;
    }

    @Override
    public void save(DataOutput out) throws IOException {
      Writer writer = getWriterByFormat(out, valueCount, bitsPerValue, getFormat());
      for (int i = 0; i < valueCount; ++i) {
        writer.add(get(i));
      }
      writer.finish();
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
      out.writeVInt(getFormat());
    }

    protected abstract int getFormat();
    public abstract void add(long v) throws IOException;
    public abstract void finish() throws IOException;
  }

  /**
   * Retrieve PackedInt data from the DataInput and return a packed int
   * structure based on it.
   *
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
    final int format = in.readVInt();

    switch (format) {
      case PACKED:
        switch (bitsPerValue) {
          case 8:
            return new Direct8(in, valueCount);
          case 16:
            return new Direct16(in, valueCount);
          case 24:
            return new Packed8ThreeBlocks(in, valueCount);
          case 32:
            return new Direct32(in, valueCount);
          case 48:
            return new Packed16ThreeBlocks(in, valueCount);
          case 64:
            return new Direct64(in, valueCount);
          default:
            return new Packed64(in, valueCount, bitsPerValue);
        }
      case PACKED_SINGLE_BLOCK:
        return Packed64SingleBlock.create(in, valueCount, bitsPerValue);
      default:
        throw new AssertionError("Unknwown Writer format: " + format);
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
    final int format = in.readVInt();
    switch (format) {
      case PACKED:
        return new PackedReaderIterator(valueCount, bitsPerValue, in);
      case PACKED_SINGLE_BLOCK:
        return new Packed64SingleBlockReaderIterator(valueCount, bitsPerValue, in);
      default:
        throw new AssertionError("Unknwown Writer format: " + format);
    }
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
    final int format = in.readVInt();
    switch (format) {
      case PACKED:
        return new DirectPackedReader(bitsPerValue, valueCount, in);
      case PACKED_SINGLE_BLOCK:
        return new DirectPacked64SingleBlockReader(bitsPerValue, valueCount, in);
      default:
        throw new AssertionError("Unknwown Writer format: " + format);
    }
  }
  
  /**
   * Create a packed integer array with the given amount of values initialized
   * to 0. the valueCount and the bitsPerValue cannot be changed after creation.
   * All Mutables known by this factory are kept fully in RAM.
   * 
   * Positive values of <code>acceptableOverheadRatio</code> will trade space
   * for speed by selecting a faster but potentially less memory-efficient
   * implementation. An <code>acceptableOverheadRatio</code> of
   * {@link PackedInts#COMPACT} will make sure that the most memory-efficient
   * implementation is selected whereas {@link PackedInts#FASTEST} will make sure
   * that the fastest implementation is selected.
   *
   * @param valueCount   the number of elements
   * @param bitsPerValue the number of bits available for any given value
   * @param acceptableOverheadRatio an acceptable overhead
   *        ratio per value
   * @return a mutable packed integer array
   * @throws java.io.IOException if the Mutable could not be created. With the
   *         current implementations, this never happens, but the method
   *         signature allows for future persistence-backed Mutables.
   * @lucene.internal
   */
  public static Mutable getMutable(int valueCount,
      int bitsPerValue, float acceptableOverheadRatio) {
    acceptableOverheadRatio = Math.max(COMPACT, acceptableOverheadRatio);
    acceptableOverheadRatio = Math.min(FASTEST, acceptableOverheadRatio);
    float acceptableOverheadPerValue = acceptableOverheadRatio * bitsPerValue; // in bits

    int maxBitsPerValue = bitsPerValue + (int) acceptableOverheadPerValue;

    if (bitsPerValue <= 8 && maxBitsPerValue >= 8) {
      return new Direct8(valueCount);
    } else if (bitsPerValue <= 16 && maxBitsPerValue >= 16) {
      return new Direct16(valueCount);
    } else if (bitsPerValue <= 32 && maxBitsPerValue >= 32) {
      return new Direct32(valueCount);
    } else if (bitsPerValue <= 64 && maxBitsPerValue >= 64) {
      return new Direct64(valueCount);
    } else if (valueCount <= Packed8ThreeBlocks.MAX_SIZE && bitsPerValue <= 24 && maxBitsPerValue >= 24) {
      return new Packed8ThreeBlocks(valueCount);
    } else if (valueCount <= Packed16ThreeBlocks.MAX_SIZE && bitsPerValue <= 48 && maxBitsPerValue >= 48) {
      return new Packed16ThreeBlocks(valueCount);
    } else {
      for (int bpv = bitsPerValue; bpv <= maxBitsPerValue; ++bpv) {
        if (Packed64SingleBlock.isSupported(bpv)) {
          float overhead = Packed64SingleBlock.overheadPerValue(bpv);
          float acceptableOverhead = acceptableOverheadPerValue + bitsPerValue - bpv;
          if (overhead <= acceptableOverhead) {
            return Packed64SingleBlock.create(valueCount, bpv);
          }
        }
      }
      return new Packed64(valueCount, bitsPerValue);
    }
  }

  /**
   * Create a packed integer array writer for the given number of values at the
   * given bits/value. Writers append to the given IndexOutput and has very
   * low memory overhead.
   *
   * Positive values of <code>acceptableOverheadRatio</code> will trade space
   * for speed by selecting a faster but potentially less memory-efficient
   * implementation. An <code>acceptableOverheadRatio</code> of
   * {@link PackedInts#COMPACT} will make sure that the most memory-efficient
   * implementation is selected whereas {@link PackedInts#FASTEST} will make sure
   * that the fastest implementation is selected.
   *
   * @param out          the destination for the produced bits.
   * @param valueCount   the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   * @param acceptableOverheadRatio an acceptable overhead ratio per value
   * @return a Writer ready for receiving values.
   * @throws IOException if bits could not be written to out.
   * @lucene.internal
   */
  public static Writer getWriter(DataOutput out,
      int valueCount, int bitsPerValue, float acceptableOverheadRatio)
    throws IOException {
    acceptableOverheadRatio = Math.max(COMPACT, acceptableOverheadRatio);
    acceptableOverheadRatio = Math.min(FASTEST, acceptableOverheadRatio);
    float acceptableOverheadPerValue = acceptableOverheadRatio * bitsPerValue; // in bits

    int maxBitsPerValue = bitsPerValue + (int) acceptableOverheadPerValue;

    if (bitsPerValue <= 8 && maxBitsPerValue >= 8) {
      return getWriterByFormat(out, valueCount, 8, PACKED);
    } else if (bitsPerValue <= 16 && maxBitsPerValue >= 16) {
      return getWriterByFormat(out, valueCount, 16, PACKED);
    } else if (bitsPerValue <= 32 && maxBitsPerValue >= 32) {
      return getWriterByFormat(out, valueCount, 32, PACKED);
    } else if (bitsPerValue <= 64 && maxBitsPerValue >= 64) {
      return getWriterByFormat(out, valueCount, 64, PACKED);
    } else if (valueCount <= Packed8ThreeBlocks.MAX_SIZE && bitsPerValue <= 24 && maxBitsPerValue >= 24) {
      return getWriterByFormat(out, valueCount, 24, PACKED);
    } else if (valueCount <= Packed16ThreeBlocks.MAX_SIZE && bitsPerValue <= 48 && maxBitsPerValue >= 48) {
      return getWriterByFormat(out, valueCount, 48, PACKED);
    } else {
      for (int bpv = bitsPerValue; bpv <= maxBitsPerValue; ++bpv) {
        if (Packed64SingleBlock.isSupported(bpv)) {
          float overhead = Packed64SingleBlock.overheadPerValue(bpv);
          float acceptableOverhead = acceptableOverheadPerValue + bitsPerValue - bpv;
          if (overhead <= acceptableOverhead) {
            return getWriterByFormat(out, valueCount, bpv, PACKED_SINGLE_BLOCK);
          }
        }
      }
      return getWriterByFormat(out, valueCount, bitsPerValue, PACKED);
    }
  }

  private static Writer getWriterByFormat(DataOutput out,
      int valueCount, int bitsPerValue, int format) throws IOException {
    switch (format) {
      case PACKED:
        return new PackedWriter(out, valueCount, bitsPerValue);
      case PACKED_SINGLE_BLOCK:
        return new Packed64SingleBlockWriter(out, valueCount, bitsPerValue);
      default:
        throw new IllegalArgumentException("Unknown format " + format);
    }
  }

  /** Returns how many bits are required to hold values up
   *  to and including maxValue
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @lucene.internal
   */
  public static int bitsRequired(long maxValue) {
    if (maxValue < 0) {
      throw new IllegalArgumentException("maxValue must be non-negative (got: " + maxValue + ")");
    }
    return Math.max(1, 64 - Long.numberOfLeadingZeros(maxValue));
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

  /**
   * Copy <code>src[srcPos:srcPos+len]</code> into
   * <code>dest[destPos:destPos+len]</code> using at most <code>mem</code>
   * bytes.
   */
  public static void copy(Reader src, int srcPos, Mutable dest, int destPos, int len, int mem) {
    assert srcPos + len <= src.size();
    assert destPos + len <= dest.size();
    final int capacity = mem >>> 3;
    if (capacity == 0) {
      for (int i = 0; i < len; ++i) {
        dest.set(destPos++, src.get(srcPos++));
      }
    } else {
      // use bulk operations
      long[] buf = new long[Math.min(capacity, len)];
      int remaining = 0;
      while (len > 0) {
        final int read = src.get(srcPos, buf, remaining, Math.min(len, buf.length - remaining));
        assert read > 0;
        srcPos += read;
        len -= read;
        remaining += read;
        final int written = dest.set(destPos, buf, 0, remaining);
        assert written > 0;
        destPos += written;
        if (written < remaining) {
          System.arraycopy(buf, written, buf, 0, remaining - written);
        }
        remaining -= written;
      }
      while (remaining > 0) {
        final int written = dest.set(destPos, buf, 0, remaining);
        remaining -= written;
      }
    }
  }

}
