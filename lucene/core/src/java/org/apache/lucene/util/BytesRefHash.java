package org.apache.lucene.util;

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

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SHIFT;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.ByteBlockPool.DirectAllocator;

/**
 * {@link BytesRefHash} is a special purpose hash-map like data-structure
 * optimized for {@link BytesRef} instances. BytesRefHash maintains mappings of
 * byte arrays to ordinal (Map<BytesRef,int>) storing the hashed bytes
 * efficiently in continuous storage. The mapping to the ordinal is
 * encapsulated inside {@link BytesRefHash} and is guaranteed to be increased
 * for each added {@link BytesRef}.
 * 
 * <p>
 * Note: The maximum capacity {@link BytesRef} instance passed to
 * {@link #add(BytesRef)} must not be longer than {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2. 
 * The internal storage is limited to 2GB total byte storage.
 * </p>
 * 
 * @lucene.internal
 */
public final class BytesRefHash {

  public static final int DEFAULT_CAPACITY = 16;

  // the following fields are needed by comparator,
  // so package private to prevent access$-methods:
  final ByteBlockPool pool;
  int[] bytesStart;

  private final BytesRef scratch1 = new BytesRef();
  private int hashSize;
  private int hashHalfSize;
  private int hashMask;
  private int count;
  private int lastCount = -1;
  private int[] ords;
  private final BytesStartArray bytesStartArray;
  private Counter bytesUsed;

  /**
   * Creates a new {@link BytesRefHash} with a {@link ByteBlockPool} using a
   * {@link DirectAllocator}.
   */
  public BytesRefHash() { 
    this(new ByteBlockPool(new DirectAllocator()));
  }

  /**
   * Creates a new {@link BytesRefHash}
   */
  public BytesRefHash(ByteBlockPool pool) {
    this(pool, DEFAULT_CAPACITY, new DirectBytesStartArray(DEFAULT_CAPACITY));
  }

  /**
   * Creates a new {@link BytesRefHash}
   */
  public BytesRefHash(ByteBlockPool pool, int capacity,
      BytesStartArray bytesStartArray) {
    hashSize = capacity;
    hashHalfSize = hashSize >> 1;
    hashMask = hashSize - 1;
    this.pool = pool;
    ords = new int[hashSize];
    Arrays.fill(ords, -1);
    this.bytesStartArray = bytesStartArray;
    bytesStart = bytesStartArray.init();
    bytesUsed = bytesStartArray.bytesUsed() == null? Counter.newCounter() : bytesStartArray.bytesUsed();
    bytesUsed.addAndGet(hashSize * RamUsageEstimator.NUM_BYTES_INT);
  }

  /**
   * Returns the number of {@link BytesRef} values in this {@link BytesRefHash}.
   * 
   * @return the number of {@link BytesRef} values in this {@link BytesRefHash}.
   */
  public int size() {
    return count;
  }

  /**
   * Populates and returns a {@link BytesRef} with the bytes for the given ord.
   * <p>
   * Note: the given ord must be a positive integer less that the current size (
   * {@link #size()})
   * </p>
   *
   * @param ord the ord
   * @param ref the {@link BytesRef} to populate
   * 
   * @return the given BytesRef instance populated with the bytes for the given ord
   */
  public BytesRef get(int ord, BytesRef ref) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert ord < bytesStart.length: "ord exceeds byteStart len: " + bytesStart.length;
    return pool.setBytesRef(ref, bytesStart[ord]);
  }

  /**
   * Returns the ords array in arbitrary order. Valid ords start at offset of 0
   * and end at a limit of {@link #size()} - 1
   * <p>
   * Note: This is a destructive operation. {@link #clear()} must be called in
   * order to reuse this {@link BytesRefHash} instance.
   * </p>
   */
  public int[] compact() {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    int upto = 0;
    for (int i = 0; i < hashSize; i++) {
      if (ords[i] != -1) {
        if (upto < i) {
          ords[upto] = ords[i];
          ords[i] = -1;
        }
        upto++;
      }
    }

    assert upto == count;
    lastCount = count;
    return ords;
  }

  /**
   * Returns the values array sorted by the referenced byte values.
   * <p>
   * Note: This is a destructive operation. {@link #clear()} must be called in
   * order to reuse this {@link BytesRefHash} instance.
   * </p>
   * 
   * @param comp
   *          the {@link Comparator} used for sorting
   */
  public int[] sort(final Comparator<BytesRef> comp) {
    final int[] compact = compact();
    new SorterTemplate() {
      @Override
      protected void swap(int i, int j) {
        final int o = compact[i];
        compact[i] = compact[j];
        compact[j] = o;
      }
      
      @Override
      protected int compare(int i, int j) {
        final int ord1 = compact[i], ord2 = compact[j];
        assert bytesStart.length > ord1 && bytesStart.length > ord2;
        return comp.compare(pool.setBytesRef(scratch1, bytesStart[ord1]),
          pool.setBytesRef(scratch2, bytesStart[ord2]));
      }

      @Override
      protected void setPivot(int i) {
        final int ord = compact[i];
        assert bytesStart.length > ord;
        pool.setBytesRef(pivot, bytesStart[ord]);
      }
  
      @Override
      protected int comparePivot(int j) {
        final int ord = compact[j];
        assert bytesStart.length > ord;
        return comp.compare(pivot,
          pool.setBytesRef(scratch2, bytesStart[ord]));
      }
      
      private final BytesRef pivot = new BytesRef(),
        scratch1 = new BytesRef(), scratch2 = new BytesRef();
    }.quickSort(0, count - 1);
    return compact;
  }

  private boolean equals(int ord, BytesRef b) {
    return pool.setBytesRef(scratch1, bytesStart[ord]).bytesEquals(b);
  }

  private boolean shrink(int targetSize) {
    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = hashSize;
    while (newSize >= 8 && newSize / 4 > targetSize) {
      newSize /= 2;
    }
    if (newSize != hashSize) {
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT
          * -(hashSize - newSize));
      hashSize = newSize;
      ords = new int[hashSize];
      Arrays.fill(ords, -1);
      hashHalfSize = newSize / 2;
      hashMask = newSize - 1;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Clears the {@link BytesRef} which maps to the given {@link BytesRef}
   */
  public void clear(boolean resetPool) {
    lastCount = count;
    count = 0;
    if (resetPool) {
      pool.dropBuffersAndReset();
    }
    bytesStart = bytesStartArray.clear();
    if (lastCount != -1 && shrink(lastCount)) {
      // shrink clears the hash entries
      return;
    }
    Arrays.fill(ords, -1);
  }

  public void clear() {
    clear(true);
  }
  
  /**
   * Closes the BytesRefHash and releases all internally used memory
   */
  public void close() {
    clear(true);
    ords = null;
    bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT
        * -hashSize);
  }

  /**
   * Adds a new {@link BytesRef}
   * 
   * @param bytes
   *          the bytes to hash
   * @return the ord the given bytes are hashed if there was no mapping for the
   *         given bytes, otherwise <code>(-(ord)-1)</code>. This guarantees
   *         that the return value will always be &gt;= 0 if the given bytes
   *         haven't been hashed before.
   * 
   * @throws MaxBytesLengthExceededException
   *           if the given bytes are > 2 +
   *           {@link ByteBlockPool#BYTE_BLOCK_SIZE}
   */
  public int add(BytesRef bytes) {
    return add(bytes, bytes.hashCode());
  }

  /**
   * Adds a new {@link BytesRef} with a pre-calculated hash code.
   * 
   * @param bytes
   *          the bytes to hash
   * @param code
   *          the bytes hash code
   * 
   *          <p>
   *          Hashcode is defined as:
   * 
   *          <pre>
   * int hash = 0;
   * for (int i = offset; i &lt; offset + length; i++) {
   *   hash = 31 * hash + bytes[i];
   * }
   * </pre>
   * 
   * @return the ord the given bytes are hashed if there was no mapping for the
   *         given bytes, otherwise <code>(-(ord)-1)</code>. This guarantees
   *         that the return value will always be &gt;= 0 if the given bytes
   *         haven't been hashed before.
   * 
   * @throws MaxBytesLengthExceededException
   *           if the given bytes are >
   *           {@link ByteBlockPool#BYTE_BLOCK_SIZE} - 2
   */
  public int add(BytesRef bytes, int code) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    final int length = bytes.length;
    // final position
    int hashPos = code & hashMask;
    int e = ords[hashPos];
    if (e != -1 && !equals(e, bytes)) {
      // Conflict: keep searching different locations in
      // the hash table.
      final int inc = ((code >> 8) + code) | 1;
      do {
        code += inc;
        hashPos = code & hashMask;
        e = ords[hashPos];
      } while (e != -1 && !equals(e, bytes));
    }

    if (e == -1) {
      // new entry
      final int len2 = 2 + bytes.length;
      if (len2 + pool.byteUpto > BYTE_BLOCK_SIZE) {
        if (len2 > BYTE_BLOCK_SIZE) {
          throw new MaxBytesLengthExceededException("bytes can be at most "
              + (BYTE_BLOCK_SIZE - 2) + " in length; got " + bytes.length);
        }
        pool.nextBuffer();
      }
      final byte[] buffer = pool.buffer;
      final int bufferUpto = pool.byteUpto;
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: "
            + bytesStart.length;
      }
      e = count++;

      bytesStart[e] = bufferUpto + pool.byteOffset;

      // We first encode the length, followed by the
      // bytes. Length is encoded as vInt, but will consume
      // 1 or 2 bytes at most (we reject too-long terms,
      // above).
      if (length < 128) {
        // 1 byte to store length
        buffer[bufferUpto] = (byte) length;
        pool.byteUpto += length + 1;
        assert length >= 0: "Length must be positive: " + length;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 1,
            length);
      } else {
        // 2 byte to store length
        buffer[bufferUpto] = (byte) (0x80 | (length & 0x7f));
        buffer[bufferUpto + 1] = (byte) ((length >> 7) & 0xff);
        pool.byteUpto += length + 2;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 2,
            length);
      }
      assert ords[hashPos] == -1;
      ords[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, true);
      }
      return e;
    }
    return -(e + 1);
  }

  public int addByPoolOffset(int offset) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    // final position
    int code = offset;
    int hashPos = offset & hashMask;
    int e = ords[hashPos];
    if (e != -1 && bytesStart[e] != offset) {
      // Conflict: keep searching different locations in
      // the hash table.
      final int inc = ((code >> 8) + code) | 1;
      do {
        code += inc;
        hashPos = code & hashMask;
        e = ords[hashPos];
      } while (e != -1 && bytesStart[e] != offset);
    }
    if (e == -1) {
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: "
            + bytesStart.length;
      }
      e = count++;
      bytesStart[e] = offset;
      assert ords[hashPos] == -1;
      ords[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, false);
      }
      return e;
    }
    return -(e + 1);
  }

  /**
   * Called when hash is too small (> 50% occupied) or too large (< 20%
   * occupied).
   */
  private void rehash(final int newSize, boolean hashOnData) {
    final int newMask = newSize - 1;
    bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT * (newSize));
    final int[] newHash = new int[newSize];
    Arrays.fill(newHash, -1);
    for (int i = 0; i < hashSize; i++) {
      final int e0 = ords[i];
      if (e0 != -1) {
        int code;
        if (hashOnData) {
          final int off = bytesStart[e0];
          final int start = off & BYTE_BLOCK_MASK;
          final byte[] bytes = pool.buffers[off >> BYTE_BLOCK_SHIFT];
          code = 0;
          final int len;
          int pos;
          if ((bytes[start] & 0x80) == 0) {
            // length is 1 byte
            len = bytes[start];
            pos = start + 1;
          } else {
            len = (bytes[start] & 0x7f) + ((bytes[start + 1] & 0xff) << 7);
            pos = start + 2;
          }

          final int endPos = pos + len;
          while (pos < endPos) {
            code = 31 * code + bytes[pos++];
          }
        } else {
          code = bytesStart[e0];
        }

        int hashPos = code & newMask;
        assert hashPos >= 0;
        if (newHash[hashPos] != -1) {
          final int inc = ((code >> 8) + code) | 1;
          do {
            code += inc;
            hashPos = code & newMask;
          } while (newHash[hashPos] != -1);
        }
        newHash[hashPos] = e0;
      }
    }

    hashMask = newMask;
    bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT * (-ords.length));
    ords = newHash;
    hashSize = newSize;
    hashHalfSize = newSize / 2;
  }

  /**
   * reinitializes the {@link BytesRefHash} after a previous {@link #clear()}
   * call. If {@link #clear()} has not been called previously this method has no
   * effect.
   */
  public void reinit() {
    if (bytesStart == null) {
      bytesStart = bytesStartArray.init();
    }
    
    if (ords == null) {
      ords = new int[hashSize];
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_INT * hashSize);
    }
  }

  /**
   * Returns the bytesStart offset into the internally used
   * {@link ByteBlockPool} for the given ord
   * 
   * @param ord
   *          the ord to look up
   * @return the bytesStart offset into the internally used
   *         {@link ByteBlockPool} for the given ord
   */
  public int byteStart(int ord) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    assert ord >= 0 && ord < count : ord;
    return bytesStart[ord];
  }

  /**
   * Thrown if a {@link BytesRef} exceeds the {@link BytesRefHash} limit of
   * {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2.
   */
  @SuppressWarnings("serial")
  public static class MaxBytesLengthExceededException extends RuntimeException {
    MaxBytesLengthExceededException(String message) {
      super(message);
    }
  }

  /** Manages allocation of the per-term addresses. */
  public abstract static class BytesStartArray {
    /**
     * Initializes the BytesStartArray. This call will allocate memory
     * 
     * @return the initialized bytes start array
     */
    public abstract int[] init();

    /**
     * Grows the {@link BytesStartArray}
     * 
     * @return the grown array
     */
    public abstract int[] grow();

    /**
     * clears the {@link BytesStartArray} and returns the cleared instance.
     * 
     * @return the cleared instance, this might be <code>null</code>
     */
    public abstract int[] clear();

    /**
     * A {@link Counter} reference holding the number of bytes used by this
     * {@link BytesStartArray}. The {@link BytesRefHash} uses this reference to
     * track it memory usage
     * 
     * @return a {@link AtomicLong} reference holding the number of bytes used
     *         by this {@link BytesStartArray}.
     */
    public abstract Counter bytesUsed();
  }
  
  /** A simple {@link BytesStartArray} that tracks all
   *  memory allocation using a shared {@link Counter}
   *  instance.  */
  public static class TrackingDirectBytesStartArray extends BytesStartArray {
    protected final int initSize;
    private int[] bytesStart;
    protected final Counter bytesUsed;
    
    public TrackingDirectBytesStartArray(int initSize, Counter bytesUsed) {
      this.initSize = initSize;
      this.bytesUsed = bytesUsed;
    }

    @Override
    public int[] clear() {
      if (bytesStart != null) {
        bytesUsed.addAndGet(-bytesStart.length * RamUsageEstimator.NUM_BYTES_INT);
      }
      return bytesStart = null;
    }

    @Override
    public int[] grow() {
      assert bytesStart != null;
      final int oldSize = bytesStart.length;
      bytesStart = ArrayUtil.grow(bytesStart, bytesStart.length + 1);
      bytesUsed.addAndGet((bytesStart.length - oldSize) * RamUsageEstimator.NUM_BYTES_INT);
      return bytesStart;
    }

    @Override
    public int[] init() {
      bytesStart = new int[ArrayUtil.oversize(initSize,
          RamUsageEstimator.NUM_BYTES_INT)];
      bytesUsed.addAndGet((bytesStart.length) * RamUsageEstimator.NUM_BYTES_INT);
      return bytesStart;
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }

  /** A simple {@link BytesStartArray} that tracks
   *  memory allocation using a private {@link AtomicLong}
   *  instance.  */
  public static class DirectBytesStartArray extends BytesStartArray {
    // TODO: can't we just merge this w/
    // TrackingDirectBytesStartArray...?  Just add a ctor
    // that makes a private bytesUsed?

    protected final int initSize;
    private int[] bytesStart;
    private final Counter bytesUsed;
    
    public DirectBytesStartArray(int initSize) {
      this.bytesUsed = Counter.newCounter();
      this.initSize = initSize;
    }

    @Override
    public int[] clear() {
      return bytesStart = null;
    }

    @Override
    public int[] grow() {
      assert bytesStart != null;
      return bytesStart = ArrayUtil.grow(bytesStart, bytesStart.length + 1);
    }

    @Override
    public int[] init() {
      return bytesStart = new int[ArrayUtil.oversize(initSize,
          RamUsageEstimator.NUM_BYTES_INT)];
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }
}
