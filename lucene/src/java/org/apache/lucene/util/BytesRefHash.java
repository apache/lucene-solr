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

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SHIFT;

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
 * The internal storage is limited to 2GB totalbyte storage.
 * </p>
 * 
 * @lucene.internal
 */
public final class BytesRefHash {

  private final ByteBlockPool pool;
  private int hashSize;
  private int hashHalfSize;
  private int hashMask;
  private int count;
  private int lastCount = -1;
  private int[] ords;
  private int[] bytesStart;
  public static final int DEFAULT_CAPACITY = 16;
  private final BytesStartArray bytesStartArray;
  private AtomicLong bytesUsed;

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
    bytesUsed = bytesStartArray.bytesUsed();
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
   * Returns the {@link BytesRef} value for the given ord.
   * <p>
   * Note: the given ord must be a positive integer less that the current size (
   * {@link #size()})
   * </p>
   * 
   * @param ord
   *          the ord
   * 
   * @return a BytesRef instance for the given ord
   */
  public BytesRef get(int ord) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert ord < bytesStart.length: "ord exceeeds byteStart len: " + bytesStart.length;
    return pool.setBytesRef(scratch1, bytesStart[ord]);
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
  public int[] sort(Comparator<BytesRef> comp) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    final int[] compact = compact();
    quickSort(comp, compact, 0, count - 1);
    return compact;
  }

  private void quickSort(Comparator<BytesRef> comp, int[] entries, int lo,
      int hi) {
    if (lo >= hi)
      return;
    if (hi == 1 + lo) {
      if (compare(comp, entries[lo], entries[hi]) > 0) {
        final int tmp = entries[lo];
        entries[lo] = entries[hi];
        entries[hi] = tmp;
      }
      return;
    }
    final int mid = (lo + hi) >>> 1;
    if (compare(comp, entries[lo], entries[mid]) > 0) {
      int tmp = entries[lo];
      entries[lo] = entries[mid];
      entries[mid] = tmp;
    }

    if (compare(comp, entries[mid], entries[hi]) > 0) {
      int tmp = entries[mid];
      entries[mid] = entries[hi];
      entries[hi] = tmp;

      if (compare(comp, entries[lo], entries[mid]) > 0) {
        int tmp2 = entries[lo];
        entries[lo] = entries[mid];
        entries[mid] = tmp2;
      }
    }
    int left = lo + 1;
    int right = hi - 1;

    if (left >= right)
      return;

    final int partition = entries[mid];

    for (;;) {
      while (compare(comp, entries[right], partition) > 0)
        --right;

      while (left < right && compare(comp, entries[left], partition) <= 0)
        ++left;

      if (left < right) {
        final int tmp = entries[left];
        entries[left] = entries[right];
        entries[right] = tmp;
        --right;
      } else {
        break;
      }
    }

    quickSort(comp, entries, lo, left);
    quickSort(comp, entries, left + 1, hi);
  }

  private final BytesRef scratch1 = new BytesRef();
  private final BytesRef scratch2 = new BytesRef();

  private boolean equals(int ord, BytesRef b) {
    return pool.setBytesRef(scratch1, bytesStart[ord]).bytesEquals(b);
  }

  private int compare(Comparator<BytesRef> comp, int ord1, int ord2) {
    assert bytesStart.length > ord1 && bytesStart.length > ord2;
    return comp.compare(pool.setBytesRef(scratch1, bytesStart[ord1]),
        pool.setBytesRef(scratch2, bytesStart[ord2]));
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
    if (resetPool)
      pool.reset();
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
            code = BytesRef.HASH_PRIME * code + bytes[pos++];
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
    if (bytesStart == null)
      bytesStart = bytesStartArray.init();
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
     * A {@link AtomicLong} reference holding the number of bytes used by this
     * {@link BytesStartArray}. The {@link BytesRefHash} uses this reference to
     * track it memory usage
     * 
     * @return a {@link AtomicLong} reference holding the number of bytes used
     *         by this {@link BytesStartArray}.
     */
    public abstract AtomicLong bytesUsed();
  }

  static class DirectBytesStartArray extends BytesStartArray {

    private final int initSize;
    private int[] bytesStart;
    private final AtomicLong bytesUsed = new AtomicLong(0);

    DirectBytesStartArray(int initSize) {
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
    public AtomicLong bytesUsed() {
      return bytesUsed;
    }

  }
}
