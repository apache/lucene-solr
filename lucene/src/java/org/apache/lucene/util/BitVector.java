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

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/** Optimized implementation of a vector of bits.  This is more-or-less like
 *  java.util.BitSet, but also includes the following:
 *  <ul>
 *  <li>a count() method, which efficiently computes the number of one bits;</li>
 *  <li>optimized read from and write to disk;</li>
 *  <li>inlinable get() method;</li>
 *  <li>store and load, as bit set or d-gaps, depending on sparseness;</li> 
 *  </ul>
 *
 *  @lucene.internal
 */
public final class BitVector implements Cloneable, Bits {

  private byte[] bits;
  private int size;
  private int count;

  /** Constructs a vector capable of holding <code>n</code> bits. */
  public BitVector(int n) {
    size = n;
    bits = new byte[getNumBytes(size)];
    count = 0;
  }

  BitVector(byte[] bits, int size) {
    this.bits = bits;
    this.size = size;
    count = -1;
  }
  
  private int getNumBytes(int size) {
    int bytesLength = size >>> 3;
    if ((size & 7) != 0) {
      bytesLength++;
    }
    return bytesLength;
  }
  
  @Override
  public Object clone() {
    byte[] copyBits = new byte[bits.length];
    System.arraycopy(bits, 0, copyBits, 0, bits.length);
    BitVector clone = new BitVector(copyBits, size);
    clone.count = count;
    return clone;
  }
  
  /** Sets the value of <code>bit</code> to one. */
  public final void set(int bit) {
    if (bit >= size) {
      throw new ArrayIndexOutOfBoundsException("bit=" + bit + " size=" + size);
    }
    bits[bit >> 3] |= 1 << (bit & 7);
    count = -1;
  }

  /** Sets the value of <code>bit</code> to true, and
   *  returns true if bit was already set */
  public final boolean getAndSet(int bit) {
    if (bit >= size) {
      throw new ArrayIndexOutOfBoundsException("bit=" + bit + " size=" + size);
    }
    final int pos = bit >> 3;
    final int v = bits[pos];
    final int flag = 1 << (bit & 7);
    if ((flag & v) != 0)
      return true;
    else {
      bits[pos] = (byte) (v | flag);
      if (count != -1)
        count++;
      return false;
    }
  }

  /** Sets the value of <code>bit</code> to zero. */
  public final void clear(int bit) {
    if (bit >= size) {
      throw new ArrayIndexOutOfBoundsException(bit);
    }
    bits[bit >> 3] &= ~(1 << (bit & 7));
    count = -1;
  }

  /** Returns <code>true</code> if <code>bit</code> is one and
    <code>false</code> if it is zero. */
  public final boolean get(int bit) {
    assert bit >= 0 && bit < size: "bit " + bit + " is out of bounds 0.." + (size-1);
    return (bits[bit >> 3] & (1 << (bit & 7))) != 0;
  }

  /** Returns the number of bits in this vector.  This is also one greater than
    the number of the largest valid bit number. */
  public final int size() {
    return size;
  }

  /** Returns the number of bits in this vector.  This is also one greater than
    the number of the largest valid bit number. */
  public final int length() {
    return size;
  }

  /** Returns the total number of one bits in this vector.  This is efficiently
    computed and cached, so that, if the vector is not changed, no
    recomputation is done for repeated calls. */
  public final int count() {
    // if the vector has been modified
    if (count == -1) {
      int c = 0;
      int end = bits.length;
      for (int i = 0; i < end; i++)
        c += BYTE_COUNTS[bits[i] & 0xFF];	  // sum bits per byte
      count = c;
    }
    return count;
  }

  /** For testing */
  public final int getRecomputedCount() {
    int c = 0;
    int end = bits.length;
    for (int i = 0; i < end; i++)
      c += BYTE_COUNTS[bits[i] & 0xFF];	  // sum bits per byte
    return c;
  }

  private static final byte[] BYTE_COUNTS = {	  // table of bits/byte
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
  };

  private static String CODEC = "BitVector";

  // Version before version tracking was added:
  private final static int VERSION_PRE = -1;

  // First version:
  private final static int VERSION_START = 0;

  // Increment version to change it:
  private final static int VERSION_CURRENT = VERSION_START;

  /** Writes this vector to the file <code>name</code> in Directory
    <code>d</code>, in a format that can be read by the constructor {@link
    #BitVector(Directory, String)}.  */
  public final void write(Directory d, String name) throws IOException {
    IndexOutput output = d.createOutput(name);
    try {
      output.writeInt(-2);
      CodecUtil.writeHeader(output, CODEC, VERSION_CURRENT);
      if (isSparse()) { 
        writeDgaps(output); // sparse bit-set more efficiently saved as d-gaps.
      } else {
        writeBits(output);
      }
    } finally {
      output.close();
    }
  }
     
  /** Write as a bit set */
  private void writeBits(IndexOutput output) throws IOException {
    output.writeInt(size());        // write size
    output.writeInt(count());       // write count
    output.writeBytes(bits, bits.length);
  }
  
  /** Write as a d-gaps list */
  private void writeDgaps(IndexOutput output) throws IOException {
    output.writeInt(-1);            // mark using d-gaps                         
    output.writeInt(size());        // write size
    output.writeInt(count());       // write count
    int last=0;
    int n = count();
    int m = bits.length;
    for (int i=0; i<m && n>0; i++) {
      if (bits[i]!=0) {
        output.writeVInt(i-last);
        output.writeByte(bits[i]);
        last = i;
        n -= BYTE_COUNTS[bits[i] & 0xFF];
      }
    }
  }

  /** Indicates if the bit vector is sparse and should be saved as a d-gaps list, or dense, and should be saved as a bit set. */
  private boolean isSparse() {

    final int setCount = count();
    if (setCount == 0) {
      return true;
    }

    final int avgGapLength = bits.length / setCount;

    // expected number of bytes for vInt encoding of each gap
    final int expectedDGapBytes;
    if (avgGapLength <= (1<< 7)) {
      expectedDGapBytes = 1;
    } else if (avgGapLength <= (1<<14)) {
      expectedDGapBytes = 2;
    } else if (avgGapLength <= (1<<21)) {
      expectedDGapBytes = 3;
    } else if (avgGapLength <= (1<<28)) {
      expectedDGapBytes = 4;
    } else {
      expectedDGapBytes = 5;
    }

    // +1 because we write the byte itself that contains the
    // set bit
    final int bytesPerSetBit = expectedDGapBytes + 1;
    
    // note: adding 32 because we start with ((int) -1) to indicate d-gaps format.
    final long expectedBits = 32 + 8 * bytesPerSetBit * count();

    // note: factor is for read/write of byte-arrays being faster than vints.  
    final long factor = 10;  
    return factor * expectedBits < size();
  }

  /** Constructs a bit vector from the file <code>name</code> in Directory
    <code>d</code>, as written by the {@link #write} method.
    */
  public BitVector(Directory d, String name) throws IOException {
    IndexInput input = d.openInput(name);

    try {
      final int firstInt = input.readInt();
      final int version;
      if (firstInt == -2) {
        // New format, with full header & version:
        version = CodecUtil.checkHeader(input, CODEC, VERSION_START, VERSION_START);
        size = input.readInt();
      } else {
        version = VERSION_PRE;
        size = firstInt;
      }
      if (size == -1) {
        readDgaps(input);
      } else {
        readBits(input);
      }
    } finally {
      input.close();
    }
  }

  /** Read as a bit set */
  private void readBits(IndexInput input) throws IOException {
    count = input.readInt();        // read count
    bits = new byte[getNumBytes(size)];     // allocate bits
    input.readBytes(bits, 0, bits.length);
  }

  /** read as a d-gaps list */ 
  private void readDgaps(IndexInput input) throws IOException {
    size = input.readInt();       // (re)read size
    count = input.readInt();        // read count
    bits = new byte[(size >> 3) + 1];     // allocate bits
    int last=0;
    int n = count();
    while (n>0) {
      last += input.readVInt();
      bits[last] = input.readByte();
      n -= BYTE_COUNTS[bits[last] & 0xFF];
    }          
  }
}
