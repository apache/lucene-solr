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
package org.apache.lucene.util.packed;



/**
 * Non-specialized {@link BulkOperation} for {@link PackedInts.Format#PACKED}.
 */
class BulkOperationPacked extends BulkOperation {

  private final int bitsPerValue;
  private final int longBlockCount;
  private final int longValueCount;
  private final int byteBlockCount;
  private final int byteValueCount;
  private final long mask;
  private final int intMask;
  private final int byteOffset;
  private final int bitsUsedOffset;
  private final int longOffset;

  public BulkOperationPacked(int bitsPerValue) {
    this.bitsPerValue = bitsPerValue;
    assert bitsPerValue > 0 && bitsPerValue <= 64;
    int blocks = bitsPerValue;
    while ((blocks & 1) == 0) {
      blocks >>>= 1;
    }
    this.longBlockCount = blocks;
    this.longValueCount = 64 * longBlockCount / bitsPerValue;
    int byteBlockCount = 8 * longBlockCount;
    int byteValueCount = longValueCount;
    while ((byteBlockCount & 1) == 0 && (byteValueCount & 1) == 0) {
      byteBlockCount >>>= 1;
      byteValueCount >>>= 1;
    }
    this.byteBlockCount = byteBlockCount;
    this.byteValueCount = byteValueCount;
    if (bitsPerValue == 64) {
      this.mask = ~0L;
    } else {
      this.mask = (1L << bitsPerValue) - 1;
    }
    this.intMask = (int) mask;
    assert longValueCount * bitsPerValue == 64 * longBlockCount;
    this.byteOffset = 8 - bitsPerValue;
    this.bitsUsedOffset = bitsPerValue - 8;
    this.longOffset = 64 - bitsPerValue;
  }

  @Override
  public int longBlockCount() {
    return longBlockCount;
  }

  @Override
  public int longValueCount() {
    return longValueCount;
  }

  @Override
  public int byteBlockCount() {
    return byteBlockCount;
  }

  @Override
  public int byteValueCount() {
    return byteValueCount;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    int bitsUsed = 0;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      if (bitsUsed == longOffset) {
        values[valuesOffset++] = (blocks[blocksOffset++] >>> bitsUsed);
        bitsUsed = 0;
      } else if (bitsUsed > longOffset) {
        values[valuesOffset++] = (blocks[blocksOffset++] >>> bitsUsed)
                | ((blocks[blocksOffset] << (64 - bitsUsed)) & mask);
        bitsUsed -= longOffset;
      } else {
        values[valuesOffset++] = (blocks[blocksOffset] >>> bitsUsed) & mask;
        bitsUsed += bitsPerValue;
      }
    }
    assert bitsUsed == 0;
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values,
                     int valuesOffset, int iterations) {
    long nextValue = 0L;
    int bitsUsed = 0;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final long bytes = blocks[blocksOffset++] & 0xFFL;
      if (bitsUsed < bitsUsedOffset) {
        // just buffer
        nextValue |= bytes << bitsUsed;
        bitsUsed += 8;
      } else {
        // flush
        values[valuesOffset++] = nextValue | ((bytes << bitsUsed) & mask);
        int bits = bitsPerValue - bitsUsed;
        while (bits <= byteOffset) {
          values[valuesOffset++] = (bytes >>> bits) & mask;
          bits += bitsPerValue;
        }
        nextValue = bytes >>> bits;
        bitsUsed = 8 - bits;
      }
    }
    assert bitsUsed == 0;
  }
  
  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    int bitsUsed = 0;
    for (int i = 0; i < longValueCount * iterations; ++i) { ;
      if (bitsUsed == longOffset) {
        values[valuesOffset++] = (int) (blocks[blocksOffset++] >>> bitsUsed);
        bitsUsed = 0;
      } else if (bitsUsed > longOffset) {
        values[valuesOffset++] = (int)((blocks[blocksOffset++] >>> bitsUsed)
                | ((blocks[blocksOffset] << (64 - bitsUsed)) & mask));
        bitsUsed -= longOffset;
      } else {
        values[valuesOffset++] = (int)(blocks[blocksOffset] >>> bitsUsed) & intMask;
        bitsUsed += bitsPerValue;
      }
    }
    assert bitsUsed == 0;
  }
  
  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    int nextValue = 0;
    int bitsUsed = 0;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final int bytes = blocks[blocksOffset++] & 0xFF;
      if (bitsUsed < bitsUsedOffset) {
        // just buffer
        nextValue |= bytes << bitsUsed;
        bitsUsed += 8;
      } else {
        // flush
        values[valuesOffset++] = (nextValue | ((bytes << bitsUsed) & intMask));
        int bits = bitsPerValue - bitsUsed;
        while (bits <= byteOffset) {
          values[valuesOffset++] = ((bytes >>> bits) & intMask);
          bits += bitsPerValue;
        }
        // then buffer
        nextValue = bytes >>> bits;
        bitsUsed = 8 - bits;
      }
    }
    assert bitsUsed == 0;
  }

  @Override
  public void encode(long[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsUsed = 0;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      if (bitsUsed < longOffset) {
        nextBlock |= values[valuesOffset++] << bitsUsed;
        bitsUsed += bitsPerValue;
      } else if (bitsUsed == longOffset) {
        nextBlock |=  values[valuesOffset++] << bitsUsed;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsUsed = 0;
      } else { // pos > longOffset
        nextBlock |= values[valuesOffset] << bitsUsed;
        blocks[blocksOffset++] = nextBlock;
        final int bits = 64 - bitsUsed;
        nextBlock = values[valuesOffset++] >>> bits;
        bitsUsed = bitsPerValue - bits;
      }
    }
    assert bitsUsed == 0;
  }

  @Override
  public void encode(int[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsUsed = 0;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      if (bitsUsed < longOffset) {
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL) << bitsUsed;
        bitsUsed += bitsPerValue;
      } else if (bitsUsed == longOffset) {
        nextBlock |=  (values[valuesOffset++] & 0xFFFFFFFFL) << bitsUsed;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsUsed = 0;
      } else { // pos > longOffset
        nextBlock |=  (values[valuesOffset] & 0xFFFFFFFFL) << bitsUsed;
        blocks[blocksOffset++] = nextBlock;
        final int bits = 64 - bitsUsed;
        nextBlock = values[valuesOffset++] >>> bits;
        bitsUsed = bitsPerValue - bits;
      }
    }
    assert bitsUsed == 0;
  }

  @Override
  public void encode(long[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int bitsUsed = 0;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final long v = values[valuesOffset++];
      assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
      if (bitsUsed < byteOffset) {
        // just buffer
        nextBlock |= v << bitsUsed;
        bitsUsed += bitsPerValue;
      } else {
        // flush as many blocks as possible
        blocks[blocksOffset++] = (byte) (nextBlock | (v << bitsUsed));
        int bits = 8 - bitsUsed;
        while (bits <= bitsUsedOffset) {
          blocks[blocksOffset++] = (byte) (v >> bits);
          bits += 8;
        }
        // then buffer
        bitsUsed = bitsPerValue - bits;
        nextBlock = (int)  ((v >>> bits) & ((1L << bitsUsed) - 1));
      }
    }
    assert bitsUsed == 0;
  }

  @Override
  public void encode(int[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int pos = 0;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final int v = values[valuesOffset++];
      assert PackedInts.unsignedBitsRequired(v & 0xFFFFFFFFL) <= bitsPerValue;
      if (pos < byteOffset) {
        // just buffer
        nextBlock |= v << pos;
        pos += bitsPerValue;
      } else {
        // flush as many blocks as possible
        blocks[blocksOffset++] = (byte) (nextBlock | (v << pos));
        int bits = 8 - pos;
        while (bits <= bitsUsedOffset) {
          blocks[blocksOffset++] = (byte) (v >>> bits);
          bits += 8;
        }
        // then buffer
        pos = bitsPerValue - bits;
        nextBlock = ((v >>> bits) & ((1 << pos) - 1));
      }
    }
    assert pos == 0;
  }
}
