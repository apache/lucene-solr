package org.apache.lucene.codecs.blockpacked;
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

import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.IntBuffer;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Reader;
import org.apache.lucene.util.packed.PackedInts.Writer;
import org.apache.lucene.util.packed.PackedInts.Mutable;
import org.apache.lucene.util.packed.PackedInts.Encoder;
import org.apache.lucene.util.packed.PackedInts.Decoder;

/**
 * Encode all values in normal area with fixed bit width, 
 * which is determined by the max value in this block.
 */
public class ForUtil {
  protected static final int[] MASK = {   0x00000000,
    0x00000001, 0x00000003, 0x00000007, 0x0000000f, 0x0000001f, 0x0000003f,
    0x0000007f, 0x000000ff, 0x000001ff, 0x000003ff, 0x000007ff, 0x00000fff,
    0x00001fff, 0x00003fff, 0x00007fff, 0x0000ffff, 0x0001ffff, 0x0003ffff,
    0x0007ffff, 0x000fffff, 0x001fffff, 0x003fffff, 0x007fffff, 0x00ffffff,
    0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff, 0x1fffffff, 0x3fffffff,
    0x7fffffff, 0xffffffff};

  /** Compress given int[] into output stream, with For format
   */
  public static int compress(final LongBuffer data, LongBuffer packed) throws IOException {
    int numBits=getNumBits(data.array());

    if (numBits == 0) { // when block is equal, save the value once
      packed.put(0, data.get(0)<<32); // java uses big endian for LongBuffer impl 
      return (getHeader(1,numBits));
    }

    PackedInts.Format format = PackedInts.fastestFormatAndBits(128, numBits, PackedInts.FASTEST).format;
    PackedInts.Encoder encoder = PackedInts.getEncoder(format, PackedInts.VERSION_CURRENT, numBits);
    int perIter = encoder.values();
    int iters = 128/perIter;
    int nblocks = encoder.blocks()*iters;
    assert 128 % perIter == 0;

    packed.rewind();
    data.rewind();

    encoder.encode(data, packed, iters);

    int encodedSize = nblocks*2;
    return getHeader(encodedSize,numBits);
  }

  /** Decompress given ouput stream into int array.
   */
  public static void decompress(LongBuffer data, LongBuffer packed, int header) throws IOException {
    // nocommit assert header isn't "malformed", ie besides
    // numBytes / bit-width there is nothing else!
    
    packed.rewind();
    data.rewind();
    int numBits = ((header >> 8) & MASK[6]);

    if (numBits == 0) {
      Arrays.fill(data.array(), (int)(packed.get(0)>>>32));
      return;
    }

    PackedInts.Format format = PackedInts.fastestFormatAndBits(128, numBits, PackedInts.FASTEST).format;
    PackedInts.Decoder decoder = PackedInts.getDecoder(format, PackedInts.VERSION_CURRENT, numBits);
    int perIter = decoder.values();
    int iters = 128/perIter;
    int nblocks = decoder.blocks()*iters;
    assert 128 % perIter == 0;

    decoder.decode(packed, data, iters);
  }

  static int getNumBits(final long[] data) {
    if (isAllEqual(data)) {
      return 0;
    }
    int size=data.length;
    int optBits=1;
    for (int i=0; i<size; ++i) {
      while ((data[i] & ~MASK[optBits]) != 0) {
        optBits++;
      }
    }
    return optBits;
  }

  protected static boolean isAllEqual(final long[] data) {
    int len = data.length;
    long v = data[0];
    for (int i=1; i<len; i++) {
      if (data[i] != v) {
        return false;
      }
    }
    return true;
  }
  static int getHeader(int encodedSize, int numBits) {
    return  (encodedSize)
          | ((numBits) << 8);
  }
  public static int getEncodedSize(int header) {
    return ((header & MASK[8]))*4;
  }
  public static int getNumBits(int header) {
    return ((header >> 8) & MASK[6]);
  }
}
