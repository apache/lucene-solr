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

import org.apache.lucene.store.*;
import org.apache.lucene.util.LuceneTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.IOException;

public class TestPackedInts extends LuceneTestCase {
  public void testBitsRequired() throws Exception {
    assertEquals(61, PackedInts.bitsRequired((long)Math.pow(2, 61)-1));
    assertEquals(61, PackedInts.bitsRequired(0x1FFFFFFFFFFFFFFFL));
    assertEquals(62, PackedInts.bitsRequired(0x3FFFFFFFFFFFFFFFL));
    assertEquals(63, PackedInts.bitsRequired(0x7FFFFFFFFFFFFFFFL));
  }

  public void testMaxValues() throws Exception {
    assertEquals("1 bit -> max == 1",
            1, PackedInts.maxValue(1));
    assertEquals("2 bit -> max == 3",
            3, PackedInts.maxValue(2));
    assertEquals("8 bit -> max == 255",
            255, PackedInts.maxValue(8));
    assertEquals("63 bit -> max == Long.MAX_VALUE",
            Long.MAX_VALUE, PackedInts.maxValue(63));
    assertEquals("64 bit -> max == Long.MAX_VALUE (same as for 63 bit)", 
            Long.MAX_VALUE, PackedInts.maxValue(64));
  }

  public void testPackedInts() throws IOException {
    int num = atLeast(5);
    for (int iter = 0; iter < num; iter++) {
      long ceil = 2;
      for(int nbits=1;nbits<63;nbits++) {
        final int valueCount = 100+random().nextInt(500);
        final Directory d = newDirectory();

        IndexOutput out = d.createOutput("out.bin", newIOContext(random()));
        PackedInts.Writer w = PackedInts.getWriter(
                out, valueCount, nbits);

        final long[] values = new long[valueCount];
        for(int i=0;i<valueCount;i++) {
          long v = random().nextLong() % ceil;
          if (v < 0) {
            v = -v;
          }
          values[i] = v;
          w.add(values[i]);
        }
        w.finish();
        final long fp = out.getFilePointer();
        out.close();
        {// test reader
          IndexInput in = d.openInput("out.bin", newIOContext(random()));
          PackedInts.Reader r = PackedInts.getReader(in);
          assertEquals(fp, in.getFilePointer());
          for(int i=0;i<valueCount;i++) {
            assertEquals("index=" + i + " ceil=" + ceil + " valueCount="
                    + valueCount + " nbits=" + nbits + " for "
                    + r.getClass().getSimpleName(), values[i], r.get(i));
          }
          in.close();
        }
        { // test reader iterator next
          IndexInput in = d.openInput("out.bin", newIOContext(random()));
          PackedInts.ReaderIterator r = PackedInts.getReaderIterator(in);
          for(int i=0;i<valueCount;i++) {
            assertEquals("index=" + i + " ceil=" + ceil + " valueCount="
                    + valueCount + " nbits=" + nbits + " for "
                    + r.getClass().getSimpleName(), values[i], r.next());
          }
          assertEquals(fp, in.getFilePointer());
          in.close();
        }
        { // test reader iterator next vs. advance
          IndexInput in = d.openInput("out.bin", newIOContext(random()));
          PackedInts.ReaderIterator intsEnum = PackedInts.getReaderIterator(in);
          for (int i = 0; i < valueCount; i += 
            1 + ((valueCount - i) <= 20 ? random().nextInt(valueCount - i)
              : random().nextInt(20))) {
            final String msg = "index=" + i + " ceil=" + ceil + " valueCount="
                + valueCount + " nbits=" + nbits + " for "
                + intsEnum.getClass().getSimpleName();
            if (i - intsEnum.ord() == 1 && random().nextBoolean()) {
              assertEquals(msg, values[i], intsEnum.next());
            } else {
              assertEquals(msg, values[i], intsEnum.advance(i));
            }
            assertEquals(msg, i, intsEnum.ord());
          }
          if (intsEnum.ord() < valueCount - 1)
            assertEquals(values[valueCount - 1], intsEnum
                .advance(valueCount - 1));
          assertEquals(valueCount - 1, intsEnum.ord());
          assertEquals(fp, in.getFilePointer());
          in.close();
        }
        
        { // test direct reader get
          IndexInput in = d.openInput("out.bin", newIOContext(random()));
          PackedInts.Reader intsEnum = PackedInts.getDirectReader(in);
          for (int i = 0; i < valueCount; i++) {
            final String msg = "index=" + i + " ceil=" + ceil + " valueCount="
                + valueCount + " nbits=" + nbits + " for "
                + intsEnum.getClass().getSimpleName();
            final int index = random().nextInt(valueCount);
            long value = intsEnum.get(index);
            assertEquals(msg, value, values[index]);
          }
          in.close();
        }
        ceil *= 2;
        d.close();
      }
    }
  }
  
  public void testControlledEquality() {
    final int VALUE_COUNT = 255;
    final int BITS_PER_VALUE = 8;

    List<PackedInts.Mutable> packedInts =
            createPackedInts(VALUE_COUNT, BITS_PER_VALUE);
    for (PackedInts.Mutable packedInt: packedInts) {
      for (int i = 0 ; i < packedInt.size() ; i++) {
        packedInt.set(i, i+1);
      }
    }
    assertListEquality(packedInts);
  }

  public void testRandomEquality() {
    final int[] VALUE_COUNTS = new int[]{0, 1, 5, 8, 100, 500};
    final int MIN_BITS_PER_VALUE = 1;
    final int MAX_BITS_PER_VALUE = 64;

    for (int valueCount: VALUE_COUNTS) {
      for (int bitsPerValue = MIN_BITS_PER_VALUE ;
           bitsPerValue <= MAX_BITS_PER_VALUE ;
           bitsPerValue++) {
        assertRandomEquality(valueCount, bitsPerValue, random().nextLong());
      }
    }
  }

  private void assertRandomEquality(int valueCount, int bitsPerValue, long randomSeed) {
    List<PackedInts.Mutable> packedInts = createPackedInts(valueCount, bitsPerValue);
    for (PackedInts.Mutable packedInt: packedInts) {
      try {
        fill(packedInt, (long)(Math.pow(2, bitsPerValue)-1), randomSeed);
      } catch (Exception e) {
        e.printStackTrace(System.err);
        fail(String.format(
                "Exception while filling %s: valueCount=%d, bitsPerValue=%s",
                packedInt.getClass().getSimpleName(),
                valueCount, bitsPerValue));
      }
    }
    assertListEquality(packedInts);
  }

  private List<PackedInts.Mutable> createPackedInts(
          int valueCount, int bitsPerValue) {
    List<PackedInts.Mutable> packedInts = new ArrayList<PackedInts.Mutable>();
    if (bitsPerValue <= 8) {
      packedInts.add(new Direct8(valueCount));
    }
    if (bitsPerValue <= 16) {
      packedInts.add(new Direct16(valueCount));
    }
    if (bitsPerValue <= 31) {
      packedInts.add(new Packed32(valueCount, bitsPerValue));
    }
    if (bitsPerValue <= 32) {
      packedInts.add(new Direct32(valueCount));
    }
    if (bitsPerValue <= 63) {
      packedInts.add(new Packed64(valueCount, bitsPerValue));
    }
    packedInts.add(new Direct64(valueCount));
    return packedInts;
  }

  private void fill(PackedInts.Mutable packedInt, long maxValue, long randomSeed) {
    Random rnd2 = new Random(randomSeed);
    maxValue++;
    for (int i = 0 ; i < packedInt.size() ; i++) {
      long value = Math.abs(rnd2.nextLong() % maxValue);
      packedInt.set(i, value);
      assertEquals(String.format(
              "The set/get of the value at index %d should match for %s",
              i, packedInt.getClass().getSimpleName()),
              value, packedInt.get(i));
    }
  }

  private void assertListEquality(
          List<? extends PackedInts.Reader> packedInts) {
    assertListEquality("", packedInts);
  }

  private void assertListEquality(
            String message, List<? extends PackedInts.Reader> packedInts) {
    if (packedInts.size() == 0) {
      return;
    }
    PackedInts.Reader base = packedInts.get(0);
    int valueCount = base.size();
    for (PackedInts.Reader packedInt: packedInts) {
      assertEquals(message + ". The number of values should be the same ",
              valueCount, packedInt.size());
    }
    for (int i = 0 ; i < valueCount ; i++) {
      for (int j = 1 ; j < packedInts.size() ; j++) {
        assertEquals(String.format(
                "%s. The value at index %d should be the same for %s and %s",
                message, i, base.getClass().getSimpleName(),
                packedInts.get(j).getClass().getSimpleName()),
                base.get(i), packedInts.get(j).get(i));
      }
    }
  }

  public void testSingleValue() throws Exception {
    Directory dir = newDirectory();
    IndexOutput out = dir.createOutput("out", newIOContext(random()));
    PackedInts.Writer w = PackedInts.getWriter(out, 1, 8);
    w.add(17);
    w.finish();
    final long end = out.getFilePointer();
    out.close();

    IndexInput in = dir.openInput("out", newIOContext(random()));
    PackedInts.getReader(in);
    assertEquals(end, in.getFilePointer());
    in.close();

    dir.close();
  }

  public void testSecondaryBlockChange() throws IOException {
    PackedInts.Mutable mutable = new Packed64(26, 5);
    mutable.set(24, 31);
    assertEquals("The value #24 should be correct", 31, mutable.get(24));
    mutable.set(4, 16);
    assertEquals("The value #24 should remain unchanged", 31, mutable.get(24));
  }

  /*
    Check if the structures properly handle the case where
    index * bitsPerValue > Integer.MAX_VALUE
    
    NOTE: this test allocates 256 MB
   */
  public void testIntOverflow() {
    int INDEX = (int)Math.pow(2, 30)+1;
    int BITS = 2;

    Packed32 p32 = new Packed32(INDEX, BITS);
    p32.set(INDEX-1, 1);
    assertEquals("The value at position " + (INDEX-1)
        + " should be correct for Packed32", 1, p32.get(INDEX-1));
    p32 = null; // To free the 256MB used

    Packed64 p64 = new Packed64(INDEX, BITS);
    p64.set(INDEX-1, 1);
    assertEquals("The value at position " + (INDEX-1)
        + " should be correct for Packed64", 1, p64.get(INDEX-1));
  }
}
