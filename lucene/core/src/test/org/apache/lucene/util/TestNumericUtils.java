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
package org.apache.lucene.util;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Tests for NumericUtils static methods.
 */
public class TestNumericUtils extends LuceneTestCase {

  /**
   * generate a series of encoded longs, each numerical one bigger than the one before.
   * check for correct ordering of the encoded bytes and that values round-trip.
   */
  public void testLongConversionAndOrdering() throws Exception {
    BytesRef previous = null;
    BytesRef current = new BytesRef(new byte[Long.BYTES]);
    for (long value = -100000L; value < 100000L; value++) {
      NumericUtils.longToSortableBytes(value, current.bytes, current.offset);
      if (previous == null) {
        previous = new BytesRef(new byte[Long.BYTES]);
      } else {
        // test if smaller
        assertTrue("current bigger than previous: ", previous.compareTo(current) < 0);
      }
      // test is back and forward conversion works
      assertEquals("forward and back conversion should generate same long", value, NumericUtils.sortableBytesToLong(current.bytes, current.offset));
      // next step
      System.arraycopy(current.bytes, current.offset, previous.bytes, previous.offset, current.length);
    }
  }

  /**
   * generate a series of encoded ints, each numerical one bigger than the one before.
   * check for correct ordering of the encoded bytes and that values round-trip.
   */
  public void testIntConversionAndOrdering() throws Exception {
    BytesRef previous = null;
    BytesRef current = new BytesRef(new byte[Integer.BYTES]);
    for (int value = -100000; value < 100000; value++) {
      NumericUtils.intToSortableBytes(value, current.bytes, current.offset);
      if (previous == null) {
        previous = new BytesRef(new byte[Integer.BYTES]);
      } else {
        // test if smaller
        assertTrue("current bigger than previous: ", previous.compareTo(current) < 0);
      }
      // test is back and forward conversion works
      assertEquals("forward and back conversion should generate same int", value, NumericUtils.sortableBytesToInt(current.bytes, current.offset));
      // next step
      System.arraycopy(current.bytes, current.offset, previous.bytes, previous.offset, current.length);
    }
  }
  
  /**
   * generate a series of encoded BigIntegers, each numerical one bigger than the one before.
   * check for correct ordering of the encoded bytes and that values round-trip.
   */
  public void testBigIntConversionAndOrdering() throws Exception {
    // we need at least 3 bytes of storage.
    int size = TestUtil.nextInt(random(), 3, 16);
    BytesRef previous = null;
    BytesRef current = new BytesRef(new byte[size]);
    for (long value = -100000L; value < 100000L; value++) {
      NumericUtils.bigIntToSortableBytes(BigInteger.valueOf(value), size, current.bytes, current.offset);
      if (previous == null) {
        previous = new BytesRef(new byte[size]);
      } else {
        // test if smaller
        assertTrue("current bigger than previous: ", previous.compareTo(current) < 0);
      }
      // test is back and forward conversion works
      assertEquals("forward and back conversion should generate same BigInteger", 
                   BigInteger.valueOf(value), 
                   NumericUtils.sortableBytesToBigInt(current.bytes, current.offset, current.length));
      // next step
      System.arraycopy(current.bytes, current.offset, previous.bytes, previous.offset, current.length);
    }
  }

  /**
   * check extreme values of longs 
   * check for correct ordering of the encoded bytes and that values round-trip.
   */
  public void testLongSpecialValues() throws Exception {
    long[] values = new long[] {
      Long.MIN_VALUE, Long.MIN_VALUE+1, Long.MIN_VALUE+2, -5003400000000L,
      -4000L, -3000L, -2000L, -1000L, -1L, 0L, 1L, 10L, 300L, 50006789999999999L, Long.MAX_VALUE-2, Long.MAX_VALUE-1, Long.MAX_VALUE
    };
    BytesRef[] encoded = new BytesRef[values.length];
    
    for (int i = 0; i < values.length; i++) {
      encoded[i] = new BytesRef(new byte[Long.BYTES]);
      NumericUtils.longToSortableBytes(values[i], encoded[i].bytes, encoded[i].offset);
      
      // check forward and back conversion
      assertEquals("forward and back conversion should generate same long", 
                   values[i], 
                   NumericUtils.sortableBytesToLong(encoded[i].bytes, encoded[i].offset));
    }
    
    // check sort order (encoded values should be ascending)
    for (int i = 1; i < encoded.length; i++) {
      assertTrue("check sort order", encoded[i-1].compareTo(encoded[i]) < 0);
    }
  }

  /**
   * check extreme values of ints
   * check for correct ordering of the encoded bytes and that values round-trip.
   */
  public void testIntSpecialValues() throws Exception {
    int[] values = new int[] {
      Integer.MIN_VALUE, Integer.MIN_VALUE+1, Integer.MIN_VALUE+2, -64765767,
      -4000, -3000, -2000, -1000, -1, 0, 1, 10, 300, 765878989, Integer.MAX_VALUE-2, Integer.MAX_VALUE-1, Integer.MAX_VALUE
    };
    BytesRef[] encoded = new BytesRef[values.length];
    
    for (int i = 0; i < values.length; i++) {
      encoded[i] = new BytesRef(new byte[Integer.BYTES]);
      NumericUtils.intToSortableBytes(values[i], encoded[i].bytes, encoded[i].offset);
      
      // check forward and back conversion
      assertEquals("forward and back conversion should generate same int", 
                   values[i], 
                   NumericUtils.sortableBytesToInt(encoded[i].bytes, encoded[i].offset));
    }
    
    // check sort order (encoded values should be ascending)
    for (int i = 1; i < encoded.length; i++) {
      assertTrue("check sort order", encoded[i-1].compareTo(encoded[i]) < 0);
    }
  }
  
  /**
   * check extreme values of big integers (4 bytes)
   * check for correct ordering of the encoded bytes and that values round-trip.
   */
  public void testBigIntSpecialValues() throws Exception {
    BigInteger[] values = new BigInteger[] {
      BigInteger.valueOf(Integer.MIN_VALUE), BigInteger.valueOf(Integer.MIN_VALUE+1), 
      BigInteger.valueOf(Integer.MIN_VALUE+2), BigInteger.valueOf(-64765767),
      BigInteger.valueOf(-4000), BigInteger.valueOf(-3000), BigInteger.valueOf(-2000), 
      BigInteger.valueOf(-1000), BigInteger.valueOf(-1), BigInteger.valueOf(0), 
      BigInteger.valueOf(1), BigInteger.valueOf(10), BigInteger.valueOf(300), 
      BigInteger.valueOf(765878989), BigInteger.valueOf(Integer.MAX_VALUE-2), 
      BigInteger.valueOf(Integer.MAX_VALUE-1), BigInteger.valueOf(Integer.MAX_VALUE)
    };
    BytesRef[] encoded = new BytesRef[values.length];
    
    for (int i = 0; i < values.length; i++) {
      encoded[i] = new BytesRef(new byte[Integer.BYTES]);
      NumericUtils.bigIntToSortableBytes(values[i], Integer.BYTES, encoded[i].bytes, encoded[i].offset);
      
      // check forward and back conversion
      assertEquals("forward and back conversion should generate same big integer", 
                   values[i], 
                   NumericUtils.sortableBytesToBigInt(encoded[i].bytes, encoded[i].offset, Integer.BYTES));
    }
    
    // check sort order (encoded values should be ascending)
    for (int i = 1; i < encoded.length; i++) {
      assertTrue("check sort order", encoded[i-1].compareTo(encoded[i]) < 0);
    }
  }

  /**
   * check various sorted values of doubles (including extreme values)
   * check for correct ordering of the encoded bytes and that values round-trip.
   */
  public void testDoubles() throws Exception {
    double[] values = new double[] {
      Double.NEGATIVE_INFINITY, -2.3E25, -1.0E15, -1.0, -1.0E-1, -1.0E-2, -0.0, 
      +0.0, 1.0E-2, 1.0E-1, 1.0, 1.0E15, 2.3E25, Double.POSITIVE_INFINITY, Double.NaN
    };
    long[] encoded = new long[values.length];
    
    // check forward and back conversion
    for (int i = 0; i < values.length; i++) {
      encoded[i] = NumericUtils.doubleToSortableLong(values[i]);
      assertTrue("forward and back conversion should generate same double", 
                 Double.compare(values[i], NumericUtils.sortableLongToDouble(encoded[i])) == 0);
    }
    
    // check sort order (encoded values should be ascending)
    for (int i = 1; i < encoded.length; i++) {
      assertTrue("check sort order", encoded[i-1] < encoded[i]);
    }
  }

  public static final double[] DOUBLE_NANs = {
    Double.NaN,
    Double.longBitsToDouble(0x7ff0000000000001L),
    Double.longBitsToDouble(0x7fffffffffffffffL),
    Double.longBitsToDouble(0xfff0000000000001L),
    Double.longBitsToDouble(0xffffffffffffffffL)
  };

  public void testSortableDoubleNaN() {
    final long plusInf = NumericUtils.doubleToSortableLong(Double.POSITIVE_INFINITY);
    for (double nan : DOUBLE_NANs) {
      assertTrue(Double.isNaN(nan));
      final long sortable = NumericUtils.doubleToSortableLong(nan);
      assertTrue("Double not sorted correctly: " + nan + ", long repr: " 
          + sortable + ", positive inf.: " + plusInf, sortable > plusInf);
    }
  }
  
  /**
   * check various sorted values of floats (including extreme values)
   * check for correct ordering of the encoded bytes and that values round-trip.
   */
  public void testFloats() throws Exception {
    float[] values = new float[] {
      Float.NEGATIVE_INFINITY, -2.3E25f, -1.0E15f, -1.0f, -1.0E-1f, -1.0E-2f, -0.0f, 
      +0.0f, 1.0E-2f, 1.0E-1f, 1.0f, 1.0E15f, 2.3E25f, Float.POSITIVE_INFINITY, Float.NaN
    };
    int[] encoded = new int[values.length];
    
    // check forward and back conversion
    for (int i = 0; i < values.length; i++) {
      encoded[i] = NumericUtils.floatToSortableInt(values[i]);
      assertTrue("forward and back conversion should generate same float", 
                 Float.compare(values[i], NumericUtils.sortableIntToFloat(encoded[i])) == 0);
    }
    
    // check sort order (encoded values should be ascending)
    for (int i = 1; i < encoded.length; i++) {
      assertTrue( "check sort order", encoded[i-1] < encoded[i] );
    }
  }

  public static final float[] FLOAT_NANs = {
    Float.NaN,
    Float.intBitsToFloat(0x7f800001),
    Float.intBitsToFloat(0x7fffffff),
    Float.intBitsToFloat(0xff800001),
    Float.intBitsToFloat(0xffffffff)
  };

  public void testSortableFloatNaN() {
    final int plusInf = NumericUtils.floatToSortableInt(Float.POSITIVE_INFINITY);
    for (float nan : FLOAT_NANs) {
      assertTrue(Float.isNaN(nan));
      final int sortable = NumericUtils.floatToSortableInt(nan);
      assertTrue("Float not sorted correctly: " + nan + ", int repr: " 
          + sortable + ", positive inf.: " + plusInf, sortable > plusInf);
    }
  }
  
  public void testAdd() throws Exception {
    int iters = atLeast(1000);
    int numBytes = TestUtil.nextInt(random(), 1, 100);
    for(int iter=0;iter<iters;iter++) {
      BigInteger v1 = new BigInteger(8*numBytes-1, random());
      BigInteger v2 = new BigInteger(8*numBytes-1, random());

      byte[] v1Bytes = new byte[numBytes];
      byte[] v1RawBytes = v1.toByteArray();
      assert v1RawBytes.length <= numBytes;
      System.arraycopy(v1RawBytes, 0, v1Bytes, v1Bytes.length-v1RawBytes.length, v1RawBytes.length);

      byte[] v2Bytes = new byte[numBytes];
      byte[] v2RawBytes = v2.toByteArray();
      assert v1RawBytes.length <= numBytes;
      System.arraycopy(v2RawBytes, 0, v2Bytes, v2Bytes.length-v2RawBytes.length, v2RawBytes.length);

      byte[] result = new byte[numBytes];
      NumericUtils.add(numBytes, 0, v1Bytes, v2Bytes, result);

      BigInteger sum = v1.add(v2);
      assertTrue("sum=" + sum + " v1=" + v1 + " v2=" + v2 + " but result=" + new BigInteger(1, result), sum.equals(new BigInteger(1, result)));
    }
  }

  public void testIllegalAdd() throws Exception {
    byte[] bytes = new byte[4];
    Arrays.fill(bytes, (byte) 0xff);
    byte[] one = new byte[4];
    one[3] = 1;
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      NumericUtils.add(4, 0, bytes, one, new byte[4]);
    });
    assertEquals("a + b overflows bytesPerDim=4", expected.getMessage());
  }
  
  public void testSubtract() throws Exception {
    int iters = atLeast(1000);
    int numBytes = TestUtil.nextInt(random(), 1, 100);
    for(int iter=0;iter<iters;iter++) {
      BigInteger v1 = new BigInteger(8*numBytes-1, random());
      BigInteger v2 = new BigInteger(8*numBytes-1, random());
      if (v1.compareTo(v2) < 0) {
        BigInteger tmp = v1;
        v1 = v2;
        v2 = tmp;
      }

      byte[] v1Bytes = new byte[numBytes];
      byte[] v1RawBytes = v1.toByteArray();
      assert v1RawBytes.length <= numBytes: "length=" + v1RawBytes.length + " vs numBytes=" + numBytes;
      System.arraycopy(v1RawBytes, 0, v1Bytes, v1Bytes.length-v1RawBytes.length, v1RawBytes.length);

      byte[] v2Bytes = new byte[numBytes];
      byte[] v2RawBytes = v2.toByteArray();
      assert v2RawBytes.length <= numBytes;
      assert v2RawBytes.length <= numBytes: "length=" + v2RawBytes.length + " vs numBytes=" + numBytes;
      System.arraycopy(v2RawBytes, 0, v2Bytes, v2Bytes.length-v2RawBytes.length, v2RawBytes.length);

      byte[] result = new byte[numBytes];
      NumericUtils.subtract(numBytes, 0, v1Bytes, v2Bytes, result);

      BigInteger diff = v1.subtract(v2);

      assertTrue("diff=" + diff + " vs result=" + new BigInteger(result) + " v1=" + v1 + " v2=" + v2, diff.equals(new BigInteger(result)));
    }
  }

  public void testIllegalSubtract() throws Exception {
    byte[] v1 = new byte[4];
    v1[3] = (byte) 0xf0;
    byte[] v2 = new byte[4];
    v2[3] = (byte) 0xf1;
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      NumericUtils.subtract(4, 0, v1, v2, new byte[4]);
    });
    assertEquals("a < b", expected.getMessage());
  }
  
  /** test round-trip encoding of random integers */
  public void testIntsRoundTrip() {
    byte[] encoded = new byte[Integer.BYTES];

    for (int i = 0; i < 10000; i++) {
      int value = random().nextInt();
      NumericUtils.intToSortableBytes(value, encoded, 0);
      assertEquals(value, NumericUtils.sortableBytesToInt(encoded, 0));
    }
  }
  
  /** test round-trip encoding of random longs */
  public void testLongsRoundTrip() {
    byte[] encoded = new byte[Long.BYTES];

    for (int i = 0; i < 10000; i++) {
      long value = TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE);
      NumericUtils.longToSortableBytes(value, encoded, 0);
      assertEquals(value, NumericUtils.sortableBytesToLong(encoded, 0));
    }
  }
  
  /** test round-trip encoding of random floats */
  public void testFloatsRoundTrip() {
    byte[] encoded = new byte[Float.BYTES];

    for (int i = 0; i < 10000; i++) {
      float value = Float.intBitsToFloat(random().nextInt());
      NumericUtils.intToSortableBytes(NumericUtils.floatToSortableInt(value), encoded, 0);
      float actual = NumericUtils.sortableIntToFloat(NumericUtils.sortableBytesToInt(encoded, 0));
      assertEquals(Float.floatToIntBits(value), Float.floatToIntBits(actual));
    }
  }
  
  /** test round-trip encoding of random doubles */
  public void testDoublesRoundTrip() {
    byte[] encoded = new byte[Double.BYTES];

    for (int i = 0; i < 10000; i++) {
      double value = Double.longBitsToDouble(TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE));
      NumericUtils.longToSortableBytes(NumericUtils.doubleToSortableLong(value), encoded, 0);
      double actual = NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(encoded, 0));
      assertEquals(Double.doubleToLongBits(value), Double.doubleToLongBits(actual));
    }
  }
  
  /** test round-trip encoding of random big integers */
  public void testBigIntsRoundTrip() {
    for (int i = 0; i < 10000; i++) {
      BigInteger value = TestUtil.nextBigInteger(random(), 16);
      int length = value.toByteArray().length;

      // make sure sign extension is tested: sometimes pad to more bytes when encoding.
      int maxLength = TestUtil.nextInt(random(), length, length + 3);
      byte[] encoded = new byte[maxLength];
      NumericUtils.bigIntToSortableBytes(value, maxLength, encoded, 0);
      assertEquals(value, NumericUtils.sortableBytesToBigInt(encoded, 0, maxLength));
    }
  }
  
  /** check sort order of random integers consistent with Integer.compare */
  public void testIntsCompare() {
    BytesRef left = new BytesRef(new byte[Integer.BYTES]);
    BytesRef right = new BytesRef(new byte[Integer.BYTES]);

    for (int i = 0; i < 10000; i++) {
      int leftValue = random().nextInt();
      NumericUtils.intToSortableBytes(leftValue, left.bytes, left.offset);

      int rightValue = random().nextInt();
      NumericUtils.intToSortableBytes(rightValue, right.bytes, right.offset);
      
      assertEquals(Integer.signum(Integer.compare(leftValue, rightValue)),
                   Integer.signum(left.compareTo(right)));
    }
  }
  
  /** check sort order of random longs consistent with Long.compare */
  public void testLongsCompare() {
    BytesRef left = new BytesRef(new byte[Long.BYTES]);
    BytesRef right = new BytesRef(new byte[Long.BYTES]);

    for (int i = 0; i < 10000; i++) {
      long leftValue = TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE);
      NumericUtils.longToSortableBytes(leftValue, left.bytes, left.offset);

      long rightValue = TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE);
      NumericUtils.longToSortableBytes(rightValue, right.bytes, right.offset);
      
      assertEquals(Integer.signum(Long.compare(leftValue, rightValue)),
                   Integer.signum(left.compareTo(right)));
    }
  }
  
  /** check sort order of random floats consistent with Float.compare */
  public void testFloatsCompare() {
    BytesRef left = new BytesRef(new byte[Float.BYTES]);
    BytesRef right = new BytesRef(new byte[Float.BYTES]);

    for (int i = 0; i < 10000; i++) {
      float leftValue = Float.intBitsToFloat(random().nextInt());
      NumericUtils.intToSortableBytes(NumericUtils.floatToSortableInt(leftValue), left.bytes, left.offset);

      float rightValue = Float.intBitsToFloat(random().nextInt());
      NumericUtils.intToSortableBytes(NumericUtils.floatToSortableInt(rightValue), right.bytes, right.offset);
      
      assertEquals(Integer.signum(Float.compare(leftValue, rightValue)),
                   Integer.signum(left.compareTo(right)));
    }
  }
  
  /** check sort order of random doubles consistent with Double.compare */
  public void testDoublesCompare() {
    BytesRef left = new BytesRef(new byte[Double.BYTES]);
    BytesRef right = new BytesRef(new byte[Double.BYTES]);

    for (int i = 0; i < 10000; i++) {
      double leftValue = Double.longBitsToDouble(TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE));
      NumericUtils.longToSortableBytes(NumericUtils.doubleToSortableLong(leftValue), left.bytes, left.offset);

      double rightValue = Double.longBitsToDouble(TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE));
      NumericUtils.longToSortableBytes(NumericUtils.doubleToSortableLong(rightValue), right.bytes, right.offset);
      
      assertEquals(Integer.signum(Double.compare(leftValue, rightValue)),
                   Integer.signum(left.compareTo(right)));
    }
  }
  
  /** check sort order of random bigintegers consistent with BigInteger.compareTo */
  public void testBigIntsCompare() {
    for (int i = 0; i < 10000; i++) {
      int maxLength = TestUtil.nextInt(random(), 1, 16);
      
      BigInteger leftValue = TestUtil.nextBigInteger(random(), maxLength);
      BytesRef left = new BytesRef(new byte[maxLength]);
      NumericUtils.bigIntToSortableBytes(leftValue, maxLength, left.bytes, left.offset);
      
      BigInteger rightValue = TestUtil.nextBigInteger(random(), maxLength);
      BytesRef right = new BytesRef(new byte[maxLength]);
      NumericUtils.bigIntToSortableBytes(rightValue, maxLength, right.bytes, right.offset);
      
      assertEquals(Integer.signum(leftValue.compareTo(rightValue)),
                   Integer.signum(left.compareTo(right)));
    }
  }
}
