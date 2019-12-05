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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

public class TestMathUtil extends LuceneTestCase {

  static long[] PRIMES = new long[] {2, 3, 5, 7, 11, 13, 17, 19, 23, 29};

  static long randomLong() {
    if (random().nextBoolean()) {
      long l = 1;
      if (random().nextBoolean()) {
        l *= -1;
      }
      for (long i : PRIMES) {
        final int m = random().nextInt(3);
        for (int j = 0; j < m; ++j) {
          l *= i;
        }
      }
      return l;
    } else if (random().nextBoolean()) {
      return random().nextLong();
    } else {
      return RandomPicks.randomFrom(random(), Arrays.asList(Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L));
    }
  }

  // slow version used for testing
  static long gcd(long l1, long l2) {
    final BigInteger gcd = BigInteger.valueOf(l1).gcd(BigInteger.valueOf(l2));
    assert gcd.bitCount() <= 64;
    return gcd.longValue();
  }

  public void testGCD() {
    final int iters = atLeast(100);
    for (int i = 0; i < iters; ++i) {
      final long l1 = randomLong();
      final long l2 = randomLong();
      final long gcd = MathUtil.gcd(l1, l2);
      final long actualGcd = gcd(l1, l2);
      assertEquals(actualGcd, gcd);
      if (gcd != 0) {
        assertEquals(l1, (l1 / gcd) * gcd);
        assertEquals(l2, (l2 / gcd) * gcd);
      }
    }
  }
  
  // ported test from commons-math
  public void testGCD2() {
    long a = 30;
    long b = 50;
    long c = 77;
    
    assertEquals(0, MathUtil.gcd(0, 0));
    assertEquals(b, MathUtil.gcd(0, b));
    assertEquals(a, MathUtil.gcd(a, 0));
    assertEquals(b, MathUtil.gcd(0, -b));
    assertEquals(a, MathUtil.gcd(-a, 0));
    
    assertEquals(10, MathUtil.gcd(a, b));
    assertEquals(10, MathUtil.gcd(-a, b));
    assertEquals(10, MathUtil.gcd(a, -b));
    assertEquals(10, MathUtil.gcd(-a, -b));
    
    assertEquals(1, MathUtil.gcd(a, c));
    assertEquals(1, MathUtil.gcd(-a, c));
    assertEquals(1, MathUtil.gcd(a, -c));
    assertEquals(1, MathUtil.gcd(-a, -c));
    
    assertEquals(3L * (1L<<45), MathUtil.gcd(3L * (1L<<50), 9L * (1L<<45)));
    assertEquals(1L<<45, MathUtil.gcd(1L<<45, Long.MIN_VALUE));
    
    assertEquals(Long.MAX_VALUE, MathUtil.gcd(Long.MAX_VALUE, 0L));
    assertEquals(Long.MAX_VALUE, MathUtil.gcd(-Long.MAX_VALUE, 0L));
    assertEquals(1, MathUtil.gcd(60247241209L, 153092023L));
    
    assertEquals(Long.MIN_VALUE, MathUtil.gcd(Long.MIN_VALUE, 0));
    assertEquals(Long.MIN_VALUE, MathUtil.gcd(0, Long.MIN_VALUE));
    assertEquals(Long.MIN_VALUE, MathUtil.gcd(Long.MIN_VALUE, Long.MIN_VALUE));
  }

  public void testAcoshMethod() {
    // acosh(NaN) == NaN
    assertTrue(Double.isNaN(MathUtil.acosh(Double.NaN)));
    // acosh(1) == +0
    assertEquals(0, Double.doubleToLongBits(MathUtil.acosh(1D)));
    // acosh(POSITIVE_INFINITY) == POSITIVE_INFINITY
    assertEquals(Double.doubleToLongBits(Double.POSITIVE_INFINITY),
        Double.doubleToLongBits(MathUtil.acosh(Double.POSITIVE_INFINITY)));
    // acosh(x) : x < 1 == NaN
    assertTrue(Double.isNaN(MathUtil.acosh(0.9D)));                      // x < 1
    assertTrue(Double.isNaN(MathUtil.acosh(0D)));                        // x == 0
    assertTrue(Double.isNaN(MathUtil.acosh(-0D)));                       // x == -0
    assertTrue(Double.isNaN(MathUtil.acosh(-0.9D)));                     // x < 0
    assertTrue(Double.isNaN(MathUtil.acosh(-1D)));                       // x == -1
    assertTrue(Double.isNaN(MathUtil.acosh(-10D)));                      // x < -1
    assertTrue(Double.isNaN(MathUtil.acosh(Double.NEGATIVE_INFINITY)));  // x == -Inf

    double epsilon = 0.000001;
    assertEquals(0, MathUtil.acosh(1), epsilon);
    assertEquals(1.5667992369724109, MathUtil.acosh(2.5), epsilon);
    assertEquals(14.719378760739708, MathUtil.acosh(1234567.89), epsilon);
  }

  public void testAsinhMethod() {

    // asinh(NaN) == NaN
    assertTrue(Double.isNaN(MathUtil.asinh(Double.NaN)));
    // asinh(+0) == +0
    assertEquals(0, Double.doubleToLongBits(MathUtil.asinh(0D)));
    // asinh(-0) == -0
    assertEquals(Double.doubleToLongBits(-0D), Double.doubleToLongBits(MathUtil.asinh(-0D)));
    // asinh(POSITIVE_INFINITY) == POSITIVE_INFINITY
    assertEquals(Double.doubleToLongBits(Double.POSITIVE_INFINITY),
        Double.doubleToLongBits(MathUtil.asinh(Double.POSITIVE_INFINITY)));
    // asinh(NEGATIVE_INFINITY) == NEGATIVE_INFINITY
    assertEquals(Double.doubleToLongBits(Double.NEGATIVE_INFINITY),
        Double.doubleToLongBits(MathUtil.asinh(Double.NEGATIVE_INFINITY)));

    double epsilon = 0.000001;
    assertEquals(-14.719378760740035, MathUtil.asinh(-1234567.89), epsilon);
    assertEquals(-1.6472311463710958, MathUtil.asinh(-2.5), epsilon);
    assertEquals(-0.8813735870195429, MathUtil.asinh(-1), epsilon);
    assertEquals(0, MathUtil.asinh(0), 0);
    assertEquals(0.8813735870195429, MathUtil.asinh(1), epsilon);
    assertEquals(1.6472311463710958, MathUtil.asinh(2.5), epsilon);
    assertEquals(14.719378760740035, MathUtil.asinh(1234567.89), epsilon  );
  }

  public void testAtanhMethod() {
    // atanh(NaN) == NaN
    assertTrue(Double.isNaN(MathUtil.atanh(Double.NaN)));
    // atanh(+0) == +0
    assertEquals(0, Double.doubleToLongBits(MathUtil.atanh(0D)));
    // atanh(-0) == -0
    assertEquals(Double.doubleToLongBits(-0D),
        Double.doubleToLongBits(MathUtil.atanh(-0D)));
    // atanh(1) == POSITIVE_INFINITY
    assertEquals(Double.doubleToLongBits(Double.POSITIVE_INFINITY),
        Double.doubleToLongBits(MathUtil.atanh(1D)));
    // atanh(-1) == NEGATIVE_INFINITY
    assertEquals(Double.doubleToLongBits(Double.NEGATIVE_INFINITY),
        Double.doubleToLongBits(MathUtil.atanh(-1D)));
    // atanh(x) : Math.abs(x) > 1 == NaN
    assertTrue(Double.isNaN(MathUtil.atanh(1.1D)));                      // x > 1
    assertTrue(Double.isNaN(MathUtil.atanh(Double.POSITIVE_INFINITY)));  // x == Inf
    assertTrue(Double.isNaN(MathUtil.atanh(-1.1D)));                     // x < -1
    assertTrue(Double.isNaN(MathUtil.atanh(Double.NEGATIVE_INFINITY)));  // x == -Inf

    double epsilon = 0.000001;
    assertEquals(Double.NEGATIVE_INFINITY, MathUtil.atanh(-1), 0);
    assertEquals(-0.5493061443340549, MathUtil.atanh(-0.5), epsilon);
    assertEquals(0, MathUtil.atanh(0), 0);
    assertEquals(0.5493061443340549, MathUtil.atanh(0.5), epsilon);
    assertEquals(Double.POSITIVE_INFINITY, MathUtil.atanh(1), 0);
  }

}
