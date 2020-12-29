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

import java.util.Arrays;

/*
 * Some of this code came from the excellent Unicode
 * conversion examples from:
 *
 *   http://www.unicode.org/Public/PROGRAMS/CVTUTF
 *
 * Full Copyright for that code follows:
 */

/*
 * Copyright 2001-2004 Unicode, Inc.
 *
 * Disclaimer
 *
 * This source code is provided as is by Unicode, Inc. No claims are
 * made as to fitness for any particular purpose. No warranties of any
 * kind are expressed or implied. The recipient agrees to determine
 * applicability of information provided. If this file has been
 * purchased on magnetic or optical media from Unicode, Inc., the
 * sole remedy for any claim will be exchange of defective media
 * within 90 days of receipt.
 *
 * Limitations on Rights to Redistribute This Code
 *
 * Unicode, Inc. hereby grants the right to freely use the information
 * supplied in this file in the creation of products supporting the
 * Unicode Standard, and to make copies of this file in any form
 * for internal or external distribution as long as this notice
 * remains attached.
 */

/*
 * Additional code came from the IBM ICU library.
 *
 *  http://www.icu-project.org
 *
 * Full Copyright for that code follows.
 */

/*
 * Copyright (C) 1999-2010, International Business Machines
 * Corporation and others.  All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * provided that the above copyright notice(s) and this permission notice appear
 * in all copies of the Software and that both the above copyright notice(s) and
 * this permission notice appear in supporting documentation.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE BE
 * LIABLE FOR ANY CLAIM, OR ANY SPECIAL INDIRECT OR CONSEQUENTIAL DAMAGES, OR
 * ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER
 * IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Except as contained in this notice, the name of a copyright holder shall not
 * be used in advertising or otherwise to promote the sale, use or other
 * dealings in this Software without prior written authorization of the
 * copyright holder.
 */

public class TestUnicodeUtil extends LuceneTestCase {
  public void testCodePointCount() {
    // Check invalid codepoints.
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0x80, 'z', 'z', 'z'));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xc0 - 1, 'z', 'z', 'z'));
    // Check 5-byte and longer sequences.
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xf8, 'z', 'z', 'z'));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xfc, 'z', 'z', 'z'));
    // Check improperly terminated codepoints.
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xc2));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xe2));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xe2, 0x82));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xf0));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xf0, 0xa4));
    assertcodePointCountThrowsAssertionOn(asByteArray('z', 0xf0, 0xa4, 0xad));

    // Check some typical examples (multibyte).
    assertEquals(0, UnicodeUtil.codePointCount(new BytesRef(asByteArray())));
    assertEquals(3, UnicodeUtil.codePointCount(new BytesRef(asByteArray('z', 'z', 'z'))));
    assertEquals(2, UnicodeUtil.codePointCount(new BytesRef(asByteArray('z', 0xc2, 0xa2))));
    assertEquals(2, UnicodeUtil.codePointCount(new BytesRef(asByteArray('z', 0xe2, 0x82, 0xac))));
    assertEquals(
        2, UnicodeUtil.codePointCount(new BytesRef(asByteArray('z', 0xf0, 0xa4, 0xad, 0xa2))));

    // And do some random stuff.
    int num = atLeast(50000);
    for (int i = 0; i < num; i++) {
      final String s = TestUtil.randomUnicodeString(random());
      final byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(s.length())];
      final int utf8Len = UnicodeUtil.UTF16toUTF8(s, 0, s.length(), utf8);
      assertEquals(
          s.codePointCount(0, s.length()),
          UnicodeUtil.codePointCount(new BytesRef(utf8, 0, utf8Len)));
    }
  }

  private byte[] asByteArray(int... ints) {
    byte[] asByteArray = new byte[ints.length];
    for (int i = 0; i < ints.length; i++) {
      asByteArray[i] = (byte) ints[i];
    }
    return asByteArray;
  }

  private void assertcodePointCountThrowsAssertionOn(byte... bytes) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          UnicodeUtil.codePointCount(new BytesRef(bytes));
        });
  }

  public void testUTF8toUTF32() {
    int[] utf32 = new int[0];
    int num = atLeast(50000);
    for (int i = 0; i < num; i++) {
      final String s = TestUtil.randomUnicodeString(random());
      final byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(s.length())];
      final int utf8Len = UnicodeUtil.UTF16toUTF8(s, 0, s.length(), utf8);
      utf32 = ArrayUtil.grow(utf32, utf8Len);
      final int utf32Len = UnicodeUtil.UTF8toUTF32(new BytesRef(utf8, 0, utf8Len), utf32);

      int[] codePoints = s.codePoints().toArray();
      if (!Arrays.equals(codePoints, 0, codePoints.length, utf32, 0, codePoints.length)) {
        System.out.println("FAILED");
        for (int j = 0; j < s.length(); j++) {
          System.out.println("  char[" + j + "]=" + Integer.toHexString(s.charAt(j)));
        }
        System.out.println();
        assertEquals(codePoints.length, utf32Len);
        for (int j = 0; j < codePoints.length; j++) {
          System.out.println(
              "  " + Integer.toHexString(utf32[j]) + " vs " + Integer.toHexString(codePoints[j]));
        }
        fail("mismatch");
      }
    }
  }

  public void testNewString() {
    final int[] codePoints = {
      Character.toCodePoint(Character.MIN_HIGH_SURROGATE, Character.MAX_LOW_SURROGATE),
      Character.toCodePoint(Character.MAX_HIGH_SURROGATE, Character.MIN_LOW_SURROGATE),
      Character.MAX_HIGH_SURROGATE,
      'A',
      -1,
    };

    final String cpString =
        ""
            + Character.MIN_HIGH_SURROGATE
            + Character.MAX_LOW_SURROGATE
            + Character.MAX_HIGH_SURROGATE
            + Character.MIN_LOW_SURROGATE
            + Character.MAX_HIGH_SURROGATE
            + 'A';

    final int[][] tests = {
      {0, 1, 0, 2},
      {0, 2, 0, 4},
      {1, 1, 2, 2},
      {1, 2, 2, 3},
      {1, 3, 2, 4},
      {2, 2, 4, 2},
      {2, 3, 0, -1},
      {4, 5, 0, -1},
      {3, -1, 0, -1}
    };

    for (int i = 0; i < tests.length; ++i) {
      int[] t = tests[i];
      int s = t[0];
      int c = t[1];
      int rs = t[2];
      int rc = t[3];

      try {
        String str = UnicodeUtil.newString(codePoints, s, c);
        assertFalse(rc == -1);
        assertEquals(cpString.substring(rs, rs + rc), str);
        continue;
      } catch (IndexOutOfBoundsException | IllegalArgumentException e1) {
        // Ignored.
      }
      assertTrue(rc == -1);
    }
  }

  public void testUTF8UTF16CharsRef() {
    int num = atLeast(3989);
    for (int i = 0; i < num; i++) {
      String unicode = TestUtil.randomRealisticUnicodeString(random());
      BytesRef ref = new BytesRef(unicode);
      CharsRefBuilder cRef = new CharsRefBuilder();
      cRef.copyUTF8Bytes(ref);
      assertEquals(cRef.toString(), unicode);
    }
  }

  public void testCalcUTF16toUTF8Length() {
    int num = atLeast(5000);
    for (int i = 0; i < num; i++) {
      String unicode = TestUtil.randomUnicodeString(random());
      byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(unicode.length())];
      int len = UnicodeUtil.UTF16toUTF8(unicode, 0, unicode.length(), utf8);
      assertEquals(len, UnicodeUtil.calcUTF16toUTF8Length(unicode, 0, unicode.length()));
    }
  }
}
