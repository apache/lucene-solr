package org.apache.lucene.util;

import java.util.Arrays;

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

public class TestCharsRef extends LuceneTestCase {
  public void testUTF16InUTF8Order() {
    final int numStrings = atLeast(1000);
    BytesRef utf8[] = new BytesRef[numStrings];
    CharsRef utf16[] = new CharsRef[numStrings];
    
    for (int i = 0; i < numStrings; i++) {
      String s = _TestUtil.randomUnicodeString(random());
      utf8[i] = new BytesRef(s);
      utf16[i] = new CharsRef(s);
    }
    
    Arrays.sort(utf8);
    Arrays.sort(utf16, CharsRef.getUTF16SortedAsUTF8Comparator());
    
    for (int i = 0; i < numStrings; i++) {
      assertEquals(utf8[i].utf8ToString(), utf16[i].toString());
    }
  }
  
  public void testAppend() {
    CharsRef ref = new CharsRef();
    StringBuilder builder = new StringBuilder();
    int numStrings = atLeast(10);
    for (int i = 0; i < numStrings; i++) {
      char[] charArray = _TestUtil.randomRealisticUnicodeString(random(), 1, 100).toCharArray();
      int offset = random().nextInt(charArray.length);
      int length = charArray.length - offset;
      builder.append(charArray, offset, length);
      ref.append(charArray, offset, length);  
    }
    
    assertEquals(builder.toString(), ref.toString());
  }
  
  public void testCopy() {
    int numIters = atLeast(10);
    for (int i = 0; i < numIters; i++) {
      CharsRef ref = new CharsRef();
      char[] charArray = _TestUtil.randomRealisticUnicodeString(random(), 1, 100).toCharArray();
      int offset = random().nextInt(charArray.length);
      int length = charArray.length - offset;
      String str = new String(charArray, offset, length);
      ref.copyChars(charArray, offset, length);
      assertEquals(str, ref.toString());  
    }
    
  }
  
  // LUCENE-3590, AIOOBE if you append to a charsref with offset != 0
  public void testAppendChars() {
    char chars[] = new char[] { 'a', 'b', 'c', 'd' };
    CharsRef c = new CharsRef(chars, 1, 3); // bcd
    c.append(new char[] { 'e' }, 0, 1);
    assertEquals("bcde", c.toString());
  }
  
  // LUCENE-3590, AIOOBE if you copy to a charsref with offset != 0
  public void testCopyChars() {
    char chars[] = new char[] { 'a', 'b', 'c', 'd' };
    CharsRef c = new CharsRef(chars, 1, 3); // bcd
    char otherchars[] = new char[] { 'b', 'c', 'd', 'e' };
    c.copyChars(otherchars, 0, 4);
    assertEquals("bcde", c.toString());
  }
  
  // LUCENE-3590, AIOOBE if you copy to a charsref with offset != 0
  public void testCopyCharsRef() {
    char chars[] = new char[] { 'a', 'b', 'c', 'd' };
    CharsRef c = new CharsRef(chars, 1, 3); // bcd
    char otherchars[] = new char[] { 'b', 'c', 'd', 'e' };
    c.copyChars(new CharsRef(otherchars, 0, 4));
    assertEquals("bcde", c.toString());
  }
  
  // LUCENE-3590: fix charsequence to fully obey interface
  public void testCharSequenceCharAt() {
    CharsRef c = new CharsRef("abc");
    
    assertEquals('b', c.charAt(1));
    
    try {
      c.charAt(-1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      // expected exception
    }
    
    try {
      c.charAt(3);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      // expected exception
    }
  }
  
  // LUCENE-3590: fix off-by-one in subsequence, and fully obey interface
  // LUCENE-4671: fix subSequence
  public void testCharSequenceSubSequence() {
    CharSequence sequences[] =  {
        new CharsRef("abc"),
        new CharsRef("0abc".toCharArray(), 1, 3),
        new CharsRef("abc0".toCharArray(), 0, 3),
        new CharsRef("0abc0".toCharArray(), 1, 3)
    };
    
    for (CharSequence c : sequences) {
      doTestSequence(c);
    }
  }
    
  private void doTestSequence(CharSequence c) {
    
    // slice
    assertEquals("a", c.subSequence(0, 1).toString());
    // mid subsequence
    assertEquals("b", c.subSequence(1, 2).toString());
    // end subsequence
    assertEquals("bc", c.subSequence(1, 3).toString());
    // empty subsequence
    assertEquals("", c.subSequence(0, 0).toString());
    
    try {
      c.subSequence(-1, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      // expected exception
    }
    
    try {
      c.subSequence(0, -1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      // expected exception
    }
    
    try {
      c.subSequence(0, 4);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      // expected exception
    }
    
    try {
      c.subSequence(2, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      // expected exception
    }
  }
}
