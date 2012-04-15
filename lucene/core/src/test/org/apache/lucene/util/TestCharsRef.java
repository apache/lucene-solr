package org.apache.lucene.util;

import java.util.Arrays;

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
}
