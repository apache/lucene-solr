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

public class TestBytesRef extends LuceneTestCase {
  public void testEmpty() {
    BytesRef b = new BytesRef();
    assertEquals(BytesRef.EMPTY_BYTES, b.bytes);
    assertEquals(0, b.offset);
    assertEquals(0, b.length);
  }
  
  public void testFromBytes() {
    byte bytes[] = new byte[] { (byte)'a', (byte)'b', (byte)'c', (byte)'d' };
    BytesRef b = new BytesRef(bytes);
    assertEquals(bytes, b.bytes);
    assertEquals(0, b.offset);
    assertEquals(4, b.length);
    
    BytesRef b2 = new BytesRef(bytes, 1, 3);
    assertEquals("bcd", b2.utf8ToString());
    
    assertFalse(b.equals(b2));
  }
  
  public void testFromChars() {
    for (int i = 0; i < 100; i++) {
      String s = _TestUtil.randomUnicodeString(random());
      String s2 = new BytesRef(s).utf8ToString();
      assertEquals(s, s2);
    }
    
    // only for 4.x
    assertEquals("\uFFFF", new BytesRef("\uFFFF").utf8ToString());
  }
}
