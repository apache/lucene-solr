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

public class TestLongsRef extends LuceneTestCase {
  public void testEmpty() {
    LongsRef i = new LongsRef();
    assertEquals(LongsRef.EMPTY_LONGS, i.longs);
    assertEquals(0, i.offset);
    assertEquals(0, i.length);
  }
  
  public void testFromLongs() {
    long longs[] = new long[] { 1, 2, 3, 4 };
    LongsRef i = new LongsRef(longs, 0, 4);
    assertEquals(longs, i.longs);
    assertEquals(0, i.offset);
    assertEquals(4, i.length);
    
    LongsRef i2 = new LongsRef(longs, 1, 3);
    assertEquals(new LongsRef(new long[] { 2, 3, 4 }, 0, 3), i2);
    
    assertFalse(i.equals(i2));
  }
  
  public void testInvalidDeepCopy() {
    LongsRef from = new LongsRef(new long[] { 1, 2 }, 0, 2);
    from.offset += 1; // now invalid
    expectThrows(IndexOutOfBoundsException.class, () -> {
      LongsRef.deepCopyOf(from);
    });
  }
}
