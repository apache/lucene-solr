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


public class TestIntsRef extends LuceneTestCase {
  public void testEmpty() {
    IntsRef i = new IntsRef();
    assertEquals(IntsRef.EMPTY_INTS, i.ints);
    assertEquals(0, i.offset);
    assertEquals(0, i.length);
  }
  
  public void testFromInts() {
    int ints[] = new int[] { 1, 2, 3, 4 };
    IntsRef i = new IntsRef(ints, 0, 4);
    assertEquals(ints, i.ints);
    assertEquals(0, i.offset);
    assertEquals(4, i.length);
    
    IntsRef i2 = new IntsRef(ints, 1, 3);
    assertEquals(new IntsRef(new int[] { 2, 3, 4 }, 0, 3), i2);
    
    assertFalse(i.equals(i2));
  }
  
  public void testInvalidDeepCopy() {
    IntsRef from = new IntsRef(new int[] { 1, 2 }, 0, 2);
    from.offset += 1; // now invalid
    expectThrows(IndexOutOfBoundsException.class, () -> {
      IntsRef.deepCopyOf(from);
    });
  }
}
