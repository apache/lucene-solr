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

/** Test java 8-compatible implementations of {@code java.util.Objects} methods */
public class TestFutureObjects extends LuceneTestCase {

  public void testCheckIndex() {
    assertEquals(0, FutureObjects.checkIndex(0, 1));
    assertEquals(1, FutureObjects.checkIndex(1, 2));

    Exception e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkIndex(-1, 0);
    });
    assertEquals("Index -1 out-of-bounds for length 0", e.getMessage());
    
    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkIndex(0, 0);
    });
    assertEquals("Index 0 out-of-bounds for length 0", e.getMessage());
    
    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkIndex(1, 0);
    });
    assertEquals("Index 1 out-of-bounds for length 0", e.getMessage());
    
    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkIndex(0, -1);
    });
    assertEquals("Index 0 out-of-bounds for length -1", e.getMessage());
  }
  
  public void testCheckFromToIndex() {
    assertEquals(0, FutureObjects.checkFromToIndex(0, 0, 0));
    assertEquals(1, FutureObjects.checkFromToIndex(1, 2, 2));
    
    Exception e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkFromToIndex(-1, 0, 0);
    });
    assertEquals("Range [-1, 0) out-of-bounds for length 0", e.getMessage());

    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkFromToIndex(1, 0, 2);
    });
    assertEquals("Range [1, 0) out-of-bounds for length 2", e.getMessage());
    
    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkFromToIndex(1, 3, 2);
    });
    assertEquals("Range [1, 3) out-of-bounds for length 2", e.getMessage());
    
    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkFromToIndex(0, 0, -1);
    });
    assertEquals("Range [0, 0) out-of-bounds for length -1", e.getMessage());
  }
  
  public void testCheckFromIndexSize() {
    assertEquals(0, FutureObjects.checkFromIndexSize(0, 0, 0));
    assertEquals(1, FutureObjects.checkFromIndexSize(1, 2, 3));
    
    Exception e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkFromIndexSize(-1, 0, 1);
    });
    assertEquals("Range [-1, -1 + 0) out-of-bounds for length 1", e.getMessage());
    
    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkFromIndexSize(0, -1, 1);
    });
    assertEquals("Range [0, 0 + -1) out-of-bounds for length 1", e.getMessage());
    
    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkFromIndexSize(0, 2, 1);
    });
    assertEquals("Range [0, 0 + 2) out-of-bounds for length 1", e.getMessage());
    
    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkFromIndexSize(1, Integer.MAX_VALUE, Integer.MAX_VALUE);
    });
    assertEquals("Range [1, 1 + 2147483647) out-of-bounds for length 2147483647", e.getMessage());
    
    e = expectThrows(IndexOutOfBoundsException.class, () -> {
      FutureObjects.checkFromIndexSize(0, 0, -1);
    });
    assertEquals("Range [0, 0 + 0) out-of-bounds for length -1", e.getMessage());
  }

}
