package org.apache.lucene.facet.collections;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.collections.IntArray;
import org.junit.Test;

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

public class IntArrayTest extends FacetTestCase {
  
  @Test
  public void test0() {
    IntArray array = new IntArray();
    
    assertEquals(0, array.size());
    
    for (int i = 0; i < 100; ++i) {
      array.addToArray(i);
    }
    
    assertEquals(100, array.size());
    for (int i = 0; i < 100; ++i) {
      assertEquals(i, array.get(i));
    }
    
    assertTrue(array.equals(array));
  }
  
  @Test
  public void test1() {
    IntArray array = new IntArray();
    IntArray array2 = new IntArray();
    
    assertEquals(0, array.size());
    
    for (int i = 0; i < 100; ++i) {
      array.addToArray(99-i);
      array2.addToArray(99-i);
    }
    
    assertEquals(100, array.size());
    for (int i = 0; i < 100; ++i) {
      assertEquals(i, array.get(99-i));
    }
    
    array.sort();
    for (int i = 0; i < 100; ++i) {
      assertEquals(i, array.get(i));
    }

    assertTrue(array.equals(array2));
  }
  
  @Test
  public void test2() {
    IntArray array = new IntArray();
    IntArray array2 = new IntArray();
    IntArray array3 = new IntArray();
    
    for (int i = 0; i < 100; ++i) {
      array.addToArray(i);
    }

    for (int i = 0; i < 100; ++i) {
      array2.addToArray(i*2);
    }

    for (int i = 0; i < 50; ++i) {
      array3.addToArray(i*2);
    }

    assertFalse(array.equals(array2));
    
    array.intersect(array2);
    assertTrue(array.equals(array3));
    assertFalse(array.equals(array2));
  }
  
  @Test
  public void testSet() {
    int[] original = new int[] { 2,4,6,8,10,12,14 };
    int[] toSet = new int[] { 1,3,5,7,9,11};
    
    IntArray arr = new IntArray();
    for (int val : original) {
      arr.addToArray(val);
    }
    
    for (int i = 0; i < toSet.length; i++ ) {
      int val = toSet[i];
      arr.set(i, val);
    }
    
    // Test to see if the set worked correctly
    for (int i = 0; i < toSet.length; i++ ) {
      assertEquals(toSet[i], arr.get(i));
    }
    
    // Now attempt to set something outside of the array
    try {
      arr.set(100, 99);
      fail("IntArray.set should have thrown an exception for attempting to set outside the array");
    } catch (ArrayIndexOutOfBoundsException e) {
      // We expected this to happen so let it fall through
      // silently
    }
    
  }

}
