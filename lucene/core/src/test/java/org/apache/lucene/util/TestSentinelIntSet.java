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



import org.junit.Test;

import java.util.HashSet;

/**
 *
 *
 **/
public class TestSentinelIntSet extends LuceneTestCase {

  @Test
  public void test() throws Exception {
    SentinelIntSet set = new SentinelIntSet(10, -1);
    assertFalse(set.exists(50));
    set.put(50);
    assertTrue(set.exists(50));
    assertEquals(1, set.size());
    assertEquals(-11, set.find(10));
    assertEquals(1, set.size());
    set.clear();
    assertEquals(0, set.size());
    assertEquals(50, set.hash(50));
    //force a rehash
    for (int i = 0; i < 20; i++){
      set.put(i);
    }
    assertEquals(20, set.size());
    assertEquals(24, set.rehashCount);
  }
  

  @Test
  public void testRandom() throws Exception {
    for (int i=0; i<10000; i++) {
      int initSz = random().nextInt(20);
      int num = random().nextInt(30);
      int maxVal = (random().nextBoolean() ? random().nextInt(50) : random().nextInt(Integer.MAX_VALUE)) + 1;

      HashSet<Integer> a = new HashSet<>(initSz);
      SentinelIntSet b = new SentinelIntSet(initSz, -1);
      
      for (int j=0; j<num; j++) {
        int val = random().nextInt(maxVal);
        boolean exists = !a.add(val);
        boolean existsB = b.exists(val);
        assertEquals(exists, existsB);
        int slot = b.find(val);
        assertEquals(exists, slot>=0);
        b.put(val);
        
        assertEquals(a.size(), b.size());
      }
      
    }

  }
  
}
