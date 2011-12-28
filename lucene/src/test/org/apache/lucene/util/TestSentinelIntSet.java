package org.apache.lucene.util;


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

import org.junit.Test;

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
}
