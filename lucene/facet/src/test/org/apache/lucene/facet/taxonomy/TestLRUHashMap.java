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
package org.apache.lucene.facet.taxonomy;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.LRUHashMap;
import org.junit.Test;

public class TestLRUHashMap extends FacetTestCase {
  // testLRU() tests that the specified size limit is indeed honored, and
  // the remaining objects in the map are indeed those that have been most
  // recently used
  @Test
  public void testLRU() throws Exception {
    LRUHashMap<String, String> lru = new LRUHashMap<>(3);
    assertEquals(0, lru.size());
    lru.put("one", "Hello world");
    assertEquals(1, lru.size());
    lru.put("two", "Hi man");
    assertEquals(2, lru.size());
    lru.put("three", "Bonjour");
    assertEquals(3, lru.size());
    lru.put("four", "Shalom");
    assertEquals(3, lru.size());
    assertNotNull(lru.get("three"));
    assertNotNull(lru.get("two"));
    assertNotNull(lru.get("four"));
    assertNull(lru.get("one"));
    lru.put("five", "Yo!");
    assertEquals(3, lru.size());
    assertNull(lru.get("three")); // three was last used, so it got removed
    assertNotNull(lru.get("five"));
    lru.get("four");
    lru.put("six", "hi");
    lru.put("seven", "hey dude");
    assertEquals(3, lru.size());
    assertNull(lru.get("one"));
    assertNull(lru.get("two"));
    assertNull(lru.get("three"));
    assertNotNull(lru.get("four"));
    assertNull(lru.get("five"));
    assertNotNull(lru.get("six"));
    assertNotNull(lru.get("seven"));
  }
}
