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
package org.apache.solr.util;

import java.util.HashSet;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class LongSetTest extends SolrTestCase {

  @Test
  public void testZeroInitialCapacity() {
    final LongSet ls = new LongSet(0);
    assertEquals(0, ls.cardinality());
    assertEquals(2, ls.getBackingArray().length);
    assertFalse(ls.containsZero());
    assertFalse(ls.iterator().hasNext());

    final HashSet<Long> hs = new HashSet<>();
    for (long jj = 1; jj <= 10; ++jj) {
      assertTrue(ls.add(jj));
      assertFalse(ls.add(jj));
      assertEquals(jj, ls.cardinality());
      assertTrue(hs.add(jj));
      assertFalse(hs.add(jj));
    }

    final LongIterator it = ls.iterator();
    while (it.hasNext()) {
      hs.remove(it.next());
    }
    assertTrue(hs.isEmpty());

    assertEquals(10, ls.cardinality());
    assertEquals(16, ls.getBackingArray().length);
  }

  @Test
  public void testAddZero() {
    final LongSet ls = new LongSet(1);
    assertEquals(0, ls.cardinality());
    assertFalse(ls.containsZero());
    assertFalse(ls.iterator().hasNext());

    assertTrue(ls.add(0L));
    assertTrue(ls.containsZero());
    assertFalse(ls.add(0L));
    assertTrue(ls.containsZero());

    final LongIterator it = ls.iterator();
    assertTrue(it.hasNext());
    assertEquals(0L, it.next());
    assertFalse(it.hasNext());
  }

  @Test
  public void testIterating() {
    final LongSet ls = new LongSet(4);
    assertTrue(ls.add(0L));
    assertTrue(ls.add(6L));
    assertTrue(ls.add(7L));
    assertTrue(ls.add(42L));

    final LongIterator it = ls.iterator();
    // non-zero values are returned first
    assertTrue(it.hasNext());
    assertNotEquals(0L, it.next());
    assertTrue(it.hasNext());
    assertNotEquals(0L, it.next());
    assertTrue(it.hasNext());
    assertNotEquals(0L, it.next());

    // and zero value (if any) is returned last
    assertTrue(it.hasNext());
    assertEquals(0L, it.next());
    assertFalse(it.hasNext());
  }

}
