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
package org.apache.lucene.index;

import org.apache.lucene.util.LuceneTestCase;

public class TestApproximatePriorityQueue extends LuceneTestCase {

  public void testBasics() {
    ApproximatePriorityQueue<Long> pq = new ApproximatePriorityQueue<>();
    pq.add(8L, 8L);
    pq.add(32L, 32L);
    pq.add(0L, 0L);
    assertFalse(pq.isEmpty());
    assertEquals(Long.valueOf(32L), pq.poll(x -> true));
    assertFalse(pq.isEmpty());
    assertEquals(Long.valueOf(8L), pq.poll(x -> true));
    assertFalse(pq.isEmpty());
    assertEquals(Long.valueOf(0L), pq.poll(x -> true));
    assertTrue(pq.isEmpty());
    assertNull(pq.poll(x -> true));
  }

  public void testCollision() {
    ApproximatePriorityQueue<Long> pq = new ApproximatePriorityQueue<>();
    pq.add(2L, 2L);
    pq.add(1L, 1L);
    pq.add(0L, 0L);
    pq.add(3L, 3L); // Same nlz as 2
    assertFalse(pq.isEmpty());
    assertEquals(Long.valueOf(2L), pq.poll(x -> true));
    assertFalse(pq.isEmpty());
    assertEquals(Long.valueOf(1L), pq.poll(x -> true));
    assertFalse(pq.isEmpty());
    assertEquals(Long.valueOf(3L), pq.poll(x -> true));
    assertFalse(pq.isEmpty());
    assertEquals(Long.valueOf(0L), pq.poll(x -> true));
    assertTrue(pq.isEmpty());
    assertNull(pq.poll(x -> true));
  }

  public void testPollWithPredicate() {
    ApproximatePriorityQueue<Long> pq = new ApproximatePriorityQueue<>();
    pq.add(8L, 8L);
    pq.add(32L, 32L);
    pq.add(0L, 0L);
    assertEquals(Long.valueOf(8L), pq.poll(x -> x == 8));
    assertNull(pq.poll(x -> x == 8));
    assertFalse(pq.isEmpty());
  }

  public void testCollisionPollWithPredicate() {
    ApproximatePriorityQueue<Long> pq = new ApproximatePriorityQueue<>();
    pq.add(2L, 2L);
    pq.add(1L, 1L);
    pq.add(0L, 0L);
    pq.add(3L, 3L); // Same nlz as 2
    assertEquals(Long.valueOf(1L), pq.poll(x -> x % 2 == 1));
    assertEquals(Long.valueOf(3L), pq.poll(x -> x % 2 == 1));
    assertNull(pq.poll(x -> x % 2 == 1));
    assertFalse(pq.isEmpty());
  }

}
