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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCase;

public class DynamicMapsTest extends SolrTestCase {

  public void testIntFloatMap() {
    IntFloatDynamicMap map = new IntFloatDynamicMap(10, -1.1f);
    Map<Integer, Float> standard = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      int key = random().nextInt(100) + 1;
      float val = random().nextFloat();
      standard.put(key, val);
      map.put(key, val);
    }

    for (Map.Entry<Integer, Float> entry: standard.entrySet()) {
      assertEquals(entry.getValue(), map.get(entry.getKey()), 0.0001);
    }
    AtomicInteger size = new AtomicInteger(0);
    map.forEachValue(i -> size.incrementAndGet());
    assertEquals(standard.size(), size.get());

    assertEquals(-1.1f, map.get(101), 0.0001);
    assertEquals(-1.1f, map.get(0), 0.0001);
  }

  public void testIntLongMap() {
    IntLongDynamicMap map = new IntLongDynamicMap(10, -1);
    Map<Integer, Long> standard = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      int key = random().nextInt(100) + 1;
      long val = random().nextLong();
      standard.put(key, val);
      map.put(key, val);
    }

    for (Map.Entry<Integer, Long> entry: standard.entrySet()) {
      assertEquals((long)entry.getValue(), map.get(entry.getKey()));
    }
    AtomicInteger size = new AtomicInteger(0);
    map.forEachValue(i -> size.incrementAndGet());
    assertEquals(standard.size(), size.get());

    assertEquals(-1, map.get(101));
    assertEquals(-1, map.get(0));
  }

  public void testIntIntMap() {
    IntIntDynamicMap map = new IntIntDynamicMap(10, -1);
    Map<Integer, Integer> standard = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      int key = random().nextInt(100) + 1;
      int val = random().nextInt();
      standard.put(key, val);
      map.put(key, val);
    }

    for (Map.Entry<Integer, Integer> entry: standard.entrySet()) {
      assertEquals((int)entry.getValue(), map.get(entry.getKey()));
    }
    AtomicInteger size = new AtomicInteger(0);
    map.forEachValue(i -> size.incrementAndGet());
    assertEquals(standard.size(), size.get());

    assertEquals(-1, map.get(101));
    assertEquals(-1, map.get(0));
  }
}
