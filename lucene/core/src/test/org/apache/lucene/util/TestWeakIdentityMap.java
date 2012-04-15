/**
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

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class TestWeakIdentityMap extends LuceneTestCase {

  public void testSimpleHashMap() {
    final WeakIdentityMap<String,String> map =
      WeakIdentityMap.newHashMap();
    // we keep strong references to the keys,
    // so WeakIdentityMap will not forget about them:
    String key1 = new String("foo");
    String key2 = new String("foo");
    String key3 = new String("foo");
    
    assertNotSame(key1, key2);
    assertEquals(key1, key2);
    assertNotSame(key1, key3);
    assertEquals(key1, key3);
    assertNotSame(key2, key3);
    assertEquals(key2, key3);

    map.put(key1, "bar1");
    map.put(key2, "bar2");
    map.put(null, "null");
    
    assertEquals(3, map.size());

    assertEquals("bar1", map.get(key1));
    assertEquals("bar2", map.get(key2));
    assertEquals(null, map.get(key3));
    assertEquals("null", map.get(null));
    
    assertTrue(map.containsKey(key1));
    assertTrue(map.containsKey(key2));
    assertFalse(map.containsKey(key3));
    assertTrue(map.containsKey(null));

    // repeat and check that we have no double entries
    map.put(key1, "bar1");
    map.put(key2, "bar2");
    map.put(null, "null");

    assertEquals(3, map.size());
    
    assertEquals("bar1", map.get(key1));
    assertEquals("bar2", map.get(key2));
    assertEquals(null, map.get(key3));
    assertEquals("null", map.get(null));
    
    assertTrue(map.containsKey(key1));
    assertTrue(map.containsKey(key2));
    assertFalse(map.containsKey(key3));
    assertTrue(map.containsKey(null));

    map.remove(null);
    assertEquals(2, map.size());
    map.remove(key1);
    assertEquals(1, map.size());
    map.put(key1, "bar1");
    map.put(key2, "bar2");
    map.put(key3, "bar3");
    assertEquals(3, map.size());
    
    // clear strong refs
    key1 = key2 = key3 = null;
    
    // check that GC does not cause problems in reap() method, wait 1 second and let GC work:
    int size = map.size();
    for (int i = 0; size > 0 && i < 10; i++) try {
      System.runFinalization();
      System.gc();
      Thread.sleep(100L);
      assertTrue(size >= map.size());
      size = map.size();
    } catch (InterruptedException ie) {}

    map.clear();
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
    
    key1 = new String("foo");
    key2 = new String("foo");
    map.put(key1, "bar1");
    map.put(key2, "bar2");
    assertEquals(2, map.size());
    
    map.clear();
    assertEquals(0, map.size());
    assertTrue(map.isEmpty());
  }

  public void testConcurrentHashMap() throws Exception {
    // don't make threadCount and keyCount random, otherwise easily OOMs or fails otherwise:
    final int threadCount = 8, keyCount = 1024;
    final ExecutorService exec = Executors.newFixedThreadPool(threadCount);
    final WeakIdentityMap<Object,Integer> map =
      WeakIdentityMap.newConcurrentHashMap();
    // we keep strong references to the keys,
    // so WeakIdentityMap will not forget about them:
    final AtomicReferenceArray<Object> keys = new AtomicReferenceArray<Object>(keyCount);
    for (int j = 0; j < keyCount; j++) {
      keys.set(j, new Object());
    }
    
    try {
      for (int t = 0; t < threadCount; t++) {
        final Random rnd = new Random(random().nextLong());
        exec.execute(new Runnable() {
          public void run() {
            final int count = atLeast(rnd, 10000);
            for (int i = 0; i < count; i++) {
              final int j = rnd.nextInt(keyCount);
              switch (rnd.nextInt(4)) {
                case 0:
                  map.put(keys.get(j), Integer.valueOf(j));
                  break;
                case 1:
                  final Integer v = map.get(keys.get(j));
                  if (v != null) {
                    assertEquals(j, v.intValue());
                  }
                  break;
                case 2:
                  map.remove(keys.get(j));
                  break;
                case 3:
                  // renew key, the old one will be GCed at some time:
                  keys.set(j, new Object());
                  break;
                default:
                  fail("Should not get here.");
              }
            }
          }
        });
      }
    } finally {
      exec.shutdown();
      while (!exec.awaitTermination(1000L, TimeUnit.MILLISECONDS));
    }
    
    // clear strong refs
    for (int j = 0; j < keyCount; j++) {
      keys.set(j, null);
    }
    
    // check that GC does not cause problems in reap() method:
    int size = map.size();
    for (int i = 0; size > 0 && i < 10; i++) try {
      System.runFinalization();
      System.gc();
      Thread.sleep(100L);
      assertTrue(size >= map.size());
      size = map.size();
    } catch (InterruptedException ie) {}
  }

}
