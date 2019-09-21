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
package org.apache.lucene.analysis;

import java.util.*;

import org.apache.lucene.util.LuceneTestCase;

public class TestCharArrayMap extends LuceneTestCase {
  public void doRandom(int iter, boolean ignoreCase) {
    CharArrayMap<Integer> map = new CharArrayMap<>(1, ignoreCase);
    HashMap<String,Integer> hmap = new HashMap<>();

    char[] key;
    for (int i=0; i<iter; i++) {
      int len = random().nextInt(5);
      key = new char[len];
      for (int j=0; j<key.length; j++) {
        key[j] = (char)random().nextInt(127);
      }
      String keyStr = new String(key);
      String hmapKey = ignoreCase ? keyStr.toLowerCase(Locale.ROOT) : keyStr; 

      int val = random().nextInt();

      Object o1 = map.put(key, val);
      Object o2 = hmap.put(hmapKey,val);
      assertEquals(o1,o2);

      // add it again with the string method
      assertEquals(val, map.put(keyStr,val).intValue());

      assertEquals(val, map.get(key,0,key.length).intValue());
      assertEquals(val, map.get(key).intValue());
      assertEquals(val, map.get(keyStr).intValue());

      assertEquals(hmap.size(), map.size());
    }
  }

  public void testCharArrayMap() {
    int num = 5 * RANDOM_MULTIPLIER;
    for (int i = 0; i < num; i++) { // pump this up for more random testing
      doRandom(1000,false);
      doRandom(1000,true);      
    }
  }

  public void testMethods() {
    CharArrayMap<Integer> cm = new CharArrayMap<>(2, false);
    HashMap<String,Integer> hm = new HashMap<>();
    hm.put("foo",1);
    hm.put("bar",2);
    cm.putAll(hm);
    assertEquals(hm.size(), cm.size());
    hm.put("baz", 3);
    cm.putAll(hm);
    assertEquals(hm.size(), cm.size());

    CharArraySet cs = cm.keySet();
    int n=0;
    for (Object o : cs) {
      assertTrue(cm.containsKey(o));
      char[] co = (char[]) o;
      assertTrue(cm.containsKey(co, 0, co.length));
      n++;
    }
    assertEquals(hm.size(), n);
    assertEquals(hm.size(), cs.size());
    assertEquals(cm.size(), cs.size());
    cs.clear();
    assertEquals(0, cs.size());
    assertEquals(0, cm.size());
    // keySet() should not allow adding new keys
    expectThrows(UnsupportedOperationException.class, () -> {
      cs.add("test");
    });

    cm.putAll(hm);
    assertEquals(hm.size(), cs.size());
    assertEquals(cm.size(), cs.size());

    Iterator<Map.Entry<Object,Integer>> iter1 = cm.entrySet().iterator();
    n=0;
    while (iter1.hasNext()) {
      Map.Entry<Object,Integer> entry = iter1.next();
      Object key = entry.getKey();
      Integer val = entry.getValue();
      assertEquals(cm.get(key), val);
      entry.setValue(val*100);
      assertEquals(val*100, (int)cm.get(key));
      n++;
    }
    assertEquals(hm.size(), n);
    cm.clear();
    cm.putAll(hm);
    assertEquals(cm.size(), n);

    CharArrayMap<Integer>.EntryIterator iter2 = cm.entrySet().iterator();
    n=0;
    while (iter2.hasNext()) {
      char[] keyc = iter2.nextKey();
      Integer val = iter2.currentValue();
      assertEquals(hm.get(new String(keyc)), val);
      iter2.setValue(val*100);
      assertEquals(val*100, (int)cm.get(keyc));
      n++;
    }
    assertEquals(hm.size(), n);

    cm.entrySet().clear();
    assertEquals(0, cm.size());
    assertEquals(0, cm.entrySet().size());
    assertTrue(cm.isEmpty());
  }

  // TODO: break this up into simpler test methods vs. "telling a story"
  public void testModifyOnUnmodifiable(){
    CharArrayMap<Integer> map = new CharArrayMap<>(2, false);
    map.put("foo",1);
    map.put("bar",2);
    final int size = map.size();
    assertEquals(2, size);
    assertTrue(map.containsKey("foo"));
    assertEquals(1, map.get("foo").intValue());
    assertTrue(map.containsKey("bar"));
    assertEquals(2, map.get("bar").intValue());

    CharArrayMap<Integer> unmodifiableMap = CharArrayMap.unmodifiableMap(map);
    assertEquals("Map size changed due to unmodifiableMap call" , size, unmodifiableMap.size());
    String NOT_IN_MAP = "SirGallahad";
    assertFalse("Test String already exists in map", unmodifiableMap.containsKey(NOT_IN_MAP));
    assertNull("Test String already exists in map", unmodifiableMap.get(NOT_IN_MAP));

    expectThrows(UnsupportedOperationException.class, () -> unmodifiableMap.put(NOT_IN_MAP.toCharArray(), 3));
    assertFalse("Test String has been added to unmodifiable map", unmodifiableMap.containsKey(NOT_IN_MAP));
    assertNull("Test String has been added to unmodifiable map", unmodifiableMap.get(NOT_IN_MAP));
    assertEquals("Size of unmodifiable map has changed", size, unmodifiableMap.size());

    expectThrows(UnsupportedOperationException.class, () -> unmodifiableMap.put(NOT_IN_MAP, 3));
    assertFalse("Test String has been added to unmodifiable map", unmodifiableMap.containsKey(NOT_IN_MAP));
    assertNull("Test String has been added to unmodifiable map", unmodifiableMap.get(NOT_IN_MAP));
    assertEquals("Size of unmodifiable map has changed", size, unmodifiableMap.size());

    expectThrows(UnsupportedOperationException.class, () -> unmodifiableMap.put(new StringBuilder(NOT_IN_MAP), 3));
    assertFalse("Test String has been added to unmodifiable map", unmodifiableMap.containsKey(NOT_IN_MAP));
    assertNull("Test String has been added to unmodifiable map", unmodifiableMap.get(NOT_IN_MAP));
    assertEquals("Size of unmodifiable map has changed", size, unmodifiableMap.size());

    expectThrows(UnsupportedOperationException.class,  unmodifiableMap::clear);
    assertEquals("Size of unmodifiable map has changed", size, unmodifiableMap.size());

    expectThrows(UnsupportedOperationException.class,  () -> unmodifiableMap.entrySet().clear());
    assertEquals("Size of unmodifiable map has changed", size, unmodifiableMap.size());

    expectThrows(UnsupportedOperationException.class,  () -> unmodifiableMap.keySet().clear());
    assertEquals("Size of unmodifiable map has changed", size, unmodifiableMap.size());

    expectThrows(UnsupportedOperationException.class, () -> unmodifiableMap.put((Object) NOT_IN_MAP, 3));
    assertFalse("Test String has been added to unmodifiable map", unmodifiableMap.containsKey(NOT_IN_MAP));
    assertNull("Test String has been added to unmodifiable map", unmodifiableMap.get(NOT_IN_MAP));
    assertEquals("Size of unmodifiable map has changed", size, unmodifiableMap.size());

    expectThrows(UnsupportedOperationException.class, () -> unmodifiableMap.putAll(Collections.singletonMap(NOT_IN_MAP, 3)));
    assertFalse("Test String has been added to unmodifiable map", unmodifiableMap.containsKey(NOT_IN_MAP));
    assertNull("Test String has been added to unmodifiable map", unmodifiableMap.get(NOT_IN_MAP));
    assertEquals("Size of unmodifiable map has changed", size, unmodifiableMap.size());

    assertTrue(unmodifiableMap.containsKey("foo"));
    assertEquals(1, unmodifiableMap.get("foo").intValue());
    assertTrue(unmodifiableMap.containsKey("bar"));
    assertEquals(2, unmodifiableMap.get("bar").intValue());
  }
  
  public void testToString() {
    CharArrayMap<Integer> cm = new CharArrayMap<>(Collections.singletonMap("test",1), false);
    assertEquals("[test]",cm.keySet().toString());
    assertEquals("[1]",cm.values().toString());
    assertEquals("[test=1]",cm.entrySet().toString());
    assertEquals("{test=1}",cm.toString());
    cm.put("test2", 2);
    assertTrue(cm.keySet().toString().contains(", "));
    assertTrue(cm.values().toString().contains(", "));
    assertTrue(cm.entrySet().toString().contains(", "));
    assertTrue(cm.toString().contains(", "));
  }
}

