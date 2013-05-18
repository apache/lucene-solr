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

package org.apache.solr.common.util;

import org.apache.lucene.util.LuceneTestCase;

public class NamedListTest extends LuceneTestCase {
  public void testRemove() {
    NamedList<String> nl = new NamedList<String>();
    nl.add("key1", "value1");
    nl.add("key2", "value2");
    assertEquals(2, nl.size());
    String value = nl.remove(0);
    assertEquals("value1", value);
    assertEquals(1, nl.size());
  }
  public void testRecursive() {
    // key1
    // key2
    // - key2a
    // - key2b
    // --- key2b1
    // --- key2b2
    // - key2c
    // key3
    // - key3a
    // --- key3a1
    // --- key3a2
    // --- key3a3
    // - key3b
    // - key3c
    NamedList<Object> nl2b = new NamedList<Object>();
    nl2b.add("key2b1", "value2b1");
    nl2b.add("key2b2", "value2b2");
    NamedList<Object> nl3a = new NamedList<Object>();
    nl3a.add("key3a1", "value3a1");
    nl3a.add("key3a2", "value3a2");
    nl3a.add("key3a3", "value3a3");
    NamedList<Object> nl2 = new NamedList<Object>();
    nl2.add("key2a", "value2a");
    nl2.add("key2b", nl2b);
    int int1 = 5;
    Integer int2 = 7;
    int int3 = 48;
    nl2.add("k2int1", int1);
    nl2.add("k2int2", int2);
    nl2.add("k2int3", int3);
    NamedList<Object> nl3 = new NamedList<Object>();
    nl3.add("key3a", nl3a);
    nl3.add("key3b", "value3b");
    nl3.add("key3c", "value3c");
    NamedList<Object> nl = new NamedList<Object>();
    nl.add("key1", "value1");
    nl.add("key2", nl2);
    nl.add("key3", nl3);

    String test1 = (String) nl.findRecursive("key2", "key2b", "key2b2");
    assertEquals(test1, "value2b2");
    String test2 = (String) nl.findRecursive("key3", "key3a", "key3a3");
    assertEquals(test2, "value3a3");
    String test3 = (String) nl.findRecursive("key3", "key3c");
    assertEquals(test3, "value3c");
    String test4 = (String) nl.findRecursive("key3", "key3c", "invalid");
    assertEquals(test4, null);
    String test5 = (String) nl.findRecursive("key3", "invalid", "invalid");
    assertEquals(test5, null);
    String test6 = (String) nl.findRecursive("invalid", "key3c");
    assertEquals(test6, null);
    Object nltest = nl.findRecursive("key2", "key2b");
    assertTrue(nltest instanceof NamedList);
    Integer int1test = (Integer) nl.findRecursive("key2", "k2int1");
    assertEquals(int1test, (Integer) 5);
    int int2test = (int) nl.findRecursive("key2", "k2int2");
    assertEquals(int2test, 7);
    int int3test = (int) nl.findRecursive("key2", "k2int3");
    assertEquals(int3test, 48);
  }
}
