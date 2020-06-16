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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.junit.Test;

public class NamedListTest extends SolrTestCase {

  public void testRemove() {
    NamedList<String> nl = new NamedList<>();
    nl.add("key1", "value1");
    nl.add("key2", "value2");
    assertEquals(2, nl.size());
    String value = null;
    value = nl.remove(0);
    assertEquals("value1", value);
    assertEquals(1, nl.size());
    value = nl.remove("key2");
    assertEquals("value2", value);
    assertEquals(0, nl.size());
  }
  
  public void testRemoveAll() {
    NamedList<String> nl = new NamedList<>();
    nl.add("key1", "value1-1");
    nl.add("key2", "value2-1");
    nl.add("key1", "value1-2");
    nl.add("key2", "value2-2");
    nl.add("key1", "value1-3");
    nl.add("key2", "value2-3");
    nl.add("key1", "value1-4");
    nl.add("key2", "value2-4");
    nl.add("key1", "value1-5");
    nl.add("key2", "value2-5");
    nl.add("key1", "value1-6");
    assertEquals(11, nl.size());
    List<String> values = null;
    values = nl.removeAll("key1");
    assertEquals("value1-1", values.get(0));
    assertEquals("value1-3", values.get(2));
    assertEquals(6, values.size());
    assertEquals(5, nl.size());
    values = nl.removeAll("key2");
    assertEquals(5, values.size());
    assertEquals(0, nl.size());
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testRemoveArgs() {
    NamedList<Object> nl = new NamedList<>();
    nl.add("key1", "value1-1");
    nl.add("key2", "value2-1");
    nl.add("key1", "value1-2");
    nl.add("key2", "value2-2");
    nl.add("key1", "value1-3");
    nl.add("key2", "value2-3");
    nl.add("key1", "value1-4");
    nl.add("key2", "value2-4");
    nl.add("key1", "value1-5");
    nl.add("key2", "value2-5");
    nl.add("key1", "value1-6");
    nl.add("key2", 0);
    nl.add("key2", "value2-7");
    assertEquals(13, nl.size());
    List<String> values = (ArrayList<String>) nl.removeConfigArgs("key1");
    assertEquals("value1-1", values.get(0));
    assertEquals("value1-3", values.get(2));
    assertEquals(6, values.size());
    assertEquals(7, nl.size());

    expectThrows(SolrException.class, () -> nl.removeConfigArgs("key2"));
    // nl should be unmodified when removeArgs throws an exception.
    assertEquals(7, nl.size());
  }
  
  public void testRecursive() {
    // key1
    // key2
    // - key2a
    // - key2b
    // --- key2b1
    // --- key2b2
    // - key2c
    // - k2int1
    // key3
    // - key3a
    // --- key3a1
    // --- key3a2
    // --- key3a3
    // - key3b
    // - key3c
    
    // this is a varied NL structure.
    NamedList<String> nl2b = new NamedList<>();
    nl2b.add("key2b1", "value2b1");
    nl2b.add("key2b2", "value2b2");
    NamedList<String> nl3a = new NamedList<>();
    nl3a.add("key3a1", "value3a1");
    nl3a.add("key3a2", "value3a2");
    nl3a.add("key3a3", "value3a3");
    NamedList<Object> nl2 = new NamedList<>();
    nl2.add("key2a", "value2a");
    nl2.add("key2b", nl2b);
    nl2.add("k2int1", 5);
    NamedList<Object> nl3 = new NamedList<>();
    nl3.add("key3a", nl3a);
    nl3.add("key3b", "value3b");
    nl3.add("key3c", "value3c");
    nl3.add("key3c", "value3c2");
    NamedList<Object> nl = new NamedList<>();
    nl.add("key1", "value1");
    nl.add("key2", nl2);
    nl.add("key3", nl3);
    
    // Simple three-level checks.
    String test1 = (String) nl.findRecursive("key2", "key2b", "key2b2");
    assertEquals("value2b2", test1);
    String test2 = (String) nl.findRecursive("key3", "key3a", "key3a3");
    assertEquals("value3a3", test2);
    // Two-level check.
    String test3 = (String) nl.findRecursive("key3", "key3c");
    assertEquals("value3c", test3);
    // Checking that invalid values return null.
    String test4 = (String) nl.findRecursive("key3", "key3c", "invalid");
    assertEquals(null, test4);
    String test5 = (String) nl.findRecursive("key3", "invalid", "invalid");
    assertEquals(null, test5);
    String test6 = (String) nl.findRecursive("invalid", "key3c");
    assertEquals(null, test6);
    // Verify that retrieved NamedList objects have the right type.
    Object test7 = nl.findRecursive("key2", "key2b");
    assertTrue(test7 instanceof NamedList);
    // Integer check.
    int test8 = (Integer) nl.findRecursive("key2", "k2int1");
    assertEquals(5, test8);
    // Check that a single argument works the same as get(String).
    String test9 = (String) nl.findRecursive("key1");
    assertEquals("value1", test9);
    // enl == explicit nested list
    //
    // key1
    // - key1a
    // - key1b
    // key2 (null list)
    NamedList<NamedList<String>> enl = new NamedList<>();
    NamedList<String> enlkey1 = new NamedList<>();
    NamedList<String> enlkey2 = null;
    enlkey1.add("key1a", "value1a");
    enlkey1.add("key1b", "value1b");
    enl.add("key1", enlkey1);
    enl.add("key2", enlkey2);
    
    // Tests that are very similar to the test above, just repeated
    // on the explicitly nested object type.
    String enltest1 = (String) enl.findRecursive("key1", "key1a");
    assertEquals("value1a", enltest1);
    String enltest2 = (String) enl.findRecursive("key1", "key1b");
    assertEquals("value1b", enltest2);
    // Verify that when a null value is stored, the standard get method
    // says it is null, then check the recursive method.
    Object enltest3 = enl.get("key2");
    assertNull(enltest3);
    Object enltest4 = enl.findRecursive("key2");
    assertNull(enltest4);
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  @SuppressWarnings({"unchecked"})
  public void testShallowMap() {
    @SuppressWarnings({"rawtypes"})
    NamedList nl = new NamedList();
    nl.add("key1", "Val1");
    @SuppressWarnings({"rawtypes"})
    Map m = nl.asShallowMap();
    m.put("key1", "Val1_");
    assertEquals("Val1_", nl.get("key1"));
    assertEquals("Val1_", m.get("key1"));
    assertEquals(0, nl.indexOf("key1", 0));
    m.putAll(Utils.makeMap("key1", "Val1__", "key2", "Val2"));
    assertEquals("Val1__", nl.get("key1"));
    assertEquals("Val1__", m.get("key1"));
    assertEquals(0, nl.indexOf("key1", 0));
    assertEquals("Val2", nl.get("key2"));
    assertEquals("Val2", m.get("key2"));

  }
}
