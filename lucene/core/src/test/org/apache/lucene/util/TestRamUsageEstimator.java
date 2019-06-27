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


import static org.apache.lucene.util.RamUsageEstimator.*;
import static org.apache.lucene.util.RamUsageTester.sizeOf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.TermQuery;

public class TestRamUsageEstimator extends LuceneTestCase {

  static final String[] strings = new String[] {
      "test string",
      "hollow",
      "catchmaster"
  };

  public void testSanity() {
    assertTrue(sizeOf("test string") > shallowSizeOfInstance(String.class));

    Holder holder = new Holder();
    holder.holder = new Holder("string2", 5000L);
    assertTrue(sizeOf(holder) > shallowSizeOfInstance(Holder.class));
    assertTrue(sizeOf(holder) > sizeOf(holder.holder));
    
    assertTrue(
        shallowSizeOfInstance(HolderSubclass.class) >= shallowSizeOfInstance(Holder.class));
    assertTrue(
        shallowSizeOfInstance(Holder.class)         == shallowSizeOfInstance(HolderSubclass2.class));

    assertTrue(sizeOf(strings) > shallowSizeOf(strings));
  }

  public void testStaticOverloads() {
    Random rnd = random();
    {
      byte[] array = new byte[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      boolean[] array = new boolean[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      char[] array = new char[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      short[] array = new short[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      int[] array = new int[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      float[] array = new float[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      long[] array = new long[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
    
    {
      double[] array = new double[rnd.nextInt(1024)];
      assertEquals(sizeOf(array), sizeOf((Object) array));
    }
  }

  public void testStrings() {
    long actual = sizeOf(strings);
    long estimated = RamUsageEstimator.sizeOf(strings);
    assertEquals(actual, estimated);
  }

  public void testBytesRefHash() {
    BytesRefHash bytes = new BytesRefHash();
    for (int i = 0; i < 100; i++) {
      bytes.add(new BytesRef("foo bar " + i));
      bytes.add(new BytesRef("baz bam " + i));
    }
    long actual = sizeOf(bytes);
    long estimated = RamUsageEstimator.sizeOf(bytes);
    assertEquals(actual, estimated);
  }

  public void testMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("primitive", 1234L);
    map.put("string", "string");
    long actual = sizeOf(map);
    long estimated = RamUsageEstimator.sizeOfObject(map);
    assertTrue(estimated > actual); // RamUsageTester under-estimates the size of map

    // test recursion
    map.clear();
    map.put("string[]", new String[]{"foo", "bar"});
    map.put("map", Collections.singletonMap("foo", "bar"));
    map.put("self", map);
    actual = sizeOf(map);
    estimated = RamUsageEstimator.sizeOfObject(map);
    assertTrue(estimated > actual);
  }

  public void testCollection() {
    List<Object> list = new ArrayList<>();
    list.add(1234L);
    list.add("string");
    list.add(new Term("foo", "bar"));
    long actual = sizeOf(list);
    long estimated = RamUsageEstimator.sizeOfObject(list);
    assertEquals(actual, estimated);

    // test recursion
    list.clear();
    list.add(1234L);
    list.add(list);
    actual = sizeOf(list);
    estimated = RamUsageEstimator.sizeOfObject(list);
    assertEquals(actual + RamUsageEstimator.shallowSizeOf(list), estimated);
  }

  public void testQuery() {
    DisjunctionMaxQuery dismax = new DisjunctionMaxQuery(
        Arrays.asList(new TermQuery(new Term("foo", "bar")), new TermQuery(new Term("baz", "bam"))), 1.0f);
    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD)
        .add(new FuzzyQuery(new Term("foo", "baz")), BooleanClause.Occur.MUST_NOT)
        .add(dismax, BooleanClause.Occur.MUST)
        .build();
    long actual = sizeOf(bq);
    long estimated = RamUsageEstimator.sizeOfObject(bq);
    assertTrue(actual < estimated);
  }

  public void testReferenceSize() {
    assertTrue(NUM_BYTES_OBJECT_REF == 4 || NUM_BYTES_OBJECT_REF == 8);
    if (Constants.JRE_IS_64BIT) {
      assertEquals("For 64 bit JVMs, reference size must be 8, unless compressed references are enabled",
          COMPRESSED_REFS_ENABLED ? 4 : 8, NUM_BYTES_OBJECT_REF);
    } else {
      assertEquals("For 32bit JVMs, reference size must always be 4", 4, NUM_BYTES_OBJECT_REF);
      assertFalse("For 32bit JVMs, compressed references can never be enabled", COMPRESSED_REFS_ENABLED);
    }
  }
  
  public void testHotspotBean() {
    assumeTrue("testHotspotBean only works on 64bit JVMs.", Constants.JRE_IS_64BIT);
    try {
      Class.forName(MANAGEMENT_FACTORY_CLASS);
    } catch (ClassNotFoundException e) {
      assumeNoException("testHotspotBean does not work on Java 8+ compact profile.", e);
    }
    try {
      Class.forName(HOTSPOT_BEAN_CLASS);
    } catch (ClassNotFoundException e) {
      assumeNoException("testHotspotBean only works on Hotspot (OpenJDK, Oracle) virtual machines.", e);
    }
    
    assertTrue("We should have been able to detect Hotspot's internal settings from the management bean.", JVM_IS_HOTSPOT_64BIT);
  }
  
  /** Helper to print out current settings for debugging {@code -Dtests.verbose=true} */
  public void testPrintValues() {
    assumeTrue("Specify -Dtests.verbose=true to print constants of RamUsageEstimator.", VERBOSE);
    System.out.println("JVM_IS_HOTSPOT_64BIT = " + JVM_IS_HOTSPOT_64BIT);
    System.out.println("COMPRESSED_REFS_ENABLED = " + COMPRESSED_REFS_ENABLED);
    System.out.println("NUM_BYTES_OBJECT_ALIGNMENT = " + NUM_BYTES_OBJECT_ALIGNMENT);
    System.out.println("NUM_BYTES_OBJECT_REF = " + NUM_BYTES_OBJECT_REF);
    System.out.println("NUM_BYTES_OBJECT_HEADER = " + NUM_BYTES_OBJECT_HEADER);
    System.out.println("NUM_BYTES_ARRAY_HEADER = " + NUM_BYTES_ARRAY_HEADER);
    System.out.println("LONG_SIZE = " + LONG_SIZE);
    System.out.println("LONG_CACHE_MIN_VALUE = " + LONG_CACHE_MIN_VALUE);
    System.out.println("LONG_CACHE_MAX_VALUE = " + LONG_CACHE_MAX_VALUE);
  }

  @SuppressWarnings("unused")
  private static class Holder {
    long field1 = 5000L;
    String name = "name";
    Holder holder;
    long field2, field3, field4;
    
    Holder() {}
    
    Holder(String name, long field1) {
      this.name = name;
      this.field1 = field1;
    }
  }
  
  @SuppressWarnings("unused")
  private static class HolderSubclass extends Holder {
    byte foo;
    int bar;
  }
  
  private static class HolderSubclass2 extends Holder {
    // empty, only inherits all fields -> size should be identical to superclass
  }
}
