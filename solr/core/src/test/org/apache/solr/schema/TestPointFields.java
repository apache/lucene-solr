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
package org.apache.solr.schema;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.DateMathParser;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for PointField functionality
 *
 *
 */
public class TestPointFields extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-point.xml");
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    clearIndex();
    assertU(commit());
    super.tearDown();
  }
  
  @Test
  public void testIntPointFieldExactQuery() throws Exception {
    doTestIntPointFieldExactQuery("number_p_i", false);
    doTestIntPointFieldExactQuery("number_p_i_mv", false);
    doTestIntPointFieldExactQuery("number_p_i_ni_dv", false);
    doTestIntPointFieldExactQuery("number_p_i_ni_ns_dv", false);
    doTestIntPointFieldExactQuery("number_p_i_ni_mv_dv", false);
  }
  
  @Test
  public void testIntPointFieldReturn() throws Exception {
    testPointFieldReturn("number_p_i", "int", new String[]{"0", "-1", "2", "3", "43", "52", "-60", "74", "80", "99"});
    clearIndex();
    assertU(commit());
    testPointFieldReturn("number_p_i_dv_ns", "int", new String[]{"0", "-1", "2", "3", "43", "52", "-60", "74", "80", "99"});
  }
  
  @Test
  public void testIntPointFieldRangeQuery() throws Exception {
    doTestIntPointFieldRangeQuery("number_p_i", "int", false);
    doTestIntPointFieldRangeQuery("number_p_i_ni_ns_dv", "int", false);
    doTestIntPointFieldRangeQuery("number_p_i_dv", "int", false);
  }
  
  @Test
  public void testIntPointFieldSort() throws Exception {
    doTestPointFieldSort("number_p_i", "number_p_i_dv", new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"});
  }
  
  @Test
  public void testIntPointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_i", "number_p_i_dv", getSequentialStringArrayWithInts(10));
  }

  @Test
  public void testIntPointFieldRangeFacet() throws Exception {
    doTestIntPointFieldRangeFacet("number_p_i_dv", "number_p_i");
  }
  
  
  @Test
  public void testIntPointFunctionQuery() throws Exception {
    doTestIntPointFunctionQuery("number_p_i_dv", "number_p_i", "int");
  }


  @Test
  public void testIntPointStats() throws Exception {
    testPointStats("number_p_i", "number_p_i_dv", new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
        0D, 9D, "10", "1", 0D);
    testPointStats("number_p_i", "number_p_i_mv_dv", new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
        0D, 9D, "10", "1", 0D);
  }

  @Test
  public void testIntPointFieldMultiValuedExactQuery() throws Exception {
    testPointFieldMultiValuedExactQuery("number_p_i_mv", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedExactQuery("number_p_i_ni_mv_dv", getSequentialStringArrayWithInts(20));
  }
  
  @Test
  public void testIntPointFieldMultiValuedReturn() throws Exception {
    testPointFieldMultiValuedReturn("number_p_i_mv", "int", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedReturn("number_p_i_ni_mv_dv", "int", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedReturn("number_p_i_dv_ns_mv", "int", getSequentialStringArrayWithInts(20));
  }
  
  @Test
  public void testIntPointFieldMultiValuedRangeQuery() throws Exception {
    testPointFieldMultiValuedRangeQuery("number_p_i_mv", "int", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedRangeQuery("number_p_i_ni_mv_dv", "int", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedRangeQuery("number_p_i_mv_dv", "int", getSequentialStringArrayWithInts(20));
  }
  
  //TODO MV SORT?
  @Test
  public void testIntPointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_i_mv", "number_p_i_mv_dv", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedFacetField("number_p_i_mv", "number_p_i_mv_dv", getRandomStringArrayWithInts(20, false));
  }

  @Test
  public void testIntPointFieldMultiValuedRangeFacet() throws Exception {
    doTestIntPointFieldMultiValuedRangeFacet("number_p_i_mv_dv", "number_p_i_mv");
  }

  @Test
  public void testIntPointMultiValuedFunctionQuery() throws Exception {
    testPointMultiValuedFunctionQuery("number_p_i_mv", "number_p_i_mv_dv", "int", getSequentialStringArrayWithInts(20));
  }
  
  @Test
  public void testIntPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testIntPointFieldsAtomicUpdates("number_p_i", "int");
    testIntPointFieldsAtomicUpdates("number_p_i_dv", "int");
    testIntPointFieldsAtomicUpdates("number_p_i_dv_ns", "int");
  }
  
  @Test
  public void testMultiValuedIntPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_i_mv", "int");
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_i_ni_mv_dv", "int");
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_i_dv_ns_mv", "int");
  }
  
  @Test
  public void testIntPointSetQuery() throws Exception {
    doTestSetQueries("number_p_i", getRandomStringArrayWithInts(10, false), false);
    doTestSetQueries("number_p_i_mv", getRandomStringArrayWithInts(10, false), true);
    doTestSetQueries("number_p_i_ni_dv", getRandomStringArrayWithInts(10, false), false);
  }
  
  // DoublePointField

  @Test
  public void testDoublePointFieldExactQuery() throws Exception {
    doTestFloatPointFieldExactQuery("number_d");
    doTestFloatPointFieldExactQuery("number_p_d");
    doTestFloatPointFieldExactQuery("number_p_d_mv");
    doTestFloatPointFieldExactQuery("number_p_d_ni_dv");
    doTestFloatPointFieldExactQuery("number_p_d_ni_ns_dv");
    doTestFloatPointFieldExactQuery("number_p_d_ni_mv_dv");
  }
  
  @Test
  public void testDoublePointFieldReturn() throws Exception {
    testPointFieldReturn("number_p_d", "double", new String[]{"0.0", "1.2", "2.5", "3.02", "0.43", "5.2", "6.01", "74.0", "80.0", "9.9"});
    clearIndex();
    assertU(commit());
    testPointFieldReturn("number_p_d_dv_ns", "double", new String[]{"0.0", "1.2", "2.5", "3.02", "0.43", "5.2", "6.01", "74.0", "80.0", "9.9"});
    clearIndex();
    assertU(commit());
    String[] arr = new String[atLeast(10)];
    for (int i = 0; i < arr.length; i++) {
      double rand = random().nextDouble() * 10;
      arr[i] = String.valueOf(rand);
    }
    testPointFieldReturn("number_p_d", "double", arr);
  }
  
  @Test
  public void testDoublePointFieldRangeQuery() throws Exception {
    doTestFloatPointFieldRangeQuery("number_p_d", "double", true);
    doTestFloatPointFieldRangeQuery("number_p_d_ni_ns_dv", "double", true);
    doTestFloatPointFieldRangeQuery("number_p_d_dv", "double", true);
  }
  
  @Test
  public void testDoublePointFieldSort() throws Exception {
    String[] arr = getRandomStringArrayWithDoubles(10, true);
    doTestPointFieldSort("number_p_d", "number_p_d_dv", arr);
  }
  
  @Test
  public void testDoublePointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_d", "number_p_d_dv", getSequentialStringArrayWithDoubles(10));
    clearIndex();
    assertU(commit());
    testPointFieldFacetField("number_p_d", "number_p_d_dv", getRandomStringArrayWithDoubles(10, false));
  }

  @Test
  public void testDoublePointFieldRangeFacet() throws Exception {
    doTestFloatPointFieldRangeFacet("number_p_d_dv", "number_p_d");
  }

  @Test
  public void testDoublePointFunctionQuery() throws Exception {
    doTestFloatPointFunctionQuery("number_p_d_dv", "number_p_d", "double");
  }
  
  @Test
  public void testDoublePointStats() throws Exception {
    testPointStats("number_p_d", "number_p_d_dv", new String[]{"-10.0", "1.1", "2.2", "3.3", "4.4", "5.5", "6.6", "7.7", "8.8", "9.9"},
        -10.0D, 9.9D, "10", "1", 1E-10D);
    testPointStats("number_p_d_mv", "number_p_d_mv_dv", new String[]{"-10.0", "1.1", "2.2", "3.3", "4.4", "5.5", "6.6", "7.7", "8.8", "9.9"},
        -10.0D, 9.9D, "10", "1", 1E-10D);
  }
  
  @Test
  public void testDoublePointFieldMultiValuedExactQuery() throws Exception {
    testPointFieldMultiValuedExactQuery("number_p_d_mv", getRandomStringArrayWithDoubles(20, false));
    testPointFieldMultiValuedExactQuery("number_p_d_ni_mv_dv", getRandomStringArrayWithDoubles(20, false));
  }
  
  @Test
  public void testDoublePointFieldMultiValuedReturn() throws Exception {
    testPointFieldMultiValuedReturn("number_p_d_mv", "double", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedReturn("number_p_d_ni_mv_dv", "double", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedReturn("number_p_d_dv_ns_mv", "double", getSequentialStringArrayWithDoubles(20));
  }
  
  @Test
  public void testDoublePointFieldMultiValuedRangeQuery() throws Exception {
    testPointFieldMultiValuedRangeQuery("number_p_d_mv", "double", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedRangeQuery("number_p_d_ni_mv_dv", "double", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedRangeQuery("number_p_d_mv_dv", "double", getSequentialStringArrayWithDoubles(20));
  }
  
  @Test
  public void testDoublePointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_d_mv", "number_p_d_mv_dv", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedFacetField("number_p_d_mv", "number_p_d_mv_dv", getRandomStringArrayWithDoubles(20, false));
  }

  @Test
  public void testDoublePointFieldMultiValuedRangeFacet() throws Exception {
    doTestDoublePointFieldMultiValuedRangeFacet("number_p_d_mv_dv", "number_p_d_mv");
  }
  
  @Test
  public void testDoublePointMultiValuedFunctionQuery() throws Exception {
    testPointMultiValuedFunctionQuery("number_p_d_mv", "number_p_d_mv_dv", "double", getSequentialStringArrayWithDoubles(20));
    testPointMultiValuedFunctionQuery("number_p_d_mv", "number_p_d_mv_dv", "double", getRandomStringArrayWithFloats(20, true));
  }
  
  @Test
  public void testDoublePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    doTestFloatPointFieldsAtomicUpdates("number_p_d", "double");
    doTestFloatPointFieldsAtomicUpdates("number_p_d_dv", "double");
    doTestFloatPointFieldsAtomicUpdates("number_p_d_dv_ns", "double");
  }
  
  @Test
  public void testMultiValuedDoublePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_d_mv", "double");
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_d_ni_mv_dv", "double");
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_d_dv_ns_mv", "double");
  }
  
  
  private void doTestFloatPointFieldsAtomicUpdates(String field, String type) throws Exception {
    assertU(adoc(sdoc("id", "1", field, "1.1234")));
    assertU(commit());

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", 1.1F))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='2.2234']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", -1.1F))));
    assertU(commit());
    
    // TODO: can this test be better?
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.>'1.1233']",
        "//result/doc[1]/" + type + "[@name='" + field + "'][.<'1.1235']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", 3.123F))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='3.123']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", 3.14F))));
    assertU(commit());
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", 1F))));
    assertU(commit());
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.>'4.13']",
        "//result/doc[1]/" + type + "[@name='" + field + "'][.<'4.15']");
  }
  
  @Test
  public void testDoublePointSetQuery() throws Exception {
    doTestSetQueries("number_p_d", getRandomStringArrayWithDoubles(10, false), false);
    doTestSetQueries("number_p_d_mv", getRandomStringArrayWithDoubles(10, false), true);
    doTestSetQueries("number_p_d_ni_dv", getRandomStringArrayWithDoubles(10, false), false);
  }
  
  // Float

  @Test
  public void testFloatPointFieldExactQuery() throws Exception {
    doTestFloatPointFieldExactQuery("number_p_f");
    doTestFloatPointFieldExactQuery("number_p_f_mv");
    doTestFloatPointFieldExactQuery("number_p_f_ni_dv");
    doTestFloatPointFieldExactQuery("number_p_f_ni_ns_dv");
    doTestFloatPointFieldExactQuery("number_p_f_ni_mv_dv");
  }
  
  @Test
  public void testFloatPointFieldReturn() throws Exception {
    testPointFieldReturn("number_p_f", "float", new String[]{"0.0", "-1.2", "2.5", "3.02", "0.43", "5.2", "6.01", "74.0", "80.0", "9.9"});
    clearIndex();
    assertU(commit());
    testPointFieldReturn("number_p_f_dv_ns", "float", new String[]{"0.0", "-1.2", "2.5", "3.02", "0.43", "5.2", "6.01", "74.0", "80.0", "9.9"});
    clearIndex();
    assertU(commit());
    String[] arr = new String[atLeast(10)];
    for (int i = 0; i < arr.length; i++) {
      float rand = random().nextFloat() * 10;
      arr[i] = String.valueOf(rand);
    }
    testPointFieldReturn("number_p_f", "float", arr);
  }
  
  @Test
  public void testFloatPointFieldRangeQuery() throws Exception {
    doTestFloatPointFieldRangeQuery("number_p_f", "float", false);
    doTestFloatPointFieldRangeQuery("number_p_f_ni_ns_dv", "float", false);
    doTestFloatPointFieldRangeQuery("number_p_f_dv", "float", false);
  }
  
  @Test
  public void testFloatPointFieldSort() throws Exception {
    String[] arr = getRandomStringArrayWithFloats(10, true);
    doTestPointFieldSort("number_p_f", "number_p_f_dv", arr);
  }
  
  @Test
  public void testFloatPointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_f", "number_p_f_dv", getSequentialStringArrayWithDoubles(10));
    clearIndex();
    assertU(commit());
    testPointFieldFacetField("number_p_f", "number_p_f_dv", getRandomStringArrayWithFloats(10, false));
  }

  @Test
  public void testFloatPointFieldRangeFacet() throws Exception {
    doTestFloatPointFieldRangeFacet("number_p_f_dv", "number_p_f");
  }

  @Test
  public void testFloatPointFunctionQuery() throws Exception {
    doTestFloatPointFunctionQuery("number_p_f_dv", "number_p_f", "float");
  }
  
  @Test
  public void testFloatPointStats() throws Exception {
    testPointStats("number_p_f", "number_p_f_dv", new String[]{"-10.0", "1.1", "2.2", "3.3", "4.4", "5.5", "6.6", "7.7", "8.8", "9.9"},
        -10D, 9.9D, "10", "1", 1E-6D);
    testPointStats("number_p_f_mv", "number_p_f_mv_dv", new String[]{"-10.0", "1.1", "2.2", "3.3", "4.4", "5.5", "6.6", "7.7", "8.8", "9.9"},
        -10D, 9.9D, "10", "1", 1E-6D);
  }
  
  @Test
  public void testFloatPointFieldMultiValuedExactQuery() throws Exception {
    testPointFieldMultiValuedExactQuery("number_p_f_mv", getRandomStringArrayWithFloats(20, false));
    testPointFieldMultiValuedExactQuery("number_p_f_ni_mv_dv", getRandomStringArrayWithFloats(20, false));
  }
  
  @Test
  public void testFloatPointFieldMultiValuedReturn() throws Exception {
    testPointFieldMultiValuedReturn("number_p_f_mv", "float", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedReturn("number_p_f_ni_mv_dv", "float", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedReturn("number_p_f_dv_ns_mv", "float", getSequentialStringArrayWithDoubles(20));
  }
  
  @Test
  public void testFloatPointFieldMultiValuedRangeQuery() throws Exception {
    testPointFieldMultiValuedRangeQuery("number_p_f_mv", "float", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedRangeQuery("number_p_f_ni_mv_dv", "float", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedRangeQuery("number_p_f_mv_dv", "float", getSequentialStringArrayWithDoubles(20));
  }
  
  @Test
  public void testFloatPointFieldMultiValuedRangeFacet() throws Exception {
    doTestDoublePointFieldMultiValuedRangeFacet("number_p_f_mv_dv", "number_p_f_mv");
  }
  
  @Test
  public void testFloatPointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_f_mv", "number_p_f_mv_dv", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedFacetField("number_p_f_mv", "number_p_f_mv_dv", getRandomStringArrayWithFloats(20, false));
  }
  
  @Test
  public void testFloatPointMultiValuedFunctionQuery() throws Exception {
    testPointMultiValuedFunctionQuery("number_p_f_mv", "number_p_f_mv_dv", "float", getSequentialStringArrayWithDoubles(20));
    testPointMultiValuedFunctionQuery("number_p_f_mv", "number_p_f_mv_dv", "float", getRandomStringArrayWithFloats(20, true));
  }
  
  
  @Test
  public void testFloatPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    doTestFloatPointFieldsAtomicUpdates("number_p_f", "float");
    doTestFloatPointFieldsAtomicUpdates("number_p_f_dv", "float");
    doTestFloatPointFieldsAtomicUpdates("number_p_f_dv_ns", "float");
  }
  
  @Test
  public void testMultiValuedFloatePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_f_mv", "float");
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_f_ni_mv_dv", "float");
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_f_dv_ns_mv", "float");
  }

  @Test
  public void testFloatPointSetQuery() throws Exception {
    doTestSetQueries("number_p_f", getRandomStringArrayWithFloats(10, false), false);
    doTestSetQueries("number_p_f_mv", getRandomStringArrayWithFloats(10, false), true);
    doTestSetQueries("number_p_f_ni_dv", getRandomStringArrayWithFloats(10, false), false);
  }
  
  // Long
  
  @Test
  public void testLongPointFieldExactQuery() throws Exception {
    doTestIntPointFieldExactQuery("number_p_l", true);
    doTestIntPointFieldExactQuery("number_p_l_mv", true);
    doTestIntPointFieldExactQuery("number_p_l_ni_dv", true);
    doTestIntPointFieldExactQuery("number_p_l_ni_ns_dv", true);
    doTestIntPointFieldExactQuery("number_p_l_ni_mv_dv", true);
  }
  
  @Test
  public void testLongPointFieldReturn() throws Exception {
    testPointFieldReturn("number_p_l", "long", new String[]{"0", "-1", "2", "3", "43", "52", "-60", "74", "80", "99", String.valueOf(Long.MAX_VALUE)});
    clearIndex();
    assertU(commit());
    testPointFieldReturn("number_p_l_dv_ns", "long", new String[]{"0", "-1", "2", "3", "43", "52", "-60", "74", "80", "99", String.valueOf(Long.MAX_VALUE)});
  }
  
  @Test
  public void testLongPointFieldRangeQuery() throws Exception {
    doTestIntPointFieldRangeQuery("number_p_l", "long", true);
    doTestIntPointFieldRangeQuery("number_p_l_ni_ns_dv", "long", true);
    doTestIntPointFieldRangeQuery("number_p_l_dv", "long", true);
  }
  
  @Test
  public void testLongPointFieldSort() throws Exception {
    doTestPointFieldSort("number_p_l", "number_p_l_dv", new String[]{String.valueOf(Integer.MIN_VALUE), 
        "1", "2", "3", "4", "5", "6", "7", 
        String.valueOf(Integer.MAX_VALUE), String.valueOf(Long.MAX_VALUE)});
  }
  
  @Test
  public void testLongPointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_l", "number_p_l_dv", getSequentialStringArrayWithInts(10));
    clearIndex();
    assertU(commit());
    testPointFieldFacetField("number_p_l", "number_p_l_dv", getRandomStringArrayWithLongs(10, true));
  }
  
  @Test
  public void testLongPointFieldRangeFacet() throws Exception {
    doTestIntPointFieldRangeFacet("number_p_l_dv", "number_p_l");
  }
  
  @Test
  public void testLongPointFunctionQuery() throws Exception {
    doTestIntPointFunctionQuery("number_p_l_dv", "number_p_l", "long");
  }
  
  @Test
  public void testLongPointStats() throws Exception {
    testPointStats("number_p_l", "number_p_l_dv", new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
        0D, 9D, "10", "1", 0D);
    testPointStats("number_p_l_mv", "number_p_l_mv_dv", new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
        0D, 9D, "10", "1", 0D);
  }
  
  @Test
  public void testLongPointFieldMultiValuedExactQuery() throws Exception {
    testPointFieldMultiValuedExactQuery("number_p_l_mv", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedExactQuery("number_p_l_ni_mv_dv", getSequentialStringArrayWithInts(20));
  }
  
  @Test
  public void testLongPointFieldMultiValuedReturn() throws Exception {
    testPointFieldMultiValuedReturn("number_p_l_mv", "long", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedReturn("number_p_l_ni_mv_dv", "long", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedReturn("number_p_l_dv_ns_mv", "long", getSequentialStringArrayWithInts(20));
  }
  
  @Test
  public void testLongPointFieldMultiValuedRangeQuery() throws Exception {
    testPointFieldMultiValuedRangeQuery("number_p_l_mv", "long", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedRangeQuery("number_p_l_ni_mv_dv", "long", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedRangeQuery("number_p_l_mv_dv", "long", getSequentialStringArrayWithInts(20));
  }
  
  @Test
  public void testLongPointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_l_mv", "number_p_l_mv_dv", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedFacetField("number_p_l_mv", "number_p_l_mv_dv", getRandomStringArrayWithLongs(20, false));
  }
  
  @Test
  public void testLongPointFieldMultiValuedRangeFacet() throws Exception {
    doTestIntPointFieldMultiValuedRangeFacet("number_p_l_mv_dv", "number_p_l_mv");
  }
  
  @Test
  public void testLongPointMultiValuedFunctionQuery() throws Exception {
    testPointMultiValuedFunctionQuery("number_p_l_mv", "number_p_l_mv_dv", "long", getSequentialStringArrayWithInts(20));
  }
  
  @Test
  public void testLongPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testIntPointFieldsAtomicUpdates("number_p_l", "long");
    testIntPointFieldsAtomicUpdates("number_p_l_dv", "long");
    testIntPointFieldsAtomicUpdates("number_p_l_dv_ns", "long");
  }
  
  @Test
  public void testMultiValuedLongPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_l_mv", "long");
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_l_ni_mv_dv", "long");
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_l_dv_ns_mv", "long");
  }
  
  @Test
  public void testLongPointSetQuery() throws Exception {
    doTestSetQueries("number_p_l", getRandomStringArrayWithLongs(10, false), false);
    doTestSetQueries("number_p_l_mv", getRandomStringArrayWithLongs(10, false), true);
    doTestSetQueries("number_p_l_ni_dv", getRandomStringArrayWithLongs(10, false), false);
  }

  // Date

  @Test
  public void testDatePointFieldExactQuery() throws Exception {
    doTestDatePointFieldExactQuery("number_p_dt", "1995-12-31T23:59:59Z");
    doTestDatePointFieldExactQuery("number_p_dt_mv", "2015-12-31T23:59:59Z-1DAY");
    doTestDatePointFieldExactQuery("number_p_dt_ni_dv", "2000-12-31T23:59:59Z+3DAYS");
    doTestDatePointFieldExactQuery("number_p_dt_ni_ns_dv", "1995-12-31T23:59:59Z-1MONTH");
    doTestDatePointFieldExactQuery("number_p_dt_ni_mv_dv", "1995-12-31T23:59:59Z+2MONTHS");
  }

  @Test
  public void testDatePointFieldReturn() throws Exception {
    testPointFieldReturn("number_p_dt", "date",
        new String[]{"1995-12-31T23:59:59Z", "1994-02-28T23:59:59Z",
            "2015-12-31T23:59:59Z", "2000-10-31T23:59:59Z", "1999-12-31T12:59:59Z"});
    clearIndex();
    assertU(commit());
    testPointFieldReturn("number_p_dt_dv_ns", "date",
        new String[]{"1995-12-31T23:59:59Z", "1994-02-28T23:59:59Z",
            "2015-12-31T23:59:59Z", "2000-10-31T23:59:59Z", "1999-12-31T12:59:59Z"});
  }

  @Test
  public void testDatePointFieldRangeQuery() throws Exception {
    doTestDatePointFieldRangeQuery("number_p_dt");
    doTestDatePointFieldRangeQuery("number_p_dt_ni_ns_dv");
  }

  @Test
  public void testDatePointFieldSort() throws Exception {
    doTestPointFieldSort("number_p_dt", "number_p_dt_dv", getSequentialStringArrayWithDates(10));
  }

  @Test
  public void testDatePointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_dt", "number_p_dt_dv", getSequentialStringArrayWithDates(10));
    clearIndex();
    assertU(commit());
    testPointFieldFacetField("number_p_dt", "number_p_dt_dv", getSequentialStringArrayWithDates(10));
  }

  @Test
  public void testDatePointFieldRangeFacet() throws Exception {
    doTestDatePointFieldRangeFacet("number_p_dt_dv", "number_p_dt");
  }

  @Test
  public void testDatePointFunctionQuery() throws Exception {
    doTestDatePointFunctionQuery("number_p_dt_dv", "number_p_dt", "date");
  }

  @Test
  public void testDatePointStats() throws Exception {
    testDatePointStats("number_p_dt", "number_p_dt_dv", getSequentialStringArrayWithDates(10));
    testDatePointStats("number_p_dt_mv", "number_p_dt_mv_dv", getSequentialStringArrayWithDates(10));
  }

  @Test
  public void testDatePointFieldMultiValuedExactQuery() throws Exception {
    testPointFieldMultiValuedExactQuery("number_p_dt_mv", getSequentialStringArrayWithDates(20));
    testPointFieldMultiValuedExactQuery("number_p_dt_ni_mv_dv", getSequentialStringArrayWithDates(20));
  }

  @Test
  public void testDatePointFieldMultiValuedReturn() throws Exception {
    testPointFieldMultiValuedReturn("number_p_dt_mv", "date", getSequentialStringArrayWithDates(20));
    testPointFieldMultiValuedReturn("number_p_dt_ni_mv_dv", "date", getSequentialStringArrayWithDates(20));
    testPointFieldMultiValuedReturn("number_p_dt_dv_ns_mv", "date", getSequentialStringArrayWithDates(20));
  }

  @Test
  public void testDatePointFieldMultiValuedRangeQuery() throws Exception {
    testPointFieldMultiValuedRangeQuery("number_p_dt_mv", "date", getSequentialStringArrayWithDates(20));
    testPointFieldMultiValuedRangeQuery("number_p_dt_ni_mv_dv", "date", getSequentialStringArrayWithDates(20));
  }

  @Test
  public void testDatePointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_dt_mv", "number_p_dt_mv_dv", getSequentialStringArrayWithDates(20));
    testPointFieldMultiValuedFacetField("number_p_dt_mv", "number_p_dt_mv_dv", getRandomStringArrayWithDates(20, false));
  }

  @Test
  public void testDatePointFieldMultiValuedRangeFacet() throws Exception {
    doTestDatePointFieldMultiValuedRangeFacet("number_p_dt_mv_dv", "number_p_dt_mv");
  }

  @Test
  public void testDatePointMultiValuedFunctionQuery() throws Exception {
    testPointMultiValuedFunctionQuery("number_p_dt_mv", "number_p_dt_mv_dv", "date", getSequentialStringArrayWithDates(20));
  }

  @Test
  public void testDatePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testDatePointFieldsAtomicUpdates("number_p_dt", "date");
    testDatePointFieldsAtomicUpdates("number_p_dt_dv", "date");
    testDatePointFieldsAtomicUpdates("number_p_dt_dv_ns", "date");
  }

  @Test
  public void testMultiValuedDatePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedDatePointFieldsAtomicUpdates("number_p_dt_mv", "date");
    testMultiValuedDatePointFieldsAtomicUpdates("number_p_dt_ni_mv_dv", "date");
    testMultiValuedDatePointFieldsAtomicUpdates("number_p_dt_dv_ns_mv", "date");
  }

  @Test
  public void testDatePointSetQuery() throws Exception {
    doTestSetQueries("number_p_dt", getRandomStringArrayWithDates(10, false), false);
    doTestSetQueries("number_p_dt_mv", getRandomStringArrayWithDates(10, false), true);
    doTestSetQueries("number_p_dt_ni_dv", getRandomStringArrayWithDates(10, false), false);
  }
  
  @Test
  public void testIndexOrDocValuesQuery() throws Exception {
    String[] fieldTypeNames = new String[]{"_p_i", "_p_l", "_p_d", "_p_f"};
    FieldType[] fieldTypes = new FieldType[]{new IntPointField(), new LongPointField(), new DoublePointField(), new FloatPointField()};
    assert fieldTypeNames.length == fieldTypes.length;
    for (int i = 0; i < fieldTypeNames.length; i++) {
      SchemaField fieldIndexed = h.getCore().getLatestSchema().getField("foo_" + fieldTypeNames[i]);
      SchemaField fieldIndexedAndDv = h.getCore().getLatestSchema().getField("foo_" + fieldTypeNames[i] + "_dv");
      SchemaField fieldIndexedMv = h.getCore().getLatestSchema().getField("foo_" + fieldTypeNames[i] + "_mv");
      SchemaField fieldIndexedAndDvMv = h.getCore().getLatestSchema().getField("foo_" + fieldTypeNames[i] + "_mv_dv");
      assertTrue(fieldTypes[i].getRangeQuery(null, fieldIndexed, "0", "10", true, true) instanceof PointRangeQuery);
      assertTrue(fieldTypes[i].getRangeQuery(null, fieldIndexedAndDv, "0", "10", true, true) instanceof IndexOrDocValuesQuery);
      assertTrue(fieldTypes[i].getRangeQuery(null, fieldIndexedMv, "0", "10", true, true) instanceof PointRangeQuery);
      assertTrue(fieldTypes[i].getRangeQuery(null, fieldIndexedAndDvMv, "0", "10", true, true) instanceof IndexOrDocValuesQuery);
      assertTrue(fieldTypes[i].getFieldQuery(null, fieldIndexed, "0") instanceof PointRangeQuery);
      assertTrue(fieldTypes[i].getFieldQuery(null, fieldIndexedAndDv, "0") instanceof IndexOrDocValuesQuery);
      assertTrue(fieldTypes[i].getFieldQuery(null, fieldIndexedMv, "0") instanceof PointRangeQuery);
      assertTrue(fieldTypes[i].getFieldQuery(null, fieldIndexedAndDvMv, "0") instanceof IndexOrDocValuesQuery);
    }
  }
  
  // Helper methods
  
  private String[] getRandomStringArrayWithDoubles(int length, boolean sorted) {
    Set<Double> set;
    if (sorted) {
      set = new TreeSet<>();
    } else {
      set = new HashSet<>();
    }
    while (set.size() < length) {
      double f = random().nextDouble() * (Double.MAX_VALUE/2);
      if (random().nextBoolean()) {
        f = f * -1;
      }
      set.add(f);
    }
    String[] stringArr = new String[length];
    int i = 0;
    for (double val:set) {
      stringArr[i] = String.valueOf(val);
      i++;
    }
    return stringArr;
  }
  
  private String[] getRandomStringArrayWithFloats(int length, boolean sorted) {
    Set<Float> set;
    if (sorted) {
      set = new TreeSet<>();
    } else {
      set = new HashSet<>();
    }
    while (set.size() < length) {
      float f = random().nextFloat() * (Float.MAX_VALUE/2);
      if (random().nextBoolean()) {
        f = f * -1;
      }
      set.add(f);
    }
    String[] stringArr = new String[length];
    int i = 0;
    for (float val:set) {
      stringArr[i] = String.valueOf(val);
      i++;
    }
    return stringArr;
  }
  
  private String[] getSequentialStringArrayWithInts(int length) {
    String[] arr = new String[length];
    for (int i = 0; i < length; i++) {
      arr[i] = String.valueOf(i);
    }
    return arr;
  }

  private String[] getSequentialStringArrayWithDates(int length) {
    assert length < 60;
    String[] arr = new String[length];
    for (int i = 0; i < length; i++) {
      arr[i] = String.format(Locale.ROOT, "1995-12-11T19:59:%02dZ", i);
    }
    return arr;
  }
  
  private String[] getSequentialStringArrayWithDoubles(int length) {
    String[] arr = new String[length];
    for (int i = 0; i < length; i++) {
      arr[i] = String.format(Locale.ROOT, "%d.0", i);
    }
    return arr;
  }
  
  private String[] getRandomStringArrayWithInts(int length, boolean sorted) {
    Set<Integer> set;
    if (sorted) {
      set = new TreeSet<>();
    } else {
      set = new HashSet<>();
    }
    while (set.size() < length) {
      int number = random().nextInt(100);
      if (random().nextBoolean()) {
        number = number * -1;
      }
      set.add(number);
    }
    String[] stringArr = new String[length];
    int i = 0;
    for (int val:set) {
      stringArr[i] = String.valueOf(val);
      i++;
    }
    return stringArr;
  }
  
  private String[] getRandomStringArrayWithLongs(int length, boolean sorted) {
    Set<Long> set;
    if (sorted) {
      set = new TreeSet<>();
    } else {
      set = new HashSet<>();
    }
    while (set.size() < length) {
      long number = random().nextLong();
      if (random().nextBoolean()) {
        number = number * -1;
      }
      set.add(number);
    }
    String[] stringArr = new String[length];
    int i = 0;
    for (long val:set) {
      stringArr[i] = String.valueOf(val);
      i++;
    }
    return stringArr;
  }

  private String[] getRandomStringArrayWithDates(int length, boolean sorted) {
    assert length < 60;
    Set<Integer> set;
    if (sorted) {
      set = new TreeSet<>();
    } else {
      set = new HashSet<>();
    }
    while (set.size() < length) {
      int number = random().nextInt(60);
      set.add(number);
    }
    String[] stringArr = new String[length];
    int i = 0;
    for (int val:set) {
      stringArr[i] = String.format(Locale.ROOT, "1995-12-11T19:59:%02dZ", val);
      i++;
    }
    return stringArr;
  }
  
  private void doTestIntPointFieldExactQuery(String field, boolean testLong) throws Exception {
    for (int i=0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), field, String.valueOf(i+1)));
    }
    assertU(commit());
    for (int i = 0; i < 10; i++) {
      assertQ(req("q", field + ":"+(i+1), "fl", "id, " + field), 
          "//*[@numFound='1']");
    }
    
    for (int i = 0; i < 10; i++) {
      assertQ(req("q", field + ":" + (i+1) + " OR " + field + ":" + ((i+1)%10 + 1)), "//*[@numFound='2']");
    }
    
    assertU(adoc("id", String.valueOf(Integer.MAX_VALUE), field, String.valueOf(Integer.MAX_VALUE)));
    assertU(commit());
    assertQ(req("q", field + ":"+Integer.MAX_VALUE, "fl", "id, " + field), 
        "//*[@numFound='1']");
    
    if (testLong) {
      for (long i = (long)Integer.MAX_VALUE; i < (long)Integer.MAX_VALUE + 10; i++) {
        assertU(adoc("id", String.valueOf(i), field, String.valueOf(i+1)));
      }
      assertU(commit());
      for (long i = (long)Integer.MAX_VALUE; i < (long)Integer.MAX_VALUE + 10; i++) {
        assertQ(req("q", field + ":"+(i+1), "fl", "id, " + field), 
            "//*[@numFound='1']");
      }
      assertU(adoc("id", String.valueOf(Long.MAX_VALUE), field, String.valueOf(Long.MAX_VALUE)));
      assertU(commit());
      assertQ(req("q", field + ":"+Long.MAX_VALUE, "fl", "id, " + field), 
          "//*[@numFound='1']");
    }
    
    clearIndex();
    assertU(commit());
  }

  private void testPointFieldReturn(String field, String type, String[] values) throws Exception {
    SchemaField sf = h.getCore().getLatestSchema().getField(field);
    assert sf.stored() || (sf.hasDocValues() && sf.useDocValuesAsStored()): 
      "Unexpected field definition for " + field; 
    for (int i=0; i < values.length; i++) {
      assertU(adoc("id", String.valueOf(i), field, values[i]));
    }
    // Check using RTG
    if (Boolean.getBoolean("enable.update.log")) {
      for (int i = 0; i < values.length; i++) {
        assertQ(req("qt", "/get", "id", String.valueOf(i)),
            "//doc/" + type + "[@name='" + field + "'][.='" + values[i] + "']");
      }
    }
    assertU(commit());
    String[] expected = new String[values.length + 1];
    expected[0] = "//*[@numFound='" + values.length + "']"; 
    for (int i = 1; i <= values.length; i++) {
      expected[i] = "//result/doc[" + i + "]/" + type + "[@name='" + field + "'][.='" + values[i-1] + "']";
    }
    assertQ(req("q", "*:*", "fl", "id, " + field, "rows", String.valueOf(values.length)), expected);

    // Check using RTG
    if (Boolean.getBoolean("enable.update.log")) {
      for (int i = 0; i < values.length; i++) {
        assertQ(req("qt", "/get", "id", String.valueOf(i)),
            "//doc/" + type + "[@name='" + field + "'][.='" + values[i] + "']");
      }
    }
  }

  private void doTestIntPointFieldRangeQuery(String fieldName, String type, boolean testLong) throws Exception {
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, String.valueOf(i)));
    }
    assertU(commit());
    assertQ(req("q", fieldName + ":[0 TO 3]", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='4']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2']",
        "//result/doc[4]/" + type + "[@name='" + fieldName + "'][.='3']");
    
    assertQ(req("q", fieldName + ":{0 TO 3]", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='2']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='3']");
    
    assertQ(req("q", fieldName + ":[0 TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2']");
    
    assertQ(req("q", fieldName + ":{0 TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='2']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='2']");
    
    assertQ(req("q", fieldName + ":{0 TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='9']",
        "0=count(//result/doc/" + type + "[@name='" + fieldName + "'][.='0'])",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1']");
    
    assertQ(req("q", fieldName + ":{* TO 3}", "fl", "id, " + fieldName, "sort", "id desc"), 
        "//*[@numFound='3']",
        "0=count(//result/doc/" + type + "[@name='" + fieldName + "'][.='3'])",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='2']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='0']");
    
    assertQ(req("q", fieldName + ":[* TO 3}", "fl", "id, " + fieldName, "sort", "id desc"), 
        "//*[@numFound='3']",
        "0=count(//result/doc/" + type + "[@name='" + fieldName + "'][.='3'])",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='2']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='0']");
    
    assertQ(req("q", fieldName + ":[* TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']",
        "//result/doc[10]/" + type + "[@name='" + fieldName + "'][.='9']");
    
    assertQ(req("q", fieldName + ":[0 TO 1] OR " + fieldName + ":[8 TO 9]" , "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='4']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='8']",
        "//result/doc[4]/" + type + "[@name='" + fieldName + "'][.='9']");
    
    assertQ(req("q", fieldName + ":[0 TO 1] AND " + fieldName + ":[1 TO 2]" , "fl", "id, " + fieldName), 
        "//*[@numFound='1']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1']");
    
    assertQ(req("q", fieldName + ":[0 TO 1] AND NOT " + fieldName + ":[1 TO 2]" , "fl", "id, " + fieldName), 
        "//*[@numFound='1']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']");

    clearIndex();
    assertU(commit());
    
    String[] arr;
    if (testLong) {
      arr = getRandomStringArrayWithLongs(100, true);
    } else {
      arr = getRandomStringArrayWithInts(100, true);
    }
    for (int i = 0; i < arr.length; i++) {
      assertU(adoc("id", String.valueOf(i), fieldName, arr[i]));
    }
    assertU(commit());
    for (int i = 0; i < arr.length; i++) {
      assertQ(req("q", fieldName + ":[" + arr[0] + " TO " + arr[i] + "]", "fl", "id, " + fieldName), 
          "//*[@numFound='" + (i + 1) + "']");
      assertQ(req("q", fieldName + ":{" + arr[0] + " TO " + arr[i] + "}", "fl", "id, " + fieldName), 
          "//*[@numFound='" + (Math.max(0,  i-1)) + "']");
      assertQ(req("q", fieldName + ":[" + arr[0] + " TO " + arr[i] + "] AND " + fieldName + ":" + arr[0].replace("-", "\\-"), "fl", "id, " + fieldName), 
          "//*[@numFound='1']");
    }
  }
  
  private void testPointFieldFacetField(String nonDocValuesField, String docValuesField, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 10;
    
    assertFalse(h.getCore().getLatestSchema().getField(docValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, numbers[i], nonDocValuesField, numbers[i]));
    }
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + docValuesField, "facet", "true", "facet.field", docValuesField), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[1] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[3] + "'][.='1']");
    
    assertU(adoc("id", "10", docValuesField, numbers[1], nonDocValuesField, numbers[1]));
    
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + docValuesField, "facet", "true", "facet.field", docValuesField), 
        "//*[@numFound='11']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[1] + "'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[3] + "'][.='1']");
    
//    assertU(commit());
//    assertQ(req("q", "id:0", "fl", "id, " + docValuesField, "facet", "true", "facet.field", docValuesField, "facet.mincount", "0"), 
//        "//*[@numFound='1']",
//        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[0] + "'][.='1']",
//        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[1] + "'][.='0']",
//        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[2] + "'][.='0']",
//        "count(//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int))==10");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    assertQEx("Expecting Exception", 
        "Can't facet on a PointField without docValues", 
        req("q", "*:*", "fl", "id, " + nonDocValuesField, "facet", "true", "facet.field", nonDocValuesField), 
        SolrException.ErrorCode.BAD_REQUEST);
  }
  
  private void doTestIntPointFieldRangeFacet(String docValuesField, String nonDocValuesField) throws Exception {
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, String.valueOf(i), nonDocValuesField, String.valueOf(i)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10'][.='0']");
    
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10'][.='0']");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2", "facet.range.method", "filter"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10'][.='0']");
    
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10'][.='0']");
  }
  
  private void doTestIntPointFunctionQuery(String dvFieldName, String nonDvFieldName, String type) throws Exception {
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), dvFieldName, String.valueOf(i), nonDvFieldName, String.valueOf(i)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "sort", "product(-1," + dvFieldName + ") asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='" + dvFieldName + "'][.='9']",
        "//result/doc[2]/" + type + "[@name='" + dvFieldName + "'][.='8']",
        "//result/doc[3]/" + type + "[@name='" + dvFieldName + "'][.='7']",
        "//result/doc[10]/" + type + "[@name='" + dvFieldName + "'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName + ", product(-1," + dvFieldName + ")", "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/float[@name='product(-1," + dvFieldName + ")'][.='-0.0']",
        "//result/doc[2]/float[@name='product(-1," + dvFieldName + ")'][.='-1.0']",
        "//result/doc[3]/float[@name='product(-1," + dvFieldName + ")'][.='-2.0']",
        "//result/doc[10]/float[@name='product(-1," + dvFieldName + ")'][.='-9.0']");
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName + ", field(" + dvFieldName + ")", "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='field(" + dvFieldName + ")'][.='0']",
        "//result/doc[2]/" + type + "[@name='field(" + dvFieldName + ")'][.='1']",
        "//result/doc[3]/" + type + "[@name='field(" + dvFieldName + ")'][.='2']",
        "//result/doc[10]/" + type + "[@name='field(" + dvFieldName + ")'][.='9']");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDvFieldName).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDvFieldName).getType() instanceof PointField);

    assertQEx("Expecting Exception", 
        "sort param could not be parsed as a query", 
        req("q", "*:*", "fl", "id, " + nonDvFieldName, "sort", "product(-1," + nonDvFieldName + ") asc"), 
        SolrException.ErrorCode.BAD_REQUEST);
  }
  
  private void testPointStats(String field, String dvField, String[] numbers, double min, double max, String count, String missing, double delta) {
    String minMin = String.valueOf(min - Math.abs(delta*min));
    String maxMin = String.valueOf(min + Math.abs(delta*min));
    String minMax = String.valueOf(max - Math.abs(delta*max));
    String maxMax = String.valueOf(max + Math.abs(delta*max));
    for (int i = 0; i < numbers.length; i++) {
      assertU(adoc("id", String.valueOf(i), dvField, numbers[i], field, numbers[i]));
    }
    assertU(adoc("id", String.valueOf(numbers.length)));
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id, " + dvField, "stats", "true", "stats.field", dvField), 
        "//*[@numFound='11']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/double[@name='min'][.>='" + minMin + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/double[@name='min'][.<='" + maxMin+ "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/double[@name='max'][.>='" + minMax + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/double[@name='max'][.<='" + maxMax + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/long[@name='count'][.='" + count + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/long[@name='missing'][.='" + missing + "']");
    
    assertFalse(h.getCore().getLatestSchema().getField(field).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof PointField);
    assertQEx("Expecting Exception", 
        "Can't calculate stats on a PointField without docValues", 
        req("q", "*:*", "fl", "id, " + field, "stats", "true", "stats.field", field), 
        SolrException.ErrorCode.BAD_REQUEST);
  }
  
  private void testPointFieldMultiValuedExactQuery(String fieldName, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 20;
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).getType() instanceof PointField);
    for (int i=0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), fieldName, numbers[i], fieldName, numbers[i+10]));
    }
    assertU(commit());
    for (int i = 0; i < 20; i++) {
      if (h.getCore().getLatestSchema().getField(fieldName).getType() instanceof DatePointField) {
        assertQ(req("q", fieldName + ":\"" + numbers[i] + "\""),
            "//*[@numFound='1']");
      } else {
        assertQ(req("q", fieldName + ":" + numbers[i].replace("-", "\\-")),
            "//*[@numFound='1']");
      }
    }
    
    for (int i = 0; i < 20; i++) {
      if (h.getCore().getLatestSchema().getField(fieldName).getType() instanceof DatePointField) {
        assertQ(req("q", fieldName + ":\"" + numbers[i] + "\"" + " OR " + fieldName + ":\"" + numbers[(i+1)%10]+"\""), "//*[@numFound='2']");
      } else {
        assertQ(req("q", fieldName + ":" + numbers[i].replace("-", "\\-") + " OR " + fieldName + ":" + numbers[(i+1)%10].replace("-", "\\-")), "//*[@numFound='2']");
      }
    }
  }
  
  private void testPointFieldMultiValuedReturn(String fieldName, String type, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 20;
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).getType() instanceof PointField);
    for (int i=9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, numbers[i], fieldName, numbers[i+10]));
    }
    // Check using RTG before commit
    if (Boolean.getBoolean("enable.update.log")) {
      for (int i = 0; i < 10; i++) {
        assertQ(req("qt", "/get", "id", String.valueOf(i)),
            "//doc/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[i] + "']",
            "//doc/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[i+10] + "']",
            "count(//doc/arr[@name='" + fieldName + "']/" + type + ")=2");
      }
    }
    // Check using RTG after commit
    assertU(commit());
    if (Boolean.getBoolean("enable.update.log")) {
      for (int i = 0; i < 10; i++) {
        assertQ(req("qt", "/get", "id", String.valueOf(i)),
            "//doc/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[i] + "']",
            "//doc/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[i+10] + "']",
            "count(//doc/arr[@name='" + fieldName + "']/" + type + ")=2");
      }
    }
    String[] expected = new String[21];
    expected[0] = "//*[@numFound='10']"; 
    for (int i = 1; i <= 10; i++) {
      // checks for each doc's two values aren't next to eachother in array, but that doesn't matter for correctness
      expected[i] = "//result/doc[" + i + "]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[i-1] + "']";
      expected[i+10] = "//result/doc[" + i + "]/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[i + 9] + "']";
    }
    assertQ(req("q", "*:*", "fl", "id, " + fieldName, "sort","id asc"), expected);
  }
  
  private void testPointFieldMultiValuedRangeQuery(String fieldName, String type, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 20;
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).getType() instanceof PointField);
    for (int i=9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, numbers[i], fieldName, numbers[i+10]));
    }
    assertU(commit());
    assertQ(req("q", String.format(Locale.ROOT, "%s:[%s TO %s]", fieldName, numbers[0], numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[10] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[11] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[2] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[12] + "']",
        "//result/doc[4]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[3] + "']",
        "//result/doc[4]/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[13] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:{%s TO %s]", fieldName, numbers[0], numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[2] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[3] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:[%s TO %s}", fieldName, numbers[0], numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[2] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:{%s TO %s}", fieldName, numbers[0], numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[2] + "']");

    assertQ(req("q", String.format(Locale.ROOT, "%s:{%s TO *}", fieldName, numbers[0]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:{%s TO *}", fieldName, numbers[10]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='9']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:{* TO %s}", fieldName, numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:[* TO %s}", fieldName, numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']");
    
    assertQ(req("q", fieldName + ":[* TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']",
        "//result/doc[10]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[9] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:[%s TO %s] OR %s:[%s TO %s]", fieldName, numbers[0], numbers[1], fieldName, numbers[8], numbers[9]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[8] + "']",
        "//result/doc[4]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[9] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:[%s TO %s] OR %s:[%s TO %s]", fieldName, numbers[0], numbers[0], fieldName, numbers[10], numbers[10]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']");
  }

  private void testPointFieldMultiValuedFacetField(String nonDocValuesField, String dvFieldName, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 20;
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).getType() instanceof PointField);
    
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), dvFieldName, numbers[i], dvFieldName, numbers[i + 10], 
          nonDocValuesField, numbers[i], nonDocValuesField, numbers[i + 10]));
     if (rarely()) {
       assertU(commit());
     }
    }
    assertU(commit());
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[1] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[3] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[11] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[12] + "'][.='1']");
    
    assertU(adoc("id", "10", dvFieldName, numbers[1], nonDocValuesField, numbers[1]));
    
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName), 
        "//*[@numFound='11']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[1] + "'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[3] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']");
    
    assertU(adoc("id", "10", dvFieldName, numbers[1], nonDocValuesField, numbers[1], dvFieldName, numbers[1], nonDocValuesField, numbers[1]));
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName, "facet.missing", "true"), 
        "//*[@numFound='11']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[1] + "'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[3] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[not(@name)][.='0']"
        );
    
    assertU(adoc("id", "10")); // add missing values
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName, "facet.missing", "true"), 
        "//*[@numFound='11']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[1] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[3] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[not(@name)][.='1']"
        );
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName, "facet.mincount", "3"), 
        "//*[@numFound='11']",
        "count(//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int)=0");
    
    assertQ(req("q", "id:0", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName), 
        "//*[@numFound='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[0] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']",
        "count(//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int)=2");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    assertQEx("Expecting Exception", 
        "Can't facet on a PointField without docValues", 
        req("q", "*:*", "fl", "id, " + nonDocValuesField, "facet", "true", "facet.field", nonDocValuesField), 
        SolrException.ErrorCode.BAD_REQUEST);
    clearIndex();
    assertU(commit());
    
    String smaller, larger;
    try {
      if (Long.parseLong(numbers[1]) < Long.parseLong(numbers[2])) {
        smaller = numbers[1];
        larger = numbers[2];
      } else {
        smaller = numbers[2];
        larger = numbers[1];
      }
    } catch (NumberFormatException e) {
      try {
        if (Double.valueOf(numbers[1]) < Double.valueOf(numbers[2])) {
          smaller = numbers[1];
          larger = numbers[2];
        } else {
          smaller = numbers[2];
          larger = numbers[1];
        }
      } catch (NumberFormatException e2) {
        if (DateMathParser.parseMath(null, numbers[1]).getTime() < DateMathParser.parseMath(null, numbers[2]).getTime()) {
          smaller = numbers[1];
          larger = numbers[2];
        } else {
          smaller = numbers[2];
          larger = numbers[1];
        }
      }
    }
    
    assertU(adoc("id", "1", dvFieldName, smaller, dvFieldName, larger));
    assertU(adoc("id", "2", dvFieldName, larger));
    assertU(commit());
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName), 
        "//*[@numFound='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + larger + "'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + smaller + "'][.='1']",
        "count(//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int)=2");
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName, "facet.sort", "index"), 
        "//*[@numFound='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + smaller +"'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='"+ larger + "'][.='2']",
        "count(//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int)=2");
    
    clearIndex();
    assertU(commit());

  }

  private void testPointMultiValuedFunctionQuery(String nonDocValuesField, String docValuesField, String type, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 20;
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, numbers[i], docValuesField, numbers[i+10], 
          nonDocValuesField, numbers[i], nonDocValuesField, numbers[i+10]));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String function = "field(" + docValuesField + ", min)";
    
//    assertQ(req("q", "*:*", "fl", "id, " + function), 
//        "//*[@numFound='10']",
//        "//result/doc[1]/" + type + "[@name='" + function + "'][.='" + numbers[0] + "']",
//        "//result/doc[2]/" + type + "[@name='" + function + "'][.='" + numbers[1] + "']",
//        "//result/doc[3]/" + type + "[@name='" + function + "'][.='" + numbers[2] + "']",
//        "//result/doc[10]/" + type + "[@name='" + function + "'][.='" + numbers[9] + "']");
    
    assertQ(req("q", "*:*", "fl", "id, " + docValuesField, "sort", function + " desc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/str[@name='id'][.='9']",
        "//result/doc[2]/str[@name='id'][.='8']",
        "//result/doc[3]/str[@name='id'][.='7']",
        "//result/doc[10]/str[@name='id'][.='0']");

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);

    function = "field(" + nonDocValuesField + ",min)";
    
    assertQEx("Expecting Exception", 
        "sort param could not be parsed as a query", 
        req("q", "*:*", "fl", "id", "sort", function + " desc"), 
        SolrException.ErrorCode.BAD_REQUEST);
    
    assertQEx("Expecting Exception", 
        "docValues='true' is required to select 'min' value from multivalued field (" + nonDocValuesField + ") at query time", 
        req("q", "*:*", "fl", "id, " + function), 
        SolrException.ErrorCode.BAD_REQUEST);
    
    function = "field(" + docValuesField + ",foo)";
    assertQEx("Expecting Exception", 
        "Multi-Valued field selector 'foo' not supported", 
        req("q", "*:*", "fl", "id, " + function), 
        SolrException.ErrorCode.BAD_REQUEST);
  }

  private void testMultiValuedIntPointFieldsAtomicUpdates(String field, String type) throws Exception {
    assertU(adoc(sdoc("id", "1", field, "1")));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("add", 2))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=2");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("remove", 1))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", ImmutableList.of(1, 2, 3)))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='3']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=3");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("removeregex", ".*"))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=0");
    
  }
  
  private void testMultiValuedFloatPointFieldsAtomicUpdates(String field, String type) throws Exception {
    assertU(adoc(sdoc("id", "1", field, "1.0")));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1.0']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("add", 2.1f))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1.0']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2.1']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=2");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("remove", 1f))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2.1']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", ImmutableList.of(1f, 2f, 3f)))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1.0']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2.0']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='3.0']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=3");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("removeregex", ".*"))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=0");
    
  }
  
  private void testIntPointFieldsAtomicUpdates(String field, String type) throws Exception {
    assertU(adoc(sdoc("id", "1", field, "1")));
    assertU(commit());

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", 1))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='2']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", -1))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='1']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", 3))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='3']");
  }

  private void doTestFloatPointFieldExactQuery(String field) throws Exception {
    for (int i=0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), field, String.valueOf(i + "." + i)));
    }
    assertU(commit());
    for (int i = 0; i < 9; i++) {
      assertQ(req("q", field + ":"+(i+1) + "." + (i+1), "fl", "id, " + field), 
          "//*[@numFound='1']");
    }
    
    for (int i = 0; i < 9; i++) {
      String num1 = (i+1) + "." + (i+1);
      String num2 = ((i+1)%9 + 1) + "." + ((i+1)%9 + 1);
      assertQ(req("q", field + ":" + num1 + " OR " + field + ":" + num2), "//*[@numFound='2']");
    }
    
    clearIndex();
    assertU(commit());
    for (int i = 0; i < atLeast(10); i++) {
      float rand = random().nextFloat() * 10;
      assertU(adoc("id", "random_number ", field, String.valueOf(rand))); //always the same id to override
      assertU(commit());
      assertQ(req("q", field + ":" + rand, "fl", "id, " + field), 
          "//*[@numFound='1']");
    }
    clearIndex();
    assertU(commit());
  }
  
  private void doTestPointFieldSort(String field, String dvField, String[] arr) throws Exception {
    assert arr != null && arr.length == 10;
    for (int i = arr.length-1; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), dvField, String.valueOf(arr[i]), field, String.valueOf(arr[i])));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id", "sort", dvField + " desc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/str[@name='id'][.='9']",
        "//result/doc[2]/str[@name='id'][.='8']",
        "//result/doc[3]/str[@name='id'][.='7']",
        "//result/doc[10]/str[@name='id'][.='0']");
    
    assertFalse(h.getCore().getLatestSchema().getField(field).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof PointField);
    assertQEx("Expecting Exception", 
        "can not sort on a PointField without doc values: " + field, 
        req("q", "*:*", "fl", "id", "sort", field + " desc"), 
        SolrException.ErrorCode.BAD_REQUEST);
    
    //TODO: sort missing
  }
  
  private void doTestFloatPointFieldRangeQuery(String fieldName, String type, boolean testDouble) throws Exception {
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, String.valueOf(i)));
    }
    assertU(commit());
    assertQ(req("q", fieldName + ":[0 TO 3]", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='4']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2.0']",
        "//result/doc[4]/" + type + "[@name='" + fieldName + "'][.='3.0']");
    
    assertQ(req("q", fieldName + ":{0 TO 3]", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='2.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='3.0']");
    
    assertQ(req("q", fieldName + ":[0 TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2.0']");
    
    assertQ(req("q", fieldName + ":{0 TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='2']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='2.0']");
    
    assertQ(req("q", fieldName + ":{0 TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='9']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']");
    
    assertQ(req("q", fieldName + ":{* TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2.0']");
    
    assertQ(req("q", fieldName + ":[* TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2.0']");
    
    assertQ(req("q", fieldName + ":[* TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[10]/" + type + "[@name='" + fieldName + "'][.='9.0']");
    
    assertQ(req("q", fieldName + ":[0.9 TO 1.01]", "fl", "id, " + fieldName), 
        "//*[@numFound='1']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']");
    
    assertQ(req("q", fieldName + ":{0.9 TO 1.01}", "fl", "id, " + fieldName), 
        "//*[@numFound='1']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']");
    
    clearIndex();
    assertU(commit());
    
    String[] arr;
    if (testDouble) {
      arr = getRandomStringArrayWithDoubles(10, true);
    } else {
      arr = getRandomStringArrayWithFloats(10, true);
    }
    for (int i = 0; i < arr.length; i++) {
      assertU(adoc("id", String.valueOf(i), fieldName, arr[i]));
    }
    assertU(commit());
    for (int i = 0; i < arr.length; i++) {
      assertQ(req("q", fieldName + ":[" + arr[0] + " TO " + arr[i] + "]", "fl", "id, " + fieldName), 
          "//*[@numFound='" + (i + 1) + "']");
      assertQ(req("q", fieldName + ":{" + arr[0] + " TO " + arr[i] + "}", "fl", "id, " + fieldName), 
          "//*[@numFound='" + (Math.max(0,  i-1)) + "']");
    }
  }
  
  private void doTestFloatPointFieldRangeFacet(String docValuesField, String nonDocValuesField) throws Exception {
    
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, String.format(Locale.ROOT, "%f", (float)i*1.1), nonDocValuesField, String.format(Locale.ROOT, "%f", (float)i*1.1)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
    
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2", "facet.range.method", "filter"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
    
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
  }
  
  private void doTestFloatPointFunctionQuery(String dvFieldName, String nonDvFieldName, String type) throws Exception {
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), dvFieldName, String.format(Locale.ROOT, "%f", (float)i*1.1), nonDvFieldName, String.format(Locale.ROOT, "%f", (float)i*1.1)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "sort", "product(-1," + dvFieldName + ") asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='" + dvFieldName + "'][.='9.9']",
        "//result/doc[2]/" + type + "[@name='" + dvFieldName + "'][.='8.8']",
        "//result/doc[3]/" + type + "[@name='" + dvFieldName + "'][.='7.7']",
        "//result/doc[10]/" + type + "[@name='" + dvFieldName + "'][.='0.0']");
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName + ", product(-1," + dvFieldName + ")", "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/float[@name='product(-1," + dvFieldName + ")'][.='-0.0']",
        "//result/doc[2]/float[@name='product(-1," + dvFieldName + ")'][.='-1.1']",
        "//result/doc[3]/float[@name='product(-1," + dvFieldName + ")'][.='-2.2']",
        "//result/doc[10]/float[@name='product(-1," + dvFieldName + ")'][.='-9.9']");
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName + ", field(" + dvFieldName + ")", "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='field(" + dvFieldName + ")'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='field(" + dvFieldName + ")'][.='1.1']",
        "//result/doc[3]/" + type + "[@name='field(" + dvFieldName + ")'][.='2.2']",
        "//result/doc[10]/" + type + "[@name='field(" + dvFieldName + ")'][.='9.9']");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDvFieldName).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDvFieldName).getType() instanceof PointField);

    assertQEx("Expecting Exception", 
        "sort param could not be parsed as a query", 
        req("q", "*:*", "fl", "id, " + nonDvFieldName, "sort", "product(-1," + nonDvFieldName + ") asc"), 
        SolrException.ErrorCode.BAD_REQUEST);
  }
  
  private void doTestSetQueries(String fieldName, String[] values, boolean multiValued) {
    for (int i = 0; i < values.length; i++) {
      assertU(adoc("id", String.valueOf(i), fieldName, values[i]));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).getType() instanceof PointField);
    
    for (int i = 0; i < values.length; i++) {
      assertQ(req("q", "{!term f='" + fieldName + "'}" + values[i], "fl", "id," + fieldName), 
          "//*[@numFound='1']");
    }
    
    for (int i = 0; i < values.length; i++) {
      assertQ(req("q", "{!terms f='" + fieldName + "'}" + values[i] + "," + values[(i + 1)%values.length], "fl", "id," + fieldName), 
          "//*[@numFound='2']");
    }
    
    if (multiValued) {
      clearIndex();
      assertU(commit());
      for (int i = 0; i < values.length; i++) {
        assertU(adoc("id", String.valueOf(i), fieldName, values[i], fieldName, values[(i+1)%values.length]));
      }
      assertU(commit());
      for (int i = 0; i < values.length; i++) {
        assertQ(req("q", "{!term f='" + fieldName + "'}" + values[i], "fl", "id," + fieldName), 
            "//*[@numFound='2']");
      }
      
      for (int i = 0; i < values.length; i++) {
        assertQ(req("q", "{!terms f='" + fieldName + "'}" + values[i] + "," + values[(i + 1)%values.length], "fl", "id," + fieldName), 
            "//*[@numFound='3']");
      }
    }
  }
  
  private void doTestDoublePointFieldMultiValuedRangeFacet(String docValuesField, String nonDocValuesField) throws Exception {
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, String.valueOf(i), docValuesField, String.valueOf(i + 10), 
          nonDocValuesField, String.valueOf(i), nonDocValuesField, String.valueOf(i + 10)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='10.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='12.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='14.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='16.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='18.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='10.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='12.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='14.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='16.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='18.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "0", "facet.range.end", "20", "facet.range.gap", "100"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='10']");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2", "facet.range.method", "filter"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='10.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='12.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='14.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='16.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='18.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
    
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='10.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='12.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='14.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='16.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='18.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
  }
  
  private void doTestIntPointFieldMultiValuedRangeFacet(String docValuesField, String nonDocValuesField) throws Exception {
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, String.valueOf(i), docValuesField, String.valueOf(i + 10), 
          nonDocValuesField, String.valueOf(i), nonDocValuesField, String.valueOf(i + 10)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='10'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='12'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='14'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='16'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='18'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='10'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='12'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='14'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='16'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='18'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "0", "facet.range.end", "20", "facet.range.gap", "100"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0'][.='10']");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2", "facet.range.method", "filter"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='10'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='12'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='14'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='16'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='18'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10'][.='0']");
    
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='10'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='12'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='14'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='16'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='18'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10'][.='0']");
  }


  private void doTestDatePointFieldExactQuery(String field, String baseDate) throws Exception {
    for (int i=0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), field, String.format(Locale.ROOT, "%s+%dMINUTES", baseDate, i+1)));
    }
    assertU(commit());
    for (int i = 0; i < 10; i++) {
      String date = String.format(Locale.ROOT, "%s+%dMINUTES", baseDate, i+1);
      assertQ(req("q", field + ":\""+date+"\"", "fl", "id, " + field),
          "//*[@numFound='1']");
    }

    for (int i = 0; i < 10; i++) {
      String date1 = String.format(Locale.ROOT, "%s+%dMINUTES", baseDate, i+1);
      String date2 = String.format(Locale.ROOT, "%s+%dMINUTES", baseDate, ((i+1)%10 + 1));
      assertQ(req("q", field + ":\"" + date1 + "\""
          + " OR " + field + ":\"" + date2 + "\""), "//*[@numFound='2']");
    }

    clearIndex();
    assertU(commit());
  }

  private void doTestDatePointFieldRangeQuery(String fieldName) throws Exception {
    String baseDate = "1995-12-31T10:59:59Z";
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, String.format(Locale.ROOT, "%s+%dHOURS", baseDate, i)));
    }
    assertU(commit());
    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "[%s+0HOURS TO %s+3HOURS]", baseDate, baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[3]/date[@name='" + fieldName + "'][.='1995-12-31T12:59:59Z']",
        "//result/doc[4]/date[@name='" + fieldName + "'][.='1995-12-31T13:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "{%s+0HOURS TO %s+3HOURS]", baseDate, baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T12:59:59Z']",
        "//result/doc[3]/date[@name='" + fieldName + "'][.='1995-12-31T13:59:59Z']");

    assertQ(req("q", fieldName + ":"+ String.format(Locale.ROOT, "[%s+0HOURS TO %s+3HOURS}",baseDate,baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[3]/date[@name='" + fieldName + "'][.='1995-12-31T12:59:59Z']");

    assertQ(req("q", fieldName + ":"+ String.format(Locale.ROOT, "{%s+0HOURS TO %s+3HOURS}",baseDate,baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T12:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "{%s+0HOURS TO *}",baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='9']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "{* TO %s+3HOURS}",baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "[* TO %s+3HOURS}",baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']");

    assertQ(req("q", fieldName + ":[* TO *}", "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']",
        "//result/doc[10]/date[@name='" + fieldName + "'][.='1995-12-31T19:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "[%s+0HOURS TO %s+1HOURS]",baseDate,baseDate)
                + " OR " + fieldName + ":" + String.format(Locale.ROOT, "[%s+8HOURS TO %s+9HOURS]",baseDate,baseDate) ,
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[3]/date[@name='" + fieldName + "'][.='1995-12-31T18:59:59Z']",
        "//result/doc[4]/date[@name='" + fieldName + "'][.='1995-12-31T19:59:59Z']");

    assertQ(req("q", fieldName + ":"+String.format(Locale.ROOT, "[%s+0HOURS TO %s+1HOURS]",baseDate,baseDate)
            +" AND " + fieldName + ":"+String.format(Locale.ROOT, "[%s+1HOURS TO %s+2HOURS]",baseDate,baseDate) , "fl", "id, " + fieldName),
        "//*[@numFound='1']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']");

    assertQ(req("q", fieldName + ":"+String.format(Locale.ROOT, "[%s+0HOURS TO %s+1HOURS]",baseDate,baseDate)
            +" AND NOT " + fieldName + ":"+String.format(Locale.ROOT, "[%s+1HOURS TO %s+2HOURS]",baseDate,baseDate) , "fl", "id, " + fieldName),
        "//*[@numFound='1']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']");

    clearIndex();
    assertU(commit());
  }

  private void doTestDatePointFieldRangeFacet(String docValuesField, String nonDocValuesField) throws Exception {
    String baseDate = "1995-01-10T10:59:59Z";
    for (int i = 0; i < 10; i++) {
      String date = String.format(Locale.ROOT, "%s+%dDAYS", baseDate, i);
      assertU(adoc("id", String.valueOf(i), docValuesField, date, nonDocValuesField, date));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+10DAYS", "facet.range.gap", "+2DAYS"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-08T10:59:59Z'][.='0']");

    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+10DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "dv"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-08T10:59:59Z'][.='0']");

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+10DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "filter"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-08T10:59:59Z'][.='0']");

    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+10DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "dv"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-08T10:59:59Z'][.='0']");
  }

  private void doTestDatePointFieldMultiValuedRangeFacet(String docValuesField, String nonDocValuesField) throws Exception {
    String baseDate = "1995-01-10T10:59:59Z";
    for (int i = 0; i < 10; i++) {
      String date1 = String.format(Locale.ROOT, "%s+%dDAYS", baseDate, i);
      String date2 = String.format(Locale.ROOT, "%s+%dDAYS", baseDate, i+10);
      assertU(adoc("id", String.valueOf(i), docValuesField, date1, docValuesField, date2,
          nonDocValuesField, date1, nonDocValuesField, date2));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+2DAYS"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-20T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-22T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-24T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-26T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-28T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1994-12-31T10:59:59Z'][.='0']");

    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "dv"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-20T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-22T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-24T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-26T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-28T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1994-12-31T10:59:59Z'][.='0']");

    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+100DAYS"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='10']");

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "filter"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-20T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-22T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-24T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-26T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-28T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1994-12-31T10:59:59Z'][.='0']");

    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "dv"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-20T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-22T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-24T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-26T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-28T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1994-12-31T10:59:59Z'][.='0']");
  }

  private void doTestDatePointFunctionQuery(String dvFieldName, String nonDvFieldName, String type) throws Exception {
    String baseDate = "1995-01-10T10:59:10Z";
    for (int i = 9; i >= 0; i--) {
      String date = String.format(Locale.ROOT, "%s+%dSECONDS", baseDate, i+1);
      assertU(adoc("id", String.valueOf(i), dvFieldName, date, nonDvFieldName, date));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "sort", "product(-1,ms(" + dvFieldName + ")) asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='" + dvFieldName + "'][.='1995-01-10T10:59:20Z']",
        "//result/doc[2]/" + type + "[@name='" + dvFieldName + "'][.='1995-01-10T10:59:19Z']",
        "//result/doc[3]/" + type + "[@name='" + dvFieldName + "'][.='1995-01-10T10:59:18Z']",
        "//result/doc[10]/" + type + "[@name='" + dvFieldName + "'][.='1995-01-10T10:59:11Z']");

    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName + ", ms(" + dvFieldName + ","+baseDate+")", "sort", "id asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/float[@name='ms(" + dvFieldName + "," + baseDate + ")'][.='1000.0']",
        "//result/doc[2]/float[@name='ms(" + dvFieldName + "," + baseDate + ")'][.='2000.0']",
        "//result/doc[3]/float[@name='ms(" + dvFieldName + "," + baseDate + ")'][.='3000.0']",
        "//result/doc[10]/float[@name='ms(" + dvFieldName + "," + baseDate + ")'][.='10000.0']");

    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName + ", field(" + dvFieldName + ")", "sort", "id asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='field(" + dvFieldName + ")'][.='1995-01-10T10:59:11Z']",
        "//result/doc[2]/" + type + "[@name='field(" + dvFieldName + ")'][.='1995-01-10T10:59:12Z']",
        "//result/doc[3]/" + type + "[@name='field(" + dvFieldName + ")'][.='1995-01-10T10:59:13Z']",
        "//result/doc[10]/" + type + "[@name='field(" + dvFieldName + ")'][.='1995-01-10T10:59:20Z']");

    assertFalse(h.getCore().getLatestSchema().getField(nonDvFieldName).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDvFieldName).getType() instanceof PointField);

    assertQEx("Expecting Exception",
        "sort param could not be parsed as a query",
        req("q", "*:*", "fl", "id, " + nonDvFieldName, "sort", "product(-1," + nonDvFieldName + ") asc"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  private void testDatePointStats(String field, String dvField, String[] dates) {
    for (int i = 0; i < dates.length; i++) {
      assertU(adoc("id", String.valueOf(i), dvField, dates[i], field, dates[i]));
    }
    assertU(adoc("id", String.valueOf(dates.length)));
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id, " + dvField, "stats", "true", "stats.field", dvField),
        "//*[@numFound='11']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/date[@name='min'][.='" + dates[0] + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/date[@name='max'][.='" + dates[dates.length-1] + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/long[@name='count'][.='" + dates.length + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/long[@name='missing'][.='1']");

    assertFalse(h.getCore().getLatestSchema().getField(field).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof PointField);
    assertQEx("Expecting Exception",
        "Can't calculate stats on a PointField without docValues",
        req("q", "*:*", "fl", "id, " + field, "stats", "true", "stats.field", field),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  private void testDatePointFieldsAtomicUpdates(String field, String type) throws Exception {
    String date = "1995-01-10T10:59:10Z";
    assertU(adoc(sdoc("id", "1", field, date)));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='"+date+"']");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", date+"+2DAYS"))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='1995-01-12T10:59:10Z']");
  }

  private void testMultiValuedDatePointFieldsAtomicUpdates(String field, String type) throws Exception {
    String date1 = "1995-01-10T10:59:10Z";
    String date2 = "1995-01-11T10:59:10Z";
    String date3 = "1995-01-12T10:59:10Z";
    assertU(adoc(sdoc("id", "1", field, date1)));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='"+date1+"']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("add", date2))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='"+date1+"']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='"+date2+"']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=2");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("remove", date1))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='"+date2+"']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", ImmutableList.of(date1, date2, date3)))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='"+date1+"']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='"+date2+"']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='"+date3+"']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=3");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("removeregex", ".*"))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=0");

  }


}
