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
package org.apache.solr.analytics.facet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

public class ValueFacetTest extends SolrAnalyticsFacetTestCase {

  @BeforeClass
  public static void populate() throws Exception {
    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      String dt = (1800+j%DATE) + "-12-31T23:59:59Z";
      String dtm = (1800+j%DATE + 10) + "-12-31T23:59:59Z";
      String s = "str" + (j%STRING);
      List<String> fields = new ArrayList<>();
      fields.add("id"); fields.add("1000"+j);

      if ( i != 0 ) {
        fields.add("int_i"); fields.add("" + i);
        fields.add("int_im"); fields.add("" + i);
        fields.add("int_im"); fields.add("" + (i+10));
      }

      if ( l != 0l ) {
        fields.add("long_l"); fields.add("" + l);
        fields.add("long_lm"); fields.add("" + l);
        fields.add("long_lm"); fields.add("" + (l+10));
      }

      if ( f != 0.0f ) {
        fields.add("float_f"); fields.add("" + f);
        fields.add("float_fm"); fields.add("" + f);
        fields.add("float_fm"); fields.add("" + (f+10));
      }

      if ( d != 0.0d ) {
        fields.add("double_d"); fields.add("" + d);
        fields.add("double_dm"); fields.add("" + d);
        fields.add("double_dm"); fields.add("" + (d+10));
      }

      if ( (j%DATE) != 0 ) {
        fields.add("date_dt"); fields.add(dt);
        fields.add("date_dtm"); fields.add(dt);
        fields.add("date_dtm"); fields.add(dtm);
      }

      if ( (j%STRING) != 0 ) {
        fields.add("string_s"); fields.add(s);
        fields.add("string_sm"); fields.add(s);
        fields.add("string_sm"); fields.add(s + "_second");
      }

      addDoc(fields);
    }
    commitDocs();
  }

  static public final int INT = 7;
  static public final int LONG = 2;
  static public final int FLOAT = 6;
  static public final int DOUBLE = 5;
  static public final int DATE = 3;
  static public final int STRING = 4;
  static public final int NUM_LOOPS = 20;

  @Test
  public void countTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("single", "count(float_f)");
    expressions.put("multi", "count(string_sm)");

    // Value Facet "with_missing"
    addFacet("with_missing", "{ 'type':'value', 'expression':'fill_missing(long_l,\\'No Long\\')' }");

    addFacetValue("1");
    addFacetResult("single", 10L);
    addFacetResult("multi", 20L);

    addFacetValue("No Long");
    addFacetResult("single", 6L);
    addFacetResult("multi", 10L);

    testGrouping(expressions, "single", true);
  }

  @Test
  public void docCountTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("single", "doc_count(date_dt)");
    expressions.put("multi", "doc_count(float_fm)");

    // Value Facet "with_missing"
    addFacet("with_missing", "{ 'type':'value', 'expression':'fill_missing(string_sm,\\'No Values\\')' }");

    addFacetValue("str1");
    addFacetResult("single", 4L);
    addFacetResult("multi", 5L);

    addFacetValue("str1_second");
    addFacetResult("single", 4L);
    addFacetResult("multi", 5L);

    addFacetValue("str2");
    addFacetResult("single", 3L);
    addFacetResult("multi", 3L);

    addFacetValue("str2_second");
    addFacetResult("single", 3L);
    addFacetResult("multi", 3L);

    addFacetValue("str3");
    addFacetResult("single", 3L);
    addFacetResult("multi", 5L);

    addFacetValue("str3_second");
    addFacetResult("single", 3L);
    addFacetResult("multi", 5L);

    addFacetValue("No Values");
    addFacetResult("single", 3L);
    addFacetResult("multi", 3L);

    // Value Facet "no_missing"
    addFacet("no_missing", "{ 'type':'value', 'expression':'long_l' }");

    addFacetValue("1");
    addFacetResult("single", 7L);
    addFacetResult("multi", 10L);

    testGrouping(expressions, false);
  }

  @Test
  public void missingTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("single", "missing(date_dt)");
    expressions.put("multi", "missing(float_fm)");

    // Value Facet "with_missing"
    addFacet("with_missing", "{ 'type':'value', 'expression':'fill_missing(string_sm,\\'No Values\\')' }");

    addFacetValue("str1");
    addFacetResult("single", 1L);
    addFacetResult("multi", 0L);

    addFacetValue("str1_second");
    addFacetResult("single", 1L);
    addFacetResult("multi", 0L);

    addFacetValue("str2");
    addFacetResult("single", 2L);
    addFacetResult("multi", 2L);

    addFacetValue("str2_second");
    addFacetResult("single", 2L);
    addFacetResult("multi", 2L);

    addFacetValue("str3");
    addFacetResult("single", 2L);
    addFacetResult("multi", 0L);

    addFacetValue("str3_second");
    addFacetResult("single", 2L);
    addFacetResult("multi", 0L);

    addFacetValue("No Values");
    addFacetResult("single", 2L);
    addFacetResult("multi", 2L);

    // Value Facet "no_missing"
    addFacet("no_missing", "{ 'type':'value', 'expression':'long_l' }");

    addFacetValue("1");
    addFacetResult("single", 3L);
    addFacetResult("multi", 0L);

    testGrouping(expressions, false);
  }

  @Test
  public void uniqueTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("int", "unique(int_i)");
    expressions.put("longs", "unique(long_lm)");
    expressions.put("float", "unique(float_f)");
    expressions.put("doubles", "unique(double_dm)");
    expressions.put("dates", "unique(date_dtm)");
    expressions.put("string", "unique(string_s)");

    // Value Facet "without_missing"
    addFacet("without_missing", "{ 'type':'value', 'expression':'date_dt' }");

    addFacetValue("1801-12-31T23:59:59Z");
    addFacetResult("int", 6L);
    addFacetResult("longs", 2L);
    addFacetResult("float", 2L);
    addFacetResult("doubles", 8L);
    addFacetResult("dates", 2L);
    addFacetResult("string", 3L);

    addFacetValue("1802-12-31T23:59:59Z");
    addFacetResult("int", 5L);
    addFacetResult("longs", 2L);
    addFacetResult("float", 2L);
    addFacetResult("doubles", 8L);
    addFacetResult("dates", 2L);
    addFacetResult("string", 3L);

    testGrouping(expressions, false);
  }

  @Test
  public void minTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("int", "min(int_i)");
    expressions.put("longs", "min(long_lm)");
    expressions.put("float", "min(float_f)");
    expressions.put("doubles", "min(double_dm)");
    expressions.put("dates", "min(date_dtm)");
    expressions.put("string", "min(string_s)");

    // Value Facet "without_missing"
    addFacet("without_missing", "{ 'type':'value', 'expression':'date_dt' }");

    addFacetValue("1801-12-31T23:59:59Z");
    addFacetResult("int", 1);
    addFacetResult("longs", 1L);
    addFacetResult("float", 1.0F);
    addFacetResult("doubles", 1.0);
    addFacetResult("dates", "1801-12-31T23:59:59Z");
    addFacetResult("string", "str1");

    addFacetValue("1802-12-31T23:59:59Z");
    addFacetResult("int", 1);
    addFacetResult("longs", 1L);
    addFacetResult("float", 2.0F);
    addFacetResult("doubles", 1.0);
    addFacetResult("dates", "1802-12-31T23:59:59Z");
    addFacetResult("string", "str1");

    testGrouping(expressions, false);
  }

  @Test
  public void maxTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("int", "max(int_i)");
    expressions.put("longs", "max(long_lm)");
    expressions.put("float", "max(float_f)");
    expressions.put("doubles", "max(double_dm)");
    expressions.put("dates", "max(date_dtm)");
    expressions.put("string", "max(string_s)");

    // Value Facet "without_missing"
    addFacet("without_missing", "{ 'type':'value', 'expression':'date_dt' }");

    addFacetValue("1801-12-31T23:59:59Z");
    addFacetResult("int", 6);
    addFacetResult("longs", 11L);
    addFacetResult("float", 4.0F);
    addFacetResult("doubles", 14.0);
    addFacetResult("dates", "1811-12-31T23:59:59Z");
    addFacetResult("string", "str3");

    addFacetValue("1802-12-31T23:59:59Z");
    addFacetResult("int", 5);
    addFacetResult("longs", 11L);
    addFacetResult("float", 5.0F);
    addFacetResult("doubles", 14.0);
    addFacetResult("dates", "1812-12-31T23:59:59Z");
    addFacetResult("string", "str3");

    testGrouping(expressions, false);
  }

  @Test
  public void sumTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("single", "sum(float_f)");
    expressions.put("multi", "sum(double_dm)");

    // Value Facet "without_missing"
    addFacet("without_missing", "{ 'type':'value', 'expression':'date_dt' }");

    addFacetValue("1801-12-31T23:59:59Z");
    addFacetResult("single", 16.0);
    addFacetResult("multi", 90.0);

    addFacetValue("1802-12-31T23:59:59Z");
    addFacetResult("single", 21.0);
    addFacetResult("multi", 74.0);

    testGrouping(expressions, false);
  }

  @Test
  public void meanTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("single", "mean(float_f)");
    expressions.put("multi", "mean(double_dm)");

    // Value Facet "without_missing"
    addFacet("without_missing", "{ 'type':'value', 'expression':'date_dt' }");

    addFacetValue("1801-12-31T23:59:59Z");
    addFacetResult("single", 16.0/7.0);
    addFacetResult("multi", 90.0/12.0);

    addFacetValue("1802-12-31T23:59:59Z");
    addFacetResult("single", 21.0/6.0);
    addFacetResult("multi", 74.0/10.0);

    testGrouping(expressions, "multi", true);
  }

  @Test
  public void medianTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("int", "median(int_i)");
    expressions.put("longs", "median(long_lm)");
    expressions.put("float", "median(float_f)");
    expressions.put("doubles", "median(double_dm)");
    expressions.put("dates", "median(date_dtm)");

    // Value Facet "without_missing"
    addFacet("without_missing", "{ 'type':'value', 'expression':'date_dt' }");

    addFacetValue("1801-12-31T23:59:59Z");
    addFacetResult("int", 3.5);
    addFacetResult("longs", 6.0);
    addFacetResult("float", 1.0);
    addFacetResult("doubles", 7.5);
    addFacetResult("dates", "1806-12-31T23:59:59Z");

    addFacetValue("1802-12-31T23:59:59Z");
    addFacetResult("int", 3.0);
    addFacetResult("longs", 6.0);
    addFacetResult("float", 3.5);
    addFacetResult("doubles", 7.50);
    addFacetResult("dates", "1808-01-01T11:59:59Z");

    testGrouping(expressions, false);
  }

  @Test
  public void percentileTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("int", "percentile(20,int_i)");
    expressions.put("longs", "percentile(80,long_lm)");
    expressions.put("float", "percentile(40,float_f)");
    expressions.put("doubles", "percentile(50,double_dm)");
    expressions.put("dates", "percentile(0,date_dtm)");
    expressions.put("string", "percentile(99.99,string_s)");

    // Value Facet "without_missing"
    addFacet("without_missing", "{ 'type':'value', 'expression':'date_dt' }");

    addFacetValue("1801-12-31T23:59:59Z");
    addFacetResult("int", 2);
    addFacetResult("longs", 11L);
    addFacetResult("float", 1.0F);
    addFacetResult("doubles", 11.0);
    addFacetResult("dates", "1801-12-31T23:59:59Z");
    addFacetResult("string", "str3");

    addFacetValue("1802-12-31T23:59:59Z");
    addFacetResult("int", 2);
    addFacetResult("longs", 11L);
    addFacetResult("float", 2.0F);
    addFacetResult("doubles", 11.0);
    addFacetResult("dates", "1802-12-31T23:59:59Z");
    addFacetResult("string", "str3");

    testGrouping(expressions, "dates", false);
  }

  @Test
  public void ordinalTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("int", "ordinal(3,int_i)");
    expressions.put("longs", "ordinal(-2,long_lm)");
    expressions.put("float", "ordinal(-3,float_f)");
    expressions.put("doubles", "ordinal(1,double_dm)");
    expressions.put("dates", "ordinal(-1,date_dtm)");
    expressions.put("string", "ordinal(4,string_s)");

    // Value Facet "without_missing"
    addFacet("without_missing", "{ 'type':'value', 'expression':'date_dt' }");

    addFacetValue("1801-12-31T23:59:59Z");
    addFacetResult("int", 3);
    addFacetResult("longs", 11L);
    addFacetResult("float", 4.0F);
    addFacetResult("doubles", 1.0);
    addFacetResult("dates", "1811-12-31T23:59:59Z");
    addFacetResult("string", "str3");

    addFacetValue("1802-12-31T23:59:59Z");
    addFacetResult("int", 3);
    addFacetResult("longs", 11L);
    addFacetResult("float", 5.0F);
    addFacetResult("doubles", 1.0);
    addFacetResult("dates", "1812-12-31T23:59:59Z");
    addFacetResult("string", "str2");

    testGrouping(expressions, "string", false);
  }
}
