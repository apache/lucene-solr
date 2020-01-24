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

public class RangeFacetTest extends SolrAnalyticsFacetTestCase {

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
  public void intRangeHardEndTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("mean", "mean(float_f)");
    expressions.put("count", "count(string_sm)");

    // Hard end on
    addFacet("hard_end_on", "{ 'type':'range', "
        + " 'field': 'int_im', "
        + " 'start': '2', "
        + " 'end' : '13', "
        + " 'gaps' : ['2', '5', '3'], "
        + " 'hardend' : true }");

    addFacetValue("[2 TO 4)");
    addFacetResult("mean", 3.5);
    addFacetResult("count", 10L);

    addFacetValue("[4 TO 9)");
    addFacetResult("mean", 16.0/5.0);
    addFacetResult("count", 12L);

    addFacetValue("[9 TO 12)");
    addFacetResult("mean", 2.0);
    addFacetResult("count", 4L);

    addFacetValue("[12 TO 13)");
    addFacetResult("mean", 3.0);
    addFacetResult("count", 4L);

    // Hard end off
    addFacet("hard_end_off", "{ 'type':'range', "
        + " 'field': 'int_im', "
        + " 'start': '2', "
        + " 'end' : '13', "
        + " 'gaps' : ['2', '5', '3'], "
        + " 'hardend' : false }");

    addFacetValue("[2 TO 4)");
    addFacetResult("mean", 3.5);
    addFacetResult("count", 10L);

    addFacetValue("[4 TO 9)");
    addFacetResult("mean", 16.0/5.0);
    addFacetResult("count", 12L);

    addFacetValue("[9 TO 12)");
    addFacetResult("mean", 2.0);
    addFacetResult("count", 4L);

    addFacetValue("[12 TO 15)");
    addFacetResult("mean", 30.0/8.0);
    addFacetResult("count", 14L);

    testGrouping(expressions);
  }

  @Test
  public void doubleOthersTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("mean", "mean(float_f)");
    expressions.put("count", "count(string_sm)");

    addFacet("before_between", "{ 'type':'range', "
        + " 'field': 'double_d', "
        + " 'start': '2', "
        + " 'end' : '4', "
        + " 'gaps' : ['1'], "
        + " 'others' : ['before', 'between'] }");

    addFacetValue("[2.0 TO 3.0)");
    addFacetResult("mean", 8.0/3.0);
    addFacetResult("count", 6L);

    addFacetValue("[3.0 TO 4.0)");
    addFacetResult("mean", 2.0);
    addFacetResult("count", 6L);

    addFacetValue("(* TO 2.0)");
    addFacetResult("mean", 10.0/3.0);
    addFacetResult("count", 6L);

    addFacetValue("[2.0 TO 4.0)");
    addFacetResult("mean", 14.0/6.0);
    addFacetResult("count", 12L);

    addFacet("after", "{ 'type':'range', "
        + " 'field': 'double_d', "
        + " 'start': '2', "
        + " 'end' : '4', "
        + " 'gaps' : ['1'], "
        + " 'others' : ['after'] }");

    addFacetValue("[2.0 TO 3.0)");
    addFacetResult("mean", 8.0/3.0);
    addFacetResult("count", 6L);

    addFacetValue("[3.0 TO 4.0)");
    addFacetResult("mean", 2.0);
    addFacetResult("count", 6L);

    addFacetValue("[4.0 TO *)");
    addFacetResult("mean", 2.5);
    addFacetResult("count", 6L);

    testGrouping(expressions);
  }

  @Test
  public void dateIncludeTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("mean", "mean(float_f)");
    expressions.put("count", "count(string_sm)");

    addFacet("lower_upper", "{ 'type':'range', "
        + " 'field': 'date_dt', "
        + " 'start': '1801-12-31T23:59:59Z', "
        + " 'end' : '1803-12-31T23:59:59Z', "
        + " 'gaps' : ['+1YEAR'], "
        + " 'include' : ['lower', 'upper'] }");

    addFacetValue("[1801-12-31T23:59:59Z TO 1802-12-31T23:59:59Z]");
    addFacetResult("mean", 37.0/13.0);
    addFacetResult("count", 20L);

    addFacetValue("[1802-12-31T23:59:59Z TO 1803-12-31T23:59:59Z]");
    addFacetResult("mean", 3.5);
    addFacetResult("count", 10L);

    testGrouping(expressions);

    addFacet("lower_upper", "{ 'type':'range', "
        + " 'field': 'date_dt', "
        + " 'start': '1801-12-31T23:59:59Z', "
        + " 'end' : '1803-12-31T23:59:59Z', "
        + " 'gaps' : ['+1YEAR'], "
        + " 'include' : ['lower','edge'] }");

    addFacetValue("[1801-12-31T23:59:59Z TO 1802-12-31T23:59:59Z)");
    addFacetResult("mean", 16.0/7.0);
    addFacetResult("count", 10L);

    addFacetValue("[1802-12-31T23:59:59Z TO 1803-12-31T23:59:59Z]");
    addFacetResult("mean", 3.5);
    addFacetResult("count", 10L);

    testGrouping(expressions);
  }

  @Test
  public void floatIncludeOuterTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("mean", "mean(int_i)");
    expressions.put("count", "count(string_sm)");

    addFacet("include_outer", "{ 'type':'range', "
        + " 'field': 'float_f', "
        + " 'start': '1', "
        + " 'end' : '5', "
        + " 'gaps' : ['2'], "
        + " 'include' : ['outer'], "
        + " 'others' : ['all'] }");

    addFacetValue("(1.0 TO 3.0)");
    addFacetResult("mean", 1.5);
    addFacetResult("count", 4L);

    addFacetValue("(3.0 TO 5.0)");
    addFacetResult("mean", 3.0);
    addFacetResult("count", 2L);

    addFacetValue("(* TO 1.0]");
    addFacetResult("mean", 4.0);
    addFacetResult("count", 8L);

    addFacetValue("[5.0 TO *)");
    addFacetResult("mean", 4.0);
    addFacetResult("count", 6L);

    addFacetValue("(1.0 TO 5.0)");
    addFacetResult("mean", 18.0/8.0);
    addFacetResult("count", 12L);

    testGrouping(expressions);
  }

  @Test
  public void floatIncludeAllTest() throws Exception {
    Map<String, String> expressions = new HashMap<>();
    expressions.put("mean", "mean(int_i)");
    expressions.put("count", "count(string_sm)");

    addFacet("include_all", "{ 'type':'range', "
        + " 'field': 'float_f', "
        + " 'start': '1', "
        + " 'end' : '5', "
        + " 'gaps' : ['2'], "
        + " 'include' : ['all'], "
        + " 'others' : ['all'] }");

    addFacetValue("[1.0 TO 3.0]");
    addFacetResult("mean", 21.0/8.0);
    addFacetResult("count", 18L);

    addFacetValue("[3.0 TO 5.0]");
    addFacetResult("mean", 3.0);
    addFacetResult("count", 14L);

    addFacetValue("(* TO 1.0]");
    addFacetResult("mean", 4.0);
    addFacetResult("count", 8L);

    addFacetValue("[5.0 TO *)");
    addFacetResult("mean", 4.0);
    addFacetResult("count", 6L);

    addFacetValue("[1.0 TO 5.0]");
    addFacetResult("mean", 3.0);
    addFacetResult("count", 26L);

    testGrouping(expressions);
  }
}
