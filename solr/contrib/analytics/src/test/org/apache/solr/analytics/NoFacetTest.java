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
package org.apache.solr.analytics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

public class NoFacetTest extends SolrAnalyticsTestCase {

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
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("single", new ETP("count(long_l)", 10L));
    expressions.put("multi", new ETP("count(string_sm)", 30L));

    testExpressions(expressions);
  }

  @Test
  public void docCountTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("single", new ETP("doc_count(date_dt)", 13L));
    expressions.put("multi", new ETP("doc_count(float_fm)", 16L));

    testExpressions(expressions);
  }

  @Test
  public void missingTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("single", new ETP("missing(string_s)", 5L));
    expressions.put("multi", new ETP("missing(date_dtm)", 7L));

    testExpressions(expressions);
  }

  @Test
  public void uniqueTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("int", new ETP("unique(int_i)", 6L));
    expressions.put("longs", new ETP("unique(long_lm)", 2L));
    expressions.put("float", new ETP("unique(float_f)", 5L));
    expressions.put("doubles", new ETP("unique(double_dm)", 8L));
    expressions.put("dates", new ETP("unique(date_dt)", 2L));
    expressions.put("strings", new ETP("unique(string_sm)", 6L));

    testExpressions(expressions);
  }

  @Test
  public void minTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("int", new ETP("min(int_i)", 1));
    expressions.put("longs", new ETP("min(long_lm)", 1L));
    expressions.put("float", new ETP("min(float_f)", 1.0F));
    expressions.put("doubles", new ETP("min(double_dm)", 1.0));
    expressions.put("dates", new ETP("min(date_dt)", "1801-12-31T23:59:59Z"));
    expressions.put("strings", new ETP("min(string_sm)", "str1"));

    testExpressions(expressions);
  }

  @Test
  public void maxTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("int", new ETP("max(int_i)", 6));
    expressions.put("longs", new ETP("max(long_lm)", 11L));
    expressions.put("float", new ETP("max(float_f)", 5.0F));
    expressions.put("doubles", new ETP("max(double_dm)", 14.0));
    expressions.put("dates", new ETP("max(date_dt)", "1802-12-31T23:59:59Z"));
    expressions.put("strings", new ETP("max(string_sm)", "str3_second"));

    testExpressions(expressions);
  }

  @Test
  public void sumTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("single", new ETP("sum(int_i)", 57.0));
    expressions.put("multi", new ETP("sum(long_lm)", 120.0));

    testExpressions(expressions);
  }

  @Test
  public void meanTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("single", new ETP("mean(int_i)", 3.3529411764));
    expressions.put("multi", new ETP("mean(long_lm)", 6.0));

    testExpressions(expressions);
  }

  @Test
  public void weightedMeanTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("single", new ETP("wmean(int_i, long_l)", 3.33333333333));
    expressions.put("multi", new ETP("wmean(double_d, float_f)", 2.470588235));

    testExpressions(expressions);
  }

  @Test
  public void sumOfSquaresTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("single", new ETP("sumofsquares(int_i)", 237.0));
    expressions.put("multi", new ETP("sumofsquares(long_lm)", 1220.0));

    testExpressions(expressions);
  }

  @Test
  public void varianceTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("single", new ETP("variance(int_i)", 2.6989619377162));
    expressions.put("multi", new ETP("variance(long_lm)", 25.0));

    testExpressions(expressions);
  }

  @Test
  public void standardDeviationTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("single", new ETP("stddev(int_i)", 1.6428517698551));
    expressions.put("multi", new ETP("stddev(long_lm)", 5.0));

    testExpressions(expressions);
  }

  @Test
  public void medianTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("int", new ETP("median(int_i)", 3.0));
    expressions.put("longs", new ETP("median(long_lm)", 6.0));
    expressions.put("float", new ETP("median(float_f)", 3.0));
    expressions.put("doubles", new ETP("median(double_dm)", 7.5));
    expressions.put("dates", new ETP("median(date_dt)", "1801-12-31T23:59:59Z"));

    testExpressions(expressions);
  }

  @Test
  public void percentileTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("int", new ETP("percentile(20,int_i)", 2));
    expressions.put("longs", new ETP("percentile(80,long_lm)", 11L));
    expressions.put("float", new ETP("percentile(40,float_f)", 2.0F));
    expressions.put("doubles", new ETP("percentile(50,double_dm)", 11.0));
    expressions.put("dates", new ETP("percentile(0,date_dt)", "1801-12-31T23:59:59Z"));
    expressions.put("strings", new ETP("percentile(99.99,string_sm)", "str3_second"));

    testExpressions(expressions);
  }

  @Test
  public void ordinalTest() throws Exception {
    Map<String, ETP> expressions = new HashMap<>();
    expressions.put("int", new ETP("ordinal(15,int_i)", 5));
    expressions.put("longs", new ETP("ordinal(11,long_lm)", 11L));
    expressions.put("float", new ETP("ordinal(-5,float_f)", 4.0F));
    expressions.put("doubles", new ETP("ordinal(1,double_dm)", 1.0));
    expressions.put("dates", new ETP("ordinal(-1,date_dt)", "1802-12-31T23:59:59Z"));
    expressions.put("strings", new ETP("ordinal(6,string_sm)", "str1_second"));

    testExpressions(expressions);
  }
}
