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
package org.apache.solr.analytics.legacy.expression;


import org.apache.solr.analytics.legacy.LegacyAbstractAnalyticsTest;
import org.apache.solr.analytics.legacy.facet.LegacyAbstractAnalyticsFacetTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class LegacyFunctionTest extends LegacyAbstractAnalyticsTest {
  static String fileName = "functions.txt";

  static public final int INT = 71;
  static public final int LONG = 36;
  static public final int FLOAT = 93;
  static public final int DOUBLE = 49;
  static public final int DATE = 12;
  static public final int STRING = 28;
  static public final int NUM_LOOPS = 100;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml","schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");

    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT+1;
      long l = j%LONG+1;
      float f = j%FLOAT+1;
      double d = j%DOUBLE+1;
      double d0 = j%DOUBLE;
      String dt = (1800+j%DATE) + "-06-30T23:59:59Z";
      String s = "str" + (j%STRING);

      double add_if = (double)i+f;
      double add_ldf = (double)l+d+f;
      double mult_if = (double)i*f;
      double mult_ldf = (double)l*d*f;
      double div_if = (double)i/f;
      double div_ld = (double)l/d;
      double pow_if = Math.pow(i,f);
      double pow_ld = Math.pow(l,d);
      int neg_i = i*-1;
      long neg_l = l*-1;
      String dm_2y = (1802+j%DATE) + "-06-30T23:59:59Z";
      String dm_2m = (1800+j%DATE) + "-08-30T23:59:59Z";
      String concat_first = "this is the first"+s;
      String concat_second = "this is the second"+s;

      assertU(adoc(LegacyAbstractAnalyticsFacetTest.filter("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f,
            "double_dd", "" + d,  "date_dtd", dt, "string_sd", s,
            "add_if_dd", ""+add_if, "add_ldf_dd", ""+add_ldf, "mult_if_dd", ""+mult_if, "mult_ldf_dd", ""+mult_ldf,
            "div_if_dd", ""+div_if, "div_ld_dd", ""+div_ld, "pow_if_dd", ""+pow_if, "pow_ld_dd", ""+pow_ld,
            "neg_id", ""+neg_i, "neg_ld", ""+neg_l, "const_8_dd", "8", "const_10_dd", "10", "dm_2y_dtd", dm_2y, "dm_2m_dtd", dm_2m,
            "const_00_dtd", "1800-06-30T23:59:59Z", "const_04_dtd", "1804-06-30T23:59:59Z", "const_first_sd", "this is the first", "const_second_sd", "this is the second",
            "concat_first_sd", concat_first, "concat_second_sd", concat_second, "miss_dd", ""+d0 )));


      if (usually()) {
        assertU(commit()); // to have several segments
      }
    }

    assertU(commit());

    setResponse(h.query(request(fileToStringArr(LegacyFunctionTest.class, fileName))));
  }

  @Test
  public void addTest() throws Exception {
    double result = (Double)getStatResult("ar", "sum", VAL_TYPE.DOUBLE);
    double calculated = (Double)getStatResult("ar", "sumc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), result, calculated, 0.0);
    // TODO checfk why asserted 2times
    assertEquals(getRawResponse(), result, calculated, 0.0);

    result = (Double)getStatResult("ar", "mean", VAL_TYPE.DOUBLE);
    calculated = (Double)getStatResult("ar", "meanc", VAL_TYPE.DOUBLE);
    assertTrue(result==calculated);
    assertEquals(getRawResponse(), result, calculated, 0.0);
  }

  @Test
  public void multiplyTest() throws Exception {
    double result = (Double)getStatResult("mr", "sum", VAL_TYPE.DOUBLE);
    double calculated = (Double)getStatResult("mr", "sumc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(),  result, calculated, 0.0);

    result = (Double)getStatResult("mr", "mean", VAL_TYPE.DOUBLE);
    calculated = (Double)getStatResult("mr", "meanc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(),  result, calculated, 0.0);
  }

  @Test
  public void divideTest() throws Exception {
    Double result = (Double)getStatResult("dr", "sum", VAL_TYPE.DOUBLE);
    Double calculated = (Double)getStatResult("dr", "sumc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(),  result, calculated, 0.0);

    result = (Double)getStatResult("dr", "mean", VAL_TYPE.DOUBLE);
    calculated = (Double)getStatResult("dr", "meanc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(),  result, calculated, 0.0);
  }

  @Test
  public void powerTest() throws Exception {
    double result = (Double)getStatResult("pr", "sum", VAL_TYPE.DOUBLE);
    double calculated = (Double)getStatResult("pr", "sumc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), result, calculated, 0.0);
    assertEquals(getRawResponse(),  result, calculated, 0.0);

    result = (Double)getStatResult("pr", "mean", VAL_TYPE.DOUBLE);
    calculated = (Double)getStatResult("pr", "meanc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), result, calculated, 0.0);
    assertEquals(getRawResponse(), result, calculated, 0.0);
  }

  @Test
  public void negateTest() throws Exception {
    double result = (Double)getStatResult("nr", "sum", VAL_TYPE.DOUBLE);
    double calculated = (Double)getStatResult("nr", "sumc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(),  result, calculated, 0.0);

    result = (Double)getStatResult("nr", "mean", VAL_TYPE.DOUBLE);
    calculated = (Double)getStatResult("nr", "meanc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(),  result, calculated, 0.0);
  }

  @Test
  public void absoluteValueTest() throws Exception {
    double result = (Double)getStatResult("avr", "sum", VAL_TYPE.DOUBLE);
    double calculated = (Double)getStatResult("avr", "sumc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(),  result, calculated, 0.0);

    result = (Double)getStatResult("avr", "mean", VAL_TYPE.DOUBLE);
    calculated = (Double)getStatResult("avr", "meanc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(),  result, calculated, 0.0);
  }

  @Test
  public void constantNumberTest() throws Exception {
    double result = (Double)getStatResult("cnr", "sum", VAL_TYPE.DOUBLE);
    double calculated = (Double)getStatResult("cnr", "sumc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), result, calculated, 0.0);
    assertEquals(getRawResponse(), result, calculated, 0.0);

    result = (Double)getStatResult("cnr", "mean", VAL_TYPE.DOUBLE);
    calculated = (Double)getStatResult("cnr", "meanc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), result, calculated, 0.0);
    assertEquals(getRawResponse(),  result, calculated, 0.0);
  }

  @Test
  public void dateMathTest() throws Exception {
    String result = (String)getStatResult("dmr", "median", VAL_TYPE.DATE);
    String calculated = (String)getStatResult("dmr", "medianc", VAL_TYPE.DATE);
    assertEquals(getRawResponse(), result, calculated);

    result = (String)getStatResult("dmr", "max", VAL_TYPE.DATE);
    calculated = (String)getStatResult("dmr", "maxc", VAL_TYPE.DATE);
    assertEquals(getRawResponse(), result, calculated);
  }

  @Test
  public void constantDateTest() throws Exception {
    String result = (String)getStatResult("cdr", "median", VAL_TYPE.DATE);
    String calculated = (String)getStatResult("cdr", "medianc", VAL_TYPE.DATE);
    assertEquals(getRawResponse(), result, calculated);
    assertEquals(getRawResponse(), result, calculated);

    result = (String)getStatResult("cdr", "max", VAL_TYPE.DATE);
    calculated = (String)getStatResult("cdr", "maxc", VAL_TYPE.DATE);
    assertEquals(getRawResponse(), result, calculated);
  }

  @Test
  public void constantStringTest() throws Exception {
    String result = (String)getStatResult("csr", "min", VAL_TYPE.STRING);
    String calculated = (String)getStatResult("csr", "minc", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), result, calculated);

    result = (String)getStatResult("csr", "max", VAL_TYPE.STRING);
    calculated = (String)getStatResult("csr", "maxc", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), result, calculated);
  }

  @Test
  public void concatenateTest() throws Exception {
    String result = (String)getStatResult("cr", "min", VAL_TYPE.STRING);
    String calculated = (String)getStatResult("cr", "minc", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), result, calculated);

    result = (String)getStatResult("cr", "max", VAL_TYPE.STRING);
    calculated = (String)getStatResult("cr", "maxc", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), result, calculated);
  }

  @Test
  public void missingTest() throws Exception {
    double min = (Double)getStatResult("ms", "min", VAL_TYPE.DOUBLE);
    double max = (Double)getStatResult("ms", "max", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), 48.0d, max, 0.0);
    assertEquals(getRawResponse(), 1.0d, min, 0.0);
  }

}
