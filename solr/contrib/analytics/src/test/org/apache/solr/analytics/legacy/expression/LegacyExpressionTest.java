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

import java.time.Instant;
import java.util.Date;

import org.apache.solr.analytics.legacy.LegacyAbstractAnalyticsTest;
import org.apache.solr.util.DateMathParser;
import org.junit.BeforeClass;
import org.junit.Test;

public class LegacyExpressionTest extends LegacyAbstractAnalyticsTest {
  private static final String fileName = "expressions.txt";

  private static final int INT = 71;
  private static final int LONG = 36;
  private static final int FLOAT = 93;
  private static final int DOUBLE = 49;
  private static final int DATE = 12;
  private static final int STRING = 28;
  private static final int NUM_LOOPS = 100;


  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml", "schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");

    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j % INT;
      long l = j % LONG;
      float f = j % FLOAT;
      double d = j % DOUBLE;
      String dt = (1800 + j % DATE) + "-12-31T23:59:59Z";
      String s = "str" + (j % STRING);
      assertU(adoc("id", "1000" + j, "int_id", "" + i, "long_ld", "" + l, "float_fd", "" + f,
          "double_dd", "" + d, "date_dtd", dt, "string_sd", s));

      if (usually()) {
        assertU(commit()); // to have several segments
      }
    }

    assertU(commit());

    setResponse(h.query(request(fileToStringArr(LegacyExpressionTest.class, fileName))));
  }

  @Test
  public void addTest() throws Exception {
    double sumResult = (Double) getStatResult("ar", "sum", VAL_TYPE.DOUBLE);
    double uniqueResult = ((Long) getStatResult("ar", "unique", VAL_TYPE.LONG)).doubleValue();
    double result = (Double) getStatResult("ar", "su", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), sumResult + uniqueResult, result, 0.0);

    double meanResult = (Double) getStatResult("ar", "mean", VAL_TYPE.DOUBLE);
    double medianResult = (Double) getStatResult("ar", "median", VAL_TYPE.DOUBLE);
    double countResult = ((Long) getStatResult("ar", "count", VAL_TYPE.LONG)).doubleValue();
    result = (Double) getStatResult("ar", "mcm", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), meanResult + countResult + medianResult, result, 0.0);
  }

  @Test
  public void multiplyTest() throws Exception {
    double sumResult = (Double) getStatResult("mr", "sum", VAL_TYPE.DOUBLE);
    double uniqueResult = ((Long) getStatResult("mr", "unique", VAL_TYPE.LONG)).doubleValue();
    double result = (Double) getStatResult("mr", "su", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), sumResult * uniqueResult, result, 0.0);

    double meanResult = (Double) getStatResult("mr", "mean", VAL_TYPE.DOUBLE);
    double medianResult = (Double) getStatResult("mr", "median", VAL_TYPE.DOUBLE);
    double countResult = ((Long) getStatResult("mr", "count", VAL_TYPE.LONG)).doubleValue();
    result = (Double) getStatResult("mr", "mcm", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), meanResult * countResult * medianResult, result, 0.0);
  }

  @Test
  public void divideTest() throws Exception {
    double sumResult = (Double) getStatResult("dr", "sum", VAL_TYPE.DOUBLE);
    double uniqueResult = ((Long) getStatResult("dr", "unique", VAL_TYPE.LONG)).doubleValue();
    double result = (Double) getStatResult("dr", "su", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), sumResult / uniqueResult, result, 0.0);

    double meanResult = (Double) getStatResult("dr", "mean", VAL_TYPE.DOUBLE);
    double countResult = ((Long) getStatResult("dr", "count", VAL_TYPE.LONG)).doubleValue();
    result = (Double) getStatResult("dr", "mc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), meanResult / countResult, result, 0.0);
  }

  @Test
  public void powerTest() throws Exception {
    double sumResult = (Double) getStatResult("pr", "sum", VAL_TYPE.DOUBLE);
    double uniqueResult = ((Long) getStatResult("pr", "unique", VAL_TYPE.LONG)).doubleValue();
    double result = (Double) getStatResult("pr", "su", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), Math.pow(sumResult, uniqueResult), result, 0.0);

    double meanResult = (Double) getStatResult("pr", "mean", VAL_TYPE.DOUBLE);
    double countResult = ((Long) getStatResult("pr", "count", VAL_TYPE.LONG)).doubleValue();
    result = (Double) getStatResult("pr", "mc", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), Math.pow(meanResult, countResult), result, 0.0);
  }

  @Test
  public void negateTest() throws Exception {
    double sumResult = (Double) getStatResult("nr", "sum", VAL_TYPE.DOUBLE);
    double result = (Double) getStatResult("nr", "s", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), -1 * sumResult, result, 0.0);

    long countResult = ((Long) getStatResult("nr", "count", VAL_TYPE.LONG));
    long lresult = (Long) getStatResult("nr", "c", VAL_TYPE.LONG);
    assertEquals(getRawResponse(), -1 * countResult, lresult, 0.0);
  }

  @Test
  public void absoluteValueTest() throws Exception {
    double sumResult = (Double) getStatResult("avr", "sum", VAL_TYPE.DOUBLE);
    double result = (Double) getStatResult("avr", "s", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), sumResult, result, 0.0);

    long countResult = ((Long) getStatResult("avr", "count", VAL_TYPE.LONG));
    long lresult = (Long) getStatResult("avr", "c", VAL_TYPE.LONG);
    assertEquals(getRawResponse(), countResult, lresult, 0.0);
  }

  @Test
  public void constantNumberTest() throws Exception {
    int result = (Integer) getStatResult("cnr", "c8", VAL_TYPE.INTEGER);
    assertEquals(getRawResponse(), 8, result, 0.0);

    double dresult = (Double) getStatResult("cnr", "c10", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), 10.0, dresult, 0.0);
  }

  @Test
  public void dateMathTest() throws Exception {
    String math = (String) getStatResult("dmr", "cme", VAL_TYPE.STRING);
    DateMathParser dateMathParser = new DateMathParser();
    dateMathParser.setNow(new Date(Instant.parse((String) getStatResult("dmr", "median", VAL_TYPE.DATE)).toEpochMilli()));
    String dateMath = (String) getStatResult("dmr", "dmme", VAL_TYPE.DATE);
    assertEquals(getRawResponse(), new Date(Instant.parse(dateMath).toEpochMilli()), dateMathParser.parseMath(math));

    math = (String) getStatResult("dmr", "cma", VAL_TYPE.STRING);
    dateMathParser = new DateMathParser();
    dateMathParser.setNow(new Date(Instant.parse((String) getStatResult("dmr", "max", VAL_TYPE.DATE)).toEpochMilli()));
    dateMath = (String) getStatResult("dmr", "dmma", VAL_TYPE.DATE);
    assertEquals(getRawResponse(), new Date(Instant.parse(dateMath).toEpochMilli()), dateMathParser.parseMath(math));
  }

  @Test
  public void constantDateTest() throws Exception {
    String date = (String) getStatResult("cdr", "cd1", VAL_TYPE.DATE);
    String str = (String) getStatResult("cdr", "cs1", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), date, str);

    date = (String) getStatResult("cdr", "cd2", VAL_TYPE.DATE);
    str = (String) getStatResult("cdr", "cs2", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), date, str);
  }

  @Test
  public void constantStringTest() throws Exception {
    String str = (String) getStatResult("csr", "cs1", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), str, "this is the first");

    str = (String) getStatResult("csr", "cs2", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), str, "this is the second");

    str = (String) getStatResult("csr", "cs3", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), str, "this is the third");
  }

  @Test
  public void concatenateTest() throws Exception {
    StringBuilder builder = new StringBuilder();
    builder.append((String) getStatResult("cr", "csmin", VAL_TYPE.STRING));
    builder.append((String) getStatResult("cr", "min", VAL_TYPE.STRING));
    String concat = (String) getStatResult("cr", "ccmin", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), concat, builder.toString());

    builder.setLength(0);
    builder.append((String) getStatResult("cr", "csmax", VAL_TYPE.STRING));
    builder.append((String) getStatResult("cr", "max", VAL_TYPE.STRING));
    concat = (String) getStatResult("cr", "ccmax", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), concat, builder.toString());
  }
}
