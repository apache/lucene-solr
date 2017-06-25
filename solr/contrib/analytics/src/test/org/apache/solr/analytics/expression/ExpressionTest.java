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
package org.apache.solr.analytics.expression;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

import com.google.common.collect.ObjectArrays;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.AbstractAnalyticsStatsTest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.DateMathParser;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExpressionTest extends AbstractAnalyticsStatsTest {
  private static final String fileName = "/analytics/requestFiles/expressions.txt";

  private static final String[] BASEPARMS = new String[]{"q", "*:*", "indent", "true", "stats", "true", "olap", "true", "rows", "0"};

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

    setResponse(h.query(request(fileToStringArr(ExpressionTest.class, fileName))));
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

    double countResult = ((Long) getStatResult("nr", "count", VAL_TYPE.LONG)).doubleValue();
    result = (Double) getStatResult("nr", "c", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), -1 * countResult, result, 0.0);
  }

  @Test
  public void absoluteValueTest() throws Exception {
    double sumResult = (Double) getStatResult("avr", "sum", VAL_TYPE.DOUBLE);
    double result = (Double) getStatResult("avr", "s", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), sumResult, result, 0.0);

    double countResult = ((Long) getStatResult("avr", "count", VAL_TYPE.LONG)).doubleValue();
    result = (Double) getStatResult("avr", "c", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), countResult, result, 0.0);
  }

  @Test
  public void constantNumberTest() throws Exception {
    double result = (Double) getStatResult("cnr", "c8", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), 8, result, 0.0);

    result = (Double) getStatResult("cnr", "c10", VAL_TYPE.DOUBLE);
    assertEquals(getRawResponse(), 10, result, 0.0);
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

  @Test
  public void reverseTest() throws Exception {
    StringBuilder builder = new StringBuilder();
    builder.append((String) getStatResult("rr", "min", VAL_TYPE.STRING));
    String rev = (String) getStatResult("rr", "rmin", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), rev, builder.reverse().toString());

    builder.setLength(0);
    builder.append((String) getStatResult("rr", "max", VAL_TYPE.STRING));
    rev = (String) getStatResult("rr", "rmax", VAL_TYPE.STRING);
    assertEquals(getRawResponse(), rev, builder.reverse().toString());
  }

  public static SolrQueryRequest request(String... args) {
    return SolrTestCaseJ4.req(ObjectArrays.concat(BASEPARMS, args, String.class));
  }

  public static String[] fileToStringArr(Class<?> clazz, String fileName) throws FileNotFoundException {
    InputStream in = clazz.getResourceAsStream(fileName);
    if (in == null) throw new FileNotFoundException("Resource not found: " + fileName);
    Scanner file = new Scanner(in, "UTF-8");
    try { 
      ArrayList<String> strList = new ArrayList<>();
      while (file.hasNextLine()) {
        String line = file.nextLine();
        if (line.length()<2) {
          continue;
        }
        String[] param = line.split("=");
        strList.add(param[0]);
        strList.add(param[1]);
      }
      return strList.toArray(new String[0]);
    } finally {
      IOUtils.closeWhileHandlingException(file, in);
    }
  }
}
