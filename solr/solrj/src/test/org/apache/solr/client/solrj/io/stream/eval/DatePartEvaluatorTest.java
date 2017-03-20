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

package org.apache.solr.client.solrj.io.stream.eval;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.MonthDay;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.collections.map.HashedMap;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.DatePartEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

/**
 * Tests numeric Date/Time stream evaluators
 */
public class DatePartEvaluatorTest {


  StreamFactory factory;
  Map<String, Object> values;

  public DatePartEvaluatorTest() {
    super();

    factory = new StreamFactory();

    factory.withFunctionName("nope", DatePartEvaluator.class);
    for (DatePartEvaluator.FUNCTION function : DatePartEvaluator.FUNCTION.values()) {
      factory.withFunctionName(function.toString(), DatePartEvaluator.class);
    }
    values = new HashedMap();
  }

  @Test
  public void testInvalidExpression() throws Exception {

    StreamEvaluator evaluator;

    try {
      evaluator = factory.constructEvaluator("nope(a)");
      evaluator.evaluate(new Tuple(null));
      assertTrue(false);
    } catch (IOException e) {
      assertTrue(e.getCause().getCause().getMessage().contains("Invalid date expression nope"));
      assertTrue(e.getCause().getCause().getMessage().contains("expecting one of [year, month, day"));
    }

    try {
      evaluator = factory.constructEvaluator("week()");
      assertTrue(false);
    } catch (IOException e) {
      assertTrue(e.getCause().getCause().getMessage().contains("Invalid expression week()"));
    }

    try {
      evaluator = factory.constructEvaluator("week(a, b)");
      assertTrue(false);
    } catch (IOException e) {
      assertTrue(e.getCause().getCause().getMessage().contains("expecting one value but found 2"));
    }

    try {
      evaluator = factory.constructEvaluator("Week()");
      assertTrue(false);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Invalid evaluator expression Week() - function 'Week' is unknown"));
    }
  }


  @Test
  public void testInvalidValues() throws Exception {
    StreamEvaluator evaluator = factory.constructEvaluator("year(a)");

    try {
      values.clear();
      values.put("a", 12);
      Object result = evaluator.evaluate(new Tuple(values));
      assertTrue(false);
    } catch (IOException e) {
      assertEquals("Invalid parameter 12 - The parameter must be a string formatted ISO_INSTANT or of type Instant,Date or LocalDateTime.", e.getMessage());
    }

    try {
      values.clear();
      values.put("a", "1995-12-31");
      Object result = evaluator.evaluate(new Tuple(values));
      assertTrue(false);
    } catch (IOException e) {
      assertEquals("Invalid parameter 1995-12-31 - The String must be formatted in the ISO_INSTANT date format.", e.getMessage());
    }

    try {
      values.clear();
      values.put("a", "");
      Object result = evaluator.evaluate(new Tuple(values));
      assertTrue(false);
    } catch (IOException e) {
      assertEquals("Invalid parameter  - The parameter must be a string formatted ISO_INSTANT or of type Instant,Date or LocalDateTime.", e.getMessage());
    }

    values.clear();
    values.put("a", null);
    assertNull(evaluator.evaluate(new Tuple(values)));
  }

  @Test
  public void testAllFunctions() throws Exception {

    //year, month, day, dayofyear, hour, minute, quarter, week, second, epoch
    testFunction("year(a)", "1995-12-31T23:59:59Z", 1995);
    testFunction("month(a)","1995-12-31T23:59:59Z", 12);
    testFunction("day(a)",  "1995-12-31T23:59:59Z", 31);
    testFunction("dayOfYear(a)",  "1995-12-31T23:59:59Z", 365);
    testFunction("dayOfQuarter(a)",  "1995-12-31T23:59:59Z", 92);
    testFunction("hour(a)",   "1995-12-31T23:59:59Z", 23);
    testFunction("minute(a)", "1995-12-31T23:59:59Z", 59);
    testFunction("quarter(a)","1995-12-31T23:59:59Z", 4);
    testFunction("week(a)",   "1995-12-31T23:59:59Z", 52);
    testFunction("second(a)", "1995-12-31T23:59:58Z", 58);
    testFunction("epoch(a)",  "1995-12-31T23:59:59Z", 820454399000l);

    testFunction("year(a)", "2017-03-17T10:30:45Z", 2017);
    testFunction("year('a')", "2017-03-17T10:30:45Z", 2017);
    testFunction("month(a)","2017-03-17T10:30:45Z", 3);
    testFunction("day(a)",  "2017-03-17T10:30:45Z", 17);
    testFunction("day('a')",  "2017-03-17T10:30:45Z", 17);
    testFunction("dayOfYear(a)",  "2017-03-17T10:30:45Z", 76);
    testFunction("dayOfQuarter(a)",  "2017-03-17T10:30:45Z", 76);
    testFunction("hour(a)",   "2017-03-17T10:30:45Z", 10);
    testFunction("minute(a)", "2017-03-17T10:30:45Z", 30);
    testFunction("quarter(a)","2017-03-17T10:30:45Z", 1);
    testFunction("week(a)",   "2017-03-17T10:30:45Z", 11);
    testFunction("second(a)", "2017-03-17T10:30:45Z", 45);
    testFunction("epoch(a)",  "2017-03-17T10:30:45Z", 1489746645000l);

    testFunction("epoch(a)",  new Date(1489746645500l).toInstant().toString(), 1489746645500l);
    testFunction("epoch(a)",  new Date(820454399990l).toInstant().toString(), 820454399990l);

    //Additionally test all functions to make sure they return a non-null number
    for (DatePartEvaluator.FUNCTION function : DatePartEvaluator.FUNCTION.values()) {
      StreamEvaluator evaluator = factory.constructEvaluator(function+"(a)");
      values.clear();
      values.put("a", "2017-03-17T10:30:45Z");
      Object result = evaluator.evaluate(new Tuple(values));
      assertNotNull(function+" should return a result",result);
      assertTrue(function+" should return a number", result instanceof Number);
    }
  }

  @Test
  public void testFunctionsOnDate() throws Exception {
    Calendar calendar = new GregorianCalendar(2017,12,5, 23, 59);
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    Date aDate = calendar.getTime();
    testFunction("year(a)", aDate, calendar.get(Calendar.YEAR));
    testFunction("month(a)", aDate, calendar.get(Calendar.MONTH)+1);
    testFunction("day(a)", aDate, calendar.get(Calendar.DAY_OF_MONTH));
    testFunction("hour(a)", aDate, calendar.get(Calendar.HOUR_OF_DAY));
    testFunction("minute(a)", aDate, calendar.get(Calendar.MINUTE));
    testFunction("epoch(a)", aDate, aDate.getTime());
  }

  @Test
  public void testFunctionsOnInstant() throws Exception {
    Calendar calendar = new GregorianCalendar(2017,12,5, 23, 59);
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    Date aDate = calendar.getTime();
    Instant instant = aDate.toInstant();
    testFunction("year(a)", instant, calendar.get(Calendar.YEAR));
    testFunction("month(a)", instant, calendar.get(Calendar.MONTH)+1);
    testFunction("day(a)", instant, calendar.get(Calendar.DAY_OF_MONTH));
    testFunction("hour(a)", instant, calendar.get(Calendar.HOUR_OF_DAY));
    testFunction("minute(a)", instant, calendar.get(Calendar.MINUTE));
    testFunction("epoch(a)", instant, aDate.getTime());
  }

  @Test
  public void testFunctionsLocalDateTime() throws Exception {

    LocalDateTime localDateTime = LocalDateTime.of(2017,12,5, 23, 59);
    Date aDate = Date.from(localDateTime.atZone(ZoneOffset.UTC).toInstant());
    testFunction("year(a)", localDateTime, 2017);
    testFunction("month(a)", localDateTime, 12);
    testFunction("day(a)", localDateTime, 5);
    testFunction("hour(a)", localDateTime, 23);
    testFunction("minute(a)", localDateTime, 59);
    testFunction("epoch(a)", localDateTime, aDate.getTime());
  }

  @Test
  public void testLimitedFunctions() throws Exception {

    MonthDay monthDay = MonthDay.of(12,5);
    testFunction("month(a)", monthDay, 12);
    testFunction("day(a)", monthDay, 5);

    try {
      testFunction("year(a)", monthDay, 2017);
      assertTrue(false);
    } catch (IOException e) {
      assertEquals("It is not possible to call 'year' function on java.time.MonthDay", e.getMessage());
    }

    YearMonth yearMonth = YearMonth.of(2018, 4);
    testFunction("month(a)", yearMonth, 4);
    testFunction("year(a)", yearMonth, 2018);

    try {
      testFunction("day(a)", yearMonth, 5);
      assertTrue(false);
    } catch (IOException e) {
      assertEquals("It is not possible to call 'day' function on java.time.YearMonth", e.getMessage());
    }

  }


  public void testFunction(String expression, Object value, Number expected) throws Exception {
    StreamEvaluator evaluator = factory.constructEvaluator(expression);
    values.clear();
    values.put("a", value);
    Object result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Number);
    assertEquals(expected, result);
  }

  @Test
  public void testExplain() throws IOException {
    StreamExpression express = StreamExpressionParser.parse("month('myfield')");
    DatePartEvaluator datePartEvaluator = new DatePartEvaluator(express,factory);
    Explanation explain = datePartEvaluator.toExplanation(factory);
    assertEquals("month(myfield)", explain.getExpression());

    express = StreamExpressionParser.parse("day(aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbb)");
    datePartEvaluator = new DatePartEvaluator(express,factory);
    explain = datePartEvaluator.toExplanation(factory);
    assertEquals("day(aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbb)", explain.getExpression());
  }
}
