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
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDay;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDayOfQuarter;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDayOfYear;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorEpoch;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorHour;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorMinute;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorMonth;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorQuarter;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorSecond;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorWeek;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorYear;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Tests numeric Date/Time stream evaluators
 */
public class TemporalEvaluatorsTest {


  StreamFactory factory;
  Map<String, Object> values;

  public TemporalEvaluatorsTest() {
    super();

    factory = new StreamFactory();

    factory.withFunctionName(TemporalEvaluatorYear.FUNCTION_NAME,  TemporalEvaluatorYear.class);
    factory.withFunctionName(TemporalEvaluatorMonth.FUNCTION_NAME, TemporalEvaluatorMonth.class);
    factory.withFunctionName(TemporalEvaluatorDay.FUNCTION_NAME,   TemporalEvaluatorDay.class);
    factory.withFunctionName(TemporalEvaluatorDayOfYear.FUNCTION_NAME,   TemporalEvaluatorDayOfYear.class);
    factory.withFunctionName(TemporalEvaluatorHour.FUNCTION_NAME,   TemporalEvaluatorHour.class);
    factory.withFunctionName(TemporalEvaluatorMinute.FUNCTION_NAME,   TemporalEvaluatorMinute.class);
    factory.withFunctionName(TemporalEvaluatorSecond.FUNCTION_NAME,   TemporalEvaluatorSecond.class);
    factory.withFunctionName(TemporalEvaluatorEpoch.FUNCTION_NAME,   TemporalEvaluatorEpoch.class);
    factory.withFunctionName(TemporalEvaluatorWeek.FUNCTION_NAME,   TemporalEvaluatorWeek.class);
    factory.withFunctionName(TemporalEvaluatorQuarter.FUNCTION_NAME,   TemporalEvaluatorQuarter.class);
    factory.withFunctionName(TemporalEvaluatorDayOfQuarter.FUNCTION_NAME,   TemporalEvaluatorDayOfQuarter.class);

    values = new HashedMap<>();
  }

  @Test
  public void testInvalidExpression() throws Exception {

    StreamEvaluator evaluator;

    try {
      evaluator = factory.constructEvaluator("week()");
      StreamContext streamContext = new StreamContext();
      evaluator.setStreamContext(streamContext);
      assertTrue(false);
    } catch (IOException e) {
      assertTrue(e.getCause().getCause().getMessage().contains("Invalid expression week()"));
    }

    try {
      evaluator = factory.constructEvaluator("week(a, b)");
      StreamContext streamContext = new StreamContext();
      evaluator.setStreamContext(streamContext);
      assertTrue(false);
    } catch (IOException e) {
      assertTrue(e.getCause().getCause().getMessage().contains("expecting one value but found 2"));
    }

    try {
      evaluator = factory.constructEvaluator("Week()");
      StreamContext streamContext = new StreamContext();
      evaluator.setStreamContext(streamContext);
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
      StreamContext streamContext = new StreamContext();
      evaluator.setStreamContext(streamContext);
      Object result = evaluator.evaluate(new Tuple(values));
      assertTrue(false);
    } catch (IOException e) {
      assertEquals("Invalid parameter 12 - The parameter must be a string formatted ISO_INSTANT or of type Long,Instant,Date,LocalDateTime or TemporalAccessor.", e.getMessage());
    }

    try {
      values.clear();
      values.put("a", "1995-12-31");
      StreamContext streamContext = new StreamContext();
      evaluator.setStreamContext(streamContext);
      Object result = evaluator.evaluate(new Tuple(values));
      assertTrue(false);
    } catch (IOException e) {
      assertEquals("Invalid parameter 1995-12-31 - The String must be formatted in the ISO_INSTANT date format.", e.getMessage());
    }

    try {
      values.clear();
      values.put("a", "");
      StreamContext streamContext = new StreamContext();
      evaluator.setStreamContext(streamContext);
      Object result = evaluator.evaluate(new Tuple(values));
      assertTrue(false);
    } catch (IOException e) {
      assertEquals("Invalid parameter  - The parameter must be a string formatted ISO_INSTANT or of type Long,Instant,Date,LocalDateTime or TemporalAccessor.", e.getMessage());
    }

    values.clear();
  }

  @Test
  public void testAllFunctions() throws Exception {

    //year, month, day, dayofyear, hour, minute, quarter, week, second, epoch
    testFunction("year(a)", "1995-12-31T23:59:59Z", 1995L);
    testFunction("month(a)","1995-12-31T23:59:59Z", 12L);
    testFunction("day(a)",  "1995-12-31T23:59:59Z", 31L);
    testFunction("dayOfYear(a)",  "1995-12-31T23:59:59Z", 365L);
    testFunction("dayOfQuarter(a)",  "1995-12-31T23:59:59Z", 92L);
    testFunction("hour(a)",   "1995-12-31T23:59:59Z", 23L);
    testFunction("minute(a)", "1995-12-31T23:59:59Z", 59L);
    testFunction("quarter(a)","1995-12-31T23:59:59Z", 4L);
    testFunction("week(a)",   "1995-12-31T23:59:59Z", 52L);
    testFunction("second(a)", "1995-12-31T23:59:58Z", 58L);
    testFunction("epoch(a)",  "1995-12-31T23:59:59Z", 820454399000l);

    testFunction("year(a)", "2017-03-17T10:30:45Z", 2017L);
    testFunction("year('a')", "2017-03-17T10:30:45Z", 2017L);
    testFunction("month(a)","2017-03-17T10:30:45Z", 3L);
    testFunction("day(a)",  "2017-03-17T10:30:45Z", 17L);
    testFunction("day('a')",  "2017-03-17T10:30:45Z", 17L);
    testFunction("dayOfYear(a)",  "2017-03-17T10:30:45Z", 76L);
    testFunction("dayOfQuarter(a)",  "2017-03-17T10:30:45Z", 76L);
    testFunction("hour(a)",   "2017-03-17T10:30:45Z", 10L);
    testFunction("minute(a)", "2017-03-17T10:30:45Z", 30L);
    testFunction("quarter(a)","2017-03-17T10:30:45Z", 1L);
    testFunction("week(a)",   "2017-03-17T10:30:45Z", 11L);
    testFunction("second(a)", "2017-03-17T10:30:45Z", 45L);
    testFunction("epoch(a)",  "2017-03-17T10:30:45Z", 1489746645000l);

    testFunction("epoch(a)",  new Date(1489746645500l).toInstant().toString(), 1489746645500l);
    testFunction("epoch(a)",  new Date(820454399990l).toInstant().toString(), 820454399990l);

  }

  @Test
  public void testFunctionsOnDate() throws Exception {
    Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ROOT);
    calendar.set(2017, 12, 5, 23, 59);
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
    Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ROOT);
    calendar.set(2017, 12, 5, 23, 59);
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
  public void testFunctionsOnLong() throws Exception {

    Long longDate = 1512518340000l;

    testFunction("year(a)", longDate, 2017);
    testFunction("month(a)", longDate, 12);
    testFunction("day(a)", longDate, 5);
    testFunction("hour(a)", longDate, 23);
    testFunction("minute(a)", longDate, 59);
    testFunction("second(a)", longDate, 0);
    testFunction("epoch(a)", longDate, longDate);

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
    StreamContext streamContext = new StreamContext();
    evaluator.setStreamContext(streamContext);
    values.clear();
    values.put("a", value);
    Object result = evaluator.evaluate(new Tuple(values));
    assertTrue(result instanceof Number);
    assertEquals(expected.longValue(), result);
  }

  @Test
  public void testExplain() throws IOException {
    StreamExpression express = StreamExpressionParser.parse("month('myfield')");
    TemporalEvaluatorMonth datePartEvaluator = new TemporalEvaluatorMonth(express,factory);
    Explanation explain = datePartEvaluator.toExplanation(factory);
    assertEquals("month(myfield)", explain.getExpression());

    express = StreamExpressionParser.parse("day(aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbb)");
    TemporalEvaluatorDay dayPartEvaluator = new TemporalEvaluatorDay(express,factory);
    explain = dayPartEvaluator.toExplanation(factory);
    assertEquals("day(aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbb)", explain.getExpression());
  }
}
