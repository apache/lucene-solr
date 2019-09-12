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
package org.apache.solr.analytics.function.mapping;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValue;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestStringValue;
import org.apache.solr.analytics.value.FillableTestValue.TestStringValueStream;
import org.junit.Test;

public class DateParseFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void singleValueLongTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1920-04-15T18:15:45Z"));
    TestLongValue val = new TestLongValue();

    AnalyticsValueStream uncasted = DateParseFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DateValue);
    DateValue func = (DateValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    func.getDate();
    assertFalse(func.exists());
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    val.setValue(date1.getTime()).setExists(true);
    assertEquals(date1, func.getDate());
    assertTrue(func.exists());
    assertEquals(date1.getTime(), func.getLong());
    assertTrue(func.exists());

    val.setValue(date2.getTime()).setExists(true);
    assertEquals(date2, func.getDate());
    assertTrue(func.exists());
    assertEquals(date2.getTime(), func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueStringTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1920-04-15T18:15:45Z"));
    TestStringValue val = new TestStringValue();

    AnalyticsValueStream uncasted = DateParseFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DateValue);
    DateValue func = (DateValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    func.getDate();
    assertFalse(func.exists());
    func.getLong();
    assertFalse(func.exists());

    // Incorrect Value
    val.setValue("1800-01T10:30:15Z").setExists(true);
    func.getDate();
    assertFalse(func.exists());
    func.getLong();
    assertFalse(func.exists());

    val.setValue("1800-01-T::Z").setExists(true);
    func.getDate();
    assertFalse(func.exists());
    func.getLong();
    assertFalse(func.exists());

    val.setValue("1800--01T30:30:15Z").setExists(true);
    func.getDate();
    assertFalse(func.exists());
    func.getLong();
    assertFalse(func.exists());


    // Value exists
    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    assertEquals(date1, func.getDate());
    assertTrue(func.exists());
    assertEquals(date1.getTime(), func.getLong());
    assertTrue(func.exists());

    val.setValue("1920-04-15T18:15:45Z").setExists(true);
    assertEquals(date2, func.getDate());
    assertTrue(func.exists());
    assertEquals(date2.getTime(), func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueLongTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1920-04-15T18:15:45Z"));
    Date date3 = Date.from(Instant.parse("2012-11-30T20:30:15Z"));
    TestLongValueStream val = new TestLongValueStream();

    AnalyticsValueStream uncasted = DateParseFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DateValueStream);
    DateValueStream func = (DateValueStream) uncasted;

    // No values
    val.setValues();
    func.streamDates( value -> {
      assertTrue("There should be no values to stream", false);
    });
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(date1.getTime());
    Iterator<Date> values1 = Arrays.asList(date1).iterator();
    func.streamDates( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());
    Iterator<Long> times1 = Arrays.asList(date1.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(times1.hasNext());
      assertEquals(times1.next().longValue(), value);
    });
    assertFalse(times1.hasNext());

    // Multiple values
    val.setValues(date1.getTime(), date2.getTime(), date3.getTime());
    Iterator<Date> values2 = Arrays.asList(date1, date2, date3).iterator();
    func.streamDates( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
    Iterator<Long> times2 = Arrays.asList(date1.getTime(), date2.getTime(), date3.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(times2.hasNext());
      assertEquals(times2.next().longValue(), value);
    });
    assertFalse(times2.hasNext());
  }

  @Test
  public void multiValueStringTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1920-04-15T18:15:45Z"));
    Date date3 = Date.from(Instant.parse("2012-11-30T20:30:15Z"));
    TestStringValueStream val = new TestStringValueStream();

    AnalyticsValueStream uncasted = DateParseFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DateValueStream);
    DateValueStream func = (DateValueStream) uncasted;

    // No values
    val.setValues();
    func.streamDates( value -> {
      assertTrue("There should be no values to stream", false);
    });
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Incorrect value
    val.setValues("10:30:15Z");
    func.streamDates( value -> {
      assertTrue("There should be no values to stream", false);
    });
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues("01-33T10:30:15Z");
    func.streamDates( value -> {
      assertTrue("There should be no values to stream", false);
    });
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues("1800-01T30:30:15Z");
    func.streamDates( value -> {
      assertTrue("There should be no values to stream", false);
    });
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues("1800-01-01T10:30:15Z");
    Iterator<Date> values1 = Arrays.asList(date1).iterator();
    func.streamDates( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());
    Iterator<Long> times1 = Arrays.asList(date1.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(times1.hasNext());
      assertEquals(times1.next().longValue(), value);
    });
    assertFalse(times1.hasNext());

    // Multiple values
    val.setValues("1800-01-01T10:30:15Z", "1920-04-15T18:15:45Z", "2012-11-30T20:30:15Z");
    Iterator<Date> values2 = Arrays.asList(date1, date2, date3).iterator();
    func.streamDates( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
    Iterator<Long> times2 = Arrays.asList(date1.getTime(), date2.getTime(), date3.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(times2.hasNext());
      assertEquals(times2.next().longValue(), value);
    });
    assertFalse(times2.hasNext());
  }
}
