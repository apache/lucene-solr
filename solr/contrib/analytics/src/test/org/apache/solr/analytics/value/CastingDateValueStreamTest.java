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
package org.apache.solr.analytics.value;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.FillableTestValue.TestDateValueStream;
import org.junit.Test;

public class CastingDateValueStreamTest extends SolrTestCaseJ4 {

  @Test
  public void dateStreamCastingTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1920-04-15T18:15:45Z"));
    Date date3 = Date.from(Instant.parse("2012-11-30T20:30:15Z"));
    TestDateValueStream val = new TestDateValueStream();

    assertTrue(val instanceof DateValueStream);
    DateValueStream casted = (DateValueStream)val;

    // No values
    val.setValues();
    casted.streamDates( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValues("1800-01-01T10:30:15Z", "1920-04-15T18:15:45Z", "2012-11-30T20:30:15Z");
    Iterator<Date> values = Arrays.asList(date1, date2, date3).iterator();
    casted.streamDates( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void stringStreamCastingTest() {
    TestDateValueStream val = new TestDateValueStream();

    assertTrue(val instanceof StringValueStream);
    StringValueStream casted = (StringValueStream)val;

    // No values
    val.setValues();
    casted.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValues("1800-01-01T10:30:15Z", "1920-04-15T18:15:45Z", "2012-11-30T20:30:15Z");
    Iterator<String> values = Arrays.asList("1800-01-01T10:30:15Z", "1920-04-15T18:15:45Z", "2012-11-30T20:30:15Z").iterator();
    casted.streamStrings( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void objectStreamCastingTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1920-04-15T18:15:45Z"));
    Date date3 = Date.from(Instant.parse("2012-11-30T20:30:15Z"));
    TestDateValueStream val = new TestDateValueStream();

    assertTrue(val instanceof AnalyticsValueStream);
    AnalyticsValueStream casted = (AnalyticsValueStream)val;

    // No values
    val.setValues();
    casted.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValues("1800-01-01T10:30:15Z", "1920-04-15T18:15:45Z", "2012-11-30T20:30:15Z");
    Iterator<Object> values = Arrays.<Object>asList(date1, date2, date3).iterator();
    casted.streamObjects( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void constantConversionTest() {
    AnalyticsValueStream val = new TestDateValueStream();
    assertSame(val, val.convertToConstant());
  }
}
