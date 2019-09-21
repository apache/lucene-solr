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
import org.apache.solr.analytics.value.AnalyticsValue;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream;
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValue;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValue;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValue;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestAnalyticsValue;
import org.apache.solr.analytics.value.FillableTestValue.TestAnalyticsValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValue;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestDateValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDateValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValue;
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestIntValue;
import org.apache.solr.analytics.value.FillableTestValue.TestIntValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValue;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestStringValue;
import org.apache.solr.analytics.value.FillableTestValue.TestStringValueStream;
import org.junit.Test;

public class FilterFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void singleValueBooleanTest() {
    TestBooleanValue val = new TestBooleanValue();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filter.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    val.setValue(true).setExists(true);
    filter.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    val.setExists(false);
    filter.setValue(true).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    val.setValue(true).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(true).setExists(true);
    filter.setValue(false).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    val.setValue(false).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(false).setExists(true);
    filter.setValue(false).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());
  }

  @Test
  public void singleValueIntTest() {
    TestIntValue val = new TestIntValue();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof IntValue);
    IntValue func = (IntValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filter.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    val.setValue(32).setExists(true);
    filter.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    val.setExists(false);
    filter.setValue(true).setExists(true);
    func.getInt();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(21, func.getInt());
    assertTrue(func.exists());

    val.setValue(-100).setExists(true);
    filter.setValue(false).setExists(true);
    func.getInt();
    assertFalse(func.exists());

    val.setValue(50).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(50, func.getInt());
    assertTrue(func.exists());

    val.setValue(5432).setExists(true);
    filter.setValue(false).setExists(true);
    func.getInt();
    assertFalse(func.exists());
  }

  @Test
  public void singleValueLongTest() {
    TestLongValue val = new TestLongValue();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof LongValue);
    LongValue func = (LongValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filter.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    val.setValue(32L).setExists(true);
    filter.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    val.setExists(false);
    filter.setValue(true).setExists(true);
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21L).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(21L, func.getLong());
    assertTrue(func.exists());

    val.setValue(-100L).setExists(true);
    filter.setValue(false).setExists(true);
    func.getLong();
    assertFalse(func.exists());

    val.setValue(50L).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(50L, func.getLong());
    assertTrue(func.exists());

    val.setValue(3245L).setExists(true);
    filter.setValue(false).setExists(true);
    func.getLong();
    assertFalse(func.exists());
  }

  @Test
  public void singleValueFloatTest() {
    TestFloatValue val = new TestFloatValue();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof FloatValue);
    FloatValue func = (FloatValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filter.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    val.setValue(32.123F).setExists(true);
    filter.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    val.setExists(false);
    filter.setValue(true).setExists(true);
    func.getFloat();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21.453F).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(21.453F, func.getFloat(), .000001);
    assertTrue(func.exists());

    val.setValue(-100.123F).setExists(true);
    filter.setValue(false).setExists(true);
    func.getFloat();
    assertFalse(func.exists());

    val.setValue(50.5F).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(50.5F, func.getFloat(), .000001);
    assertTrue(func.exists());

    val.setValue(25643.23F).setExists(true);
    filter.setValue(false).setExists(true);
    func.getFloat();
    assertFalse(func.exists());
  }

  @Test
  public void singleValueDoubleTest() {
    TestDoubleValue val = new TestDoubleValue();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filter.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    val.setValue(32.123).setExists(true);
    filter.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    val.setExists(false);
    filter.setValue(true).setExists(true);
    func.getDouble();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21.453).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(21.453, func.getDouble(), .000001);
    assertTrue(func.exists());

    val.setValue(-100.123).setExists(true);
    filter.setValue(false).setExists(true);
    func.getDouble();
    assertFalse(func.exists());

    val.setValue(50.5).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(50.5, func.getDouble(), .000001);
    assertTrue(func.exists());

    val.setValue(25643.23).setExists(true);
    filter.setValue(false).setExists(true);
    func.getDouble();
    assertFalse(func.exists());
  }

  @Test
  public void singleValueDateTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1810-12-02T10:30:15Z"));
    Date date3 = Date.from(Instant.parse("2023-11-01T20:30:15Z"));

    TestDateValue val = new TestDateValue();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof DateValue);
    DateValue func = (DateValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filter.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    val.setValue("1810-12-02T10:30:15Z").setExists(true);
    filter.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    val.setExists(false);
    filter.setValue(true).setExists(true);
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    val.setValue("1810-12-02T10:30:15Z").setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(date1.getTime(), func.getLong());
    assertTrue(func.exists());

    val.setValue("1931-03-16T18:15:45Z").setExists(true);
    filter.setValue(false).setExists(true);
    func.getLong();
    assertFalse(func.exists());

    val.setValue("2023-11-01T20:30:15Z").setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(date3.getTime(), func.getLong());
    assertTrue(func.exists());

    val.setValue("1810-12-02T10:30:15Z").setExists(true);
    filter.setValue(false).setExists(true);
    func.getLong();
    assertFalse(func.exists());
  }

  @Test
  public void singleValueStringTest() {
    TestStringValue val = new TestStringValue();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filter.setExists(false);
    func.getString();
    assertFalse(func.exists());

    val.setValue("abc123").setExists(true);
    filter.setExists(false);
    func.getString();
    assertFalse(func.exists());

    val.setExists(false);
    filter.setValue(true).setExists(true);
    func.getString();
    assertFalse(func.exists());

    // Value exists
    val.setValue("abc").setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals("abc", func.getString());
    assertTrue(func.exists());

    val.setValue("def").setExists(true);
    filter.setValue(false).setExists(true);
    func.getString();
    assertFalse(func.exists());

    val.setValue("123").setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals("123", func.getString());
    assertTrue(func.exists());

    val.setValue("456").setExists(true);
    filter.setValue(false).setExists(true);
    func.getString();
    assertFalse(func.exists());
  }

  @Test
  public void singleValueObjectTest() {
    TestAnalyticsValue val = new TestAnalyticsValue();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof AnalyticsValue);
    AnalyticsValue func = (AnalyticsValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filter.setExists(false);
    func.getObject();
    assertFalse(func.exists());

    val.setValue("1810-12-02T10:30:15Z").setExists(true);
    filter.setExists(false);
    func.getObject();
    assertFalse(func.exists());

    val.setExists(false);
    filter.setValue(true).setExists(true);
    func.getObject();
    assertFalse(func.exists());

    // Value exists
    val.setValue(3L).setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals(3L, func.getObject());
    assertTrue(func.exists());

    val.setValue(new Date(2)).setExists(true);
    filter.setValue(false).setExists(true);
    func.getObject();
    assertFalse(func.exists());

    val.setValue("3").setExists(true);
    filter.setValue(true).setExists(true);
    assertEquals("3", func.getObject());
    assertTrue(func.exists());

    val.setValue(new TestAnalyticsValue()).setExists(true);
    filter.setValue(false).setExists(true);
    func.getObject();
    assertFalse(func.exists());
  }

  @Test
  public void multiValueBooleanTest() {
    TestBooleanValueStream val = new TestBooleanValueStream();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val.setValues();
    filter.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues(false, true, false);
    filter.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    filter.setValue(true).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(true);

    filter.setValue(true).setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    filter.setValue(false).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    // Multiple values
    val.setValues(false, false, true, true);

    filter.setValue(false).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Boolean> values3 = Arrays.asList(false, false, true, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());

    filter.setValue(false).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueIntTest() {
    TestIntValueStream val = new TestIntValueStream();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof IntValueStream);
    IntValueStream func = (IntValueStream) uncasted;

    // No values
    val.setValues();
    filter.setExists(false);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues(1, 3, 5);
    filter.setExists(false);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    filter.setValue(true).setExists(true);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(-4);

    filter.setValue(true).setExists(true);
    Iterator<Integer> values1 = Arrays.asList(-4).iterator();
    func.streamInts( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().intValue(), value);
    });
    assertFalse(values1.hasNext());

    filter.setValue(false).setExists(true);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Integer> values2 = Arrays.asList(-4).iterator();
    func.streamInts( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().intValue(), value);
    });
    assertFalse(values2.hasNext());

    // Multiple values
    val.setValues(4, -10, 50, -74);

    filter.setValue(false).setExists(true);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Integer> values3 = Arrays.asList(4, -10, 50, -74).iterator();
    func.streamInts( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().intValue(), value);
    });
    assertFalse(values3.hasNext());

    filter.setValue(false).setExists(true);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueLongTest() {
    TestLongValueStream val = new TestLongValueStream();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof LongValueStream);
    LongValueStream func = (LongValueStream) uncasted;

    // No values
    val.setValues();
    filter.setExists(false);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues(1L, 3L, 5L);
    filter.setExists(false);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    filter.setValue(true).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(-4L);

    filter.setValue(true).setExists(true);
    Iterator<Long> values1 = Arrays.asList(-4L).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    filter.setValue(false).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Long> values2 = Arrays.asList(-4L).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());

    // Multiple values
    val.setValues(4L, -10L, 50L, -74L);

    filter.setValue(false).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Long> values3 = Arrays.asList(4L, -10L, 50L, -74L).iterator();
    func.streamLongs( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().longValue(), value);
    });
    assertFalse(values3.hasNext());

    filter.setValue(false).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueFloatTest() {
    TestFloatValueStream val = new TestFloatValueStream();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof FloatValueStream);
    FloatValueStream func = (FloatValueStream) uncasted;

    // No values
    val.setValues();
    filter.setExists(false);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues(50.343F, -74.9874F, 2342332342.32F);
    filter.setExists(false);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    filter.setValue(true).setExists(true);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(-4.23423F);

    filter.setValue(true).setExists(true);
    Iterator<Float> values1 = Arrays.asList(-4.23423F).iterator();
    func.streamFloats( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .0000001);
    });
    assertFalse(values1.hasNext());

    filter.setValue(false).setExists(true);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Float> values2 = Arrays.asList(-4.23423F).iterator();
    func.streamFloats( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .0000001);
    });
    assertFalse(values2.hasNext());

    // Multiple values
    val.setValues(4.3F, -10.134F, 50.343F, -74.9874F);

    filter.setValue(false).setExists(true);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Float> values3 = Arrays.asList(4.3F, -10.134F, 50.343F, -74.9874F).iterator();
    func.streamFloats( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value, .0000001);
    });
    assertFalse(values3.hasNext());

    filter.setValue(false).setExists(true);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueDoubleTest() {
    TestDoubleValueStream val = new TestDoubleValueStream();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values
    val.setValues();
    filter.setExists(false);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues(50.343, -74.9874, 2342332342.32);
    filter.setExists(false);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    filter.setValue(true).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(-4.23423);

    filter.setValue(true).setExists(true);
    Iterator<Double> values1 = Arrays.asList(-4.23423).iterator();
    func.streamDoubles( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .0000001);
    });
    assertFalse(values1.hasNext());

    filter.setValue(false).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Double> values2 = Arrays.asList(-4.23423).iterator();
    func.streamDoubles( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .0000001);
    });
    assertFalse(values2.hasNext());

    // Multiple values
    val.setValues(4.3, -10.134, 50.343, -74.9874);

    filter.setValue(false).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Double> values3 = Arrays.asList(4.3, -10.134, 50.343, -74.9874).iterator();
    func.streamDoubles( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value, .0000001);
    });
    assertFalse(values3.hasNext());

    filter.setValue(false).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueDateTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1810-12-02T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1931-03-16T18:15:45Z"));
    Date date3 = Date.from(Instant.parse("2023-11-01T20:30:15Z"));

    TestDateValueStream val = new TestDateValueStream();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof DateValueStream);
    DateValueStream func = (DateValueStream) uncasted;

    // No values
    val.setValues();
    filter.setExists(false);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues("1810-12-02T10:30:15Z", "1850-12-02T20:30:15Z");
    filter.setExists(false);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    filter.setValue(true).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues("1810-12-02T10:30:15Z");

    filter.setValue(true).setExists(true);
    Iterator<Long> values1 = Arrays.asList(date1.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    filter.setValue(false).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Long> values2 = Arrays.asList(date1.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());

    // Multiple values
    val.setValues("1810-12-02T10:30:15Z", "1931-03-16T18:15:45Z", "2023-11-01T20:30:15Z");

    filter.setValue(false).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Long> values3 = Arrays.asList(date1.getTime(), date2.getTime(), date3.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().longValue(), value);
    });
    assertFalse(values3.hasNext());

    filter.setValue(false).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueStringTest() {
    TestStringValueStream val = new TestStringValueStream();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream uncasted = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // No values
    val.setValues();
    filter.setExists(false);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues("abc", "123", "def", "456");
    filter.setExists(false);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    filter.setValue(true).setExists(true);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues("abcdef");

    filter.setValue(true).setExists(true);
    Iterator<String> values1 = Arrays.asList("abcdef").iterator();
    func.streamStrings( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    filter.setValue(false).setExists(true);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<String> values2 = Arrays.asList("abcdef").iterator();
    func.streamStrings( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    // Multiple values
    val.setValues("abc", "123", "def", "456");

    filter.setValue(false).setExists(true);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<String> values3 = Arrays.asList("abc", "123", "def", "456").iterator();
    func.streamStrings( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());

    filter.setValue(false).setExists(true);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueObjectTest() {
    TestAnalyticsValueStream val = new TestAnalyticsValueStream();
    TestBooleanValue filter = new TestBooleanValue();

    AnalyticsValueStream func = FilterFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filter});

    // No values
    val.setValues();
    filter.setExists(false);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues(3, "3", new Date(3));
    filter.setExists(false);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    filter.setValue(true).setExists(true);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    Object obj = new TestAnalyticsValueStream();
    val.setValues(obj);

    filter.setValue(true).setExists(true);
    Iterator<Object> values1 = Arrays.<Object>asList(obj).iterator();
    func.streamObjects( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    filter.setValue(false).setExists(true);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Object> values2 = Arrays.<Object>asList(obj).iterator();
    func.streamObjects( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    // Multiple values
    val.setValues(3, "3", new Date(3));

    filter.setValue(false).setExists(true);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    filter.setValue(true).setExists(true);
    Iterator<Object> values3 = Arrays.<Object>asList(3, "3", new Date(3)).iterator();
    func.streamObjects( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());

    filter.setValue(false).setExists(true);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }
}
