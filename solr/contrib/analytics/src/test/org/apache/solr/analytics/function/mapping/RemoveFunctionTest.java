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

public class RemoveFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void castingTest() {
    assertTrue(RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestStringValue()}) instanceof StringValue);

    assertFalse(RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestIntValueStream(), new TestFloatValue()}) instanceof FloatValue);

    assertFalse(RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestLongValueStream(), new TestDateValue()}) instanceof AnalyticsValue);

    assertFalse(RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestLongValue(), new TestAnalyticsValue()}) instanceof StringValue);

    assertTrue(RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestIntValue(), new TestLongValue()}) instanceof DoubleValue);

    assertFalse(RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestDateValue(), new TestStringValue()}) instanceof DateValue);

    assertFalse(RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValueStream(), new TestStringValue()}) instanceof BooleanValueStream);

    assertFalse(RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestDoubleValue(), new TestIntValue()}) instanceof LongValue);
  }

  @Test
  public void singleValueBooleanTest() {
    TestBooleanValue val = new TestBooleanValue();
    TestBooleanValue remover = new TestBooleanValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    remover.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    val.setExists(false);
    remover.setValue(false).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    val.setValue(true).setExists(true);
    remover.setValue(false).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(true).setExists(true);
    remover.setValue(true).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    val.setValue(false).setExists(true);
    remover.setValue(true).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(false).setExists(true);
    remover.setValue(false).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    val.setValue(false).setExists(true);
    remover.setExists(false);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueIntTest() {
    TestIntValue val = new TestIntValue();
    TestIntValue remover = new TestIntValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof IntValue);
    IntValue func = (IntValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    remover.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    val.setExists(false);
    remover.setValue(-234).setExists(true);
    func.getInt();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21).setExists(true);
    remover.setValue(234).setExists(true);
    assertEquals(21, func.getInt());
    assertTrue(func.exists());

    val.setValue(-154).setExists(true);
    remover.setValue(-154).setExists(true);
    func.getInt();
    assertFalse(func.exists());

    val.setValue(52334).setExists(true);
    remover.setExists(false);
    assertEquals(52334, func.getInt());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueLongTest() {
    TestLongValue val = new TestLongValue();
    TestLongValue remover = new TestLongValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof LongValue);
    LongValue func = (LongValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    remover.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    val.setExists(false);
    remover.setValue(234L).setExists(true);
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21L).setExists(true);
    remover.setValue(234L).setExists(true);
    assertEquals(21L, func.getLong());
    assertTrue(func.exists());

    val.setValue(3421L).setExists(true);
    remover.setValue(3421L).setExists(true);
    func.getLong();
    assertFalse(func.exists());

    val.setValue(-52334L).setExists(true);
    remover.setExists(false);
    assertEquals(-52334L, func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueFloatTest() {
    TestFloatValue val = new TestFloatValue();
    TestFloatValue remover = new TestFloatValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof FloatValue);
    FloatValue func = (FloatValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    remover.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    val.setExists(false);
    remover.setValue(3124123.32F).setExists(true);
    func.getFloat();
    assertFalse(func.exists());

    // Value exists
    val.setValue(-21.324F).setExists(true);
    remover.setValue(23423.423342F).setExists(true);
    assertEquals(-21.324F, func.getFloat(), .00000001);
    assertTrue(func.exists());

    val.setValue(84353.452F).setExists(true);
    remover.setValue(84353.452F).setExists(true);
    func.getFloat();
    assertFalse(func.exists());

    val.setValue(2345.345543F).setExists(true);
    remover.setExists(false);
    assertEquals(2345.345543F, func.getFloat(), .00000001);
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDoubleTest() {
    TestDoubleValue val = new TestDoubleValue();
    TestDoubleValue remover = new TestDoubleValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    remover.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    val.setExists(false);
    remover.setValue(3124123.32).setExists(true);
    func.getDouble();
    assertFalse(func.exists());

    // Value exists
    val.setValue(-21.324).setExists(true);
    remover.setValue(23423.423342).setExists(true);
    assertEquals(-21.324, func.getDouble(), .00000001);
    assertTrue(func.exists());

    val.setValue(84353.452).setExists(true);
    remover.setValue(84353.452).setExists(true);
    func.getDouble();
    assertFalse(func.exists());

    val.setValue(2345.345543).setExists(true);
    remover.setExists(false);
    assertEquals(2345.345543, func.getDouble(), .00000001);
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDateTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1810-12-02T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1950-02-23T14:54:34Z"));
    Date date3 = Date.from(Instant.parse("2023-11-01T20:30:15Z"));

    TestDateValue val = new TestDateValue();
    TestDateValue remover = new TestDateValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof DateValue);
    DateValue func = (DateValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    remover.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    val.setExists(false);
    remover.setValue("1950-02-23T14:54:34Z").setExists(true);
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    val.setValue("1810-12-02T10:30:15Z").setExists(true);
    remover.setValue("2023-11-01T20:30:15Z").setExists(true);
    assertEquals(date1.getTime(), func.getLong());
    assertTrue(func.exists());

    val.setValue("1950-02-23T14:54:34Z").setExists(true);
    remover.setValue("1950-02-23T14:54:34Z").setExists(true);
    func.getLong();
    assertFalse(func.exists());

    val.setValue("2023-11-01T20:30:15Z").setExists(true);
    remover.setExists(false);
    assertEquals(date3.getTime(), func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueStringTest() {
    TestStringValue val = new TestStringValue();
    TestStringValue remover = new TestStringValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    remover.setExists(false);
    func.getString();
    assertFalse(func.exists());

    val.setExists(false);
    remover.setValue("abc456").setExists(true);
    func.getString();
    assertFalse(func.exists());

    // Value exists
    val.setValue("abc123").setExists(true);
    remover.setValue("def456").setExists(true);
    assertEquals("abc123", func.getString());
    assertTrue(func.exists());

    val.setValue("this will be removed").setExists(true);
    remover.setValue("this will be removed").setExists(true);
    func.getString();
    assertFalse(func.exists());

    val.setValue("def123").setExists(true);
    remover.setExists(false);
    assertEquals("def123", func.getString());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueObjectTest() {
    TestAnalyticsValue val = new TestAnalyticsValue();
    TestAnalyticsValue remover = new TestAnalyticsValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof AnalyticsValue);
    AnalyticsValue func = (AnalyticsValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    remover.setExists(false);
    func.getObject();
    assertFalse(func.exists());

    val.setExists(false);
    remover.setValue(Boolean.TRUE).setExists(true);
    func.getObject();
    assertFalse(func.exists());

    // Value exists
    val.setValue("abc123").setExists(true);
    remover.setValue(new Date(123)).setExists(true);
    assertEquals("abc123", func.getObject());
    assertTrue(func.exists());

    val.setValue(23423.0d).setExists(true);
    remover.setValue(23423.0d).setExists(true);
    func.getObject();
    assertFalse(func.exists());

    val.setValue(234L).setExists(true);
    remover.setExists(false);
    assertEquals(234L, func.getObject());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueBooleanTest() {
    TestBooleanValueStream val = new TestBooleanValueStream();
    TestBooleanValue remover = new TestBooleanValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val.setValues();
    remover.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    remover.setValue(true).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(false, true, false, true, true);
    remover.setValue(true).setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(false, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues(false, true, false, true, true);
    remover.setExists(false);
    Iterator<Boolean> values2 = Arrays.asList(false, true, false, true, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(false, true, false, true, true);
    remover.setValue(false).setExists(true);
    Iterator<Boolean> values3 = Arrays.asList(true, true, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());

    val.setValues(false, false, false);
    remover.setValue(false).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueIntTest() {
    TestIntValueStream val = new TestIntValueStream();
    TestIntValue remover = new TestIntValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof IntValueStream);
    IntValueStream func = (IntValueStream) uncasted;

    // No values
    val.setValues();
    remover.setExists(false);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    remover.setValue(324).setExists(true);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(1, 234, -234, 4439, -234, -3245);
    remover.setValue(-234).setExists(true);
    Iterator<Integer> values1 = Arrays.asList(1, 234, 4439, -3245).iterator();
    func.streamInts( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().intValue(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues(1, 234, -234, 4439, -234, -3245);
    remover.setExists(false);
    Iterator<Integer> values2 = Arrays.asList(1, 234, -234, 4439, -234, -3245).iterator();
    func.streamInts( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().intValue(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(1, 234, -234, 4439, -234, -3245);
    remover.setValue(100).setExists(true);
    Iterator<Integer> values3 = Arrays.asList(1, 234, -234, 4439, -234, -3245).iterator();
    func.streamInts( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().intValue(), value);
    });
    assertFalse(values3.hasNext());

    val.setValues(1, 1);
    remover.setValue(1).setExists(true);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueLongTest() {
    TestLongValueStream val = new TestLongValueStream();
    TestLongValue remover = new TestLongValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof LongValueStream);
    LongValueStream func = (LongValueStream) uncasted;

    // No values
    val.setValues();
    remover.setExists(false);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    remover.setValue(2323L).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(323L, -9423L, -1234L, 23423L, -1234L, -1234L);
    remover.setValue(-1234L).setExists(true);
    Iterator<Long> values1 = Arrays.asList(323L, -9423L, 23423L).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues(323L, -9423L, -1234L, 23423L, -1234L, -1234L);
    remover.setExists(false);
    Iterator<Long> values2 = Arrays.asList(323L, -9423L, -1234L, 23423L, -1234L, -1234L).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(323L, -9423L, -1234L, 23423L, -1234L, -1234L);
    remover.setValue(100L).setExists(true);
    Iterator<Long> values3 = Arrays.asList(323L, -9423L, -1234L, 23423L, -1234L, -1234L).iterator();
    func.streamLongs( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().longValue(), value);
    });
    assertFalse(values3.hasNext());

    val.setValues(10L);
    remover.setValue(10L).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueFloatTest() {
    TestFloatValueStream val = new TestFloatValueStream();
    TestFloatValue remover = new TestFloatValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof FloatValueStream);
    FloatValueStream func = (FloatValueStream) uncasted;

    // No values
    val.setValues();
    remover.setExists(false);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    remover.setValue(230.32F).setExists(true);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(-1234.9478F, -9423.5F, -1234.9478F, 23423.324F, 942.0F);
    remover.setValue(-1234.9478F).setExists(true);
    Iterator<Float> values1 = Arrays.asList(-9423.5F, 23423.324F, 942.0F).iterator();
    func.streamFloats( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .00000001);
    });
    assertFalse(values1.hasNext());

    val.setValues(-1234.9478F, -9423.5F, -1234.9478F, 23423.324F, 942.0F);
    remover.setExists(false);
    Iterator<Float> values2 = Arrays.asList(-1234.9478F, -9423.5F, -1234.9478F, 23423.324F, 942.0F).iterator();
    func.streamFloats( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .00000001);
    });
    assertFalse(values2.hasNext());

    val.setValues(-1234.9478F, -9423.5F, -1234.9478F, 23423.324F, 942.0F);
    remover.setValue(23423.5845F).setExists(true);
    Iterator<Float> values3= Arrays.asList(-1234.9478F, -9423.5F, -1234.9478F, 23423.324F, 942.0F).iterator();
    func.streamFloats( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value, .00000001);
    });
    assertFalse(values3.hasNext());

    val.setValues(23.56F, 23.56F, 23.56F);
    remover.setValue(23.56F).setExists(true);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueDoubleTest() {
    TestDoubleValueStream val = new TestDoubleValueStream();
    TestDoubleValue remover = new TestDoubleValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values
    val.setValues();
    remover.setExists(false);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    remover.setValue(234237.67).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(323.213, -9423.5, 124544.42);
    remover.setValue(124544.42).setExists(true);
    Iterator<Double> values1 = Arrays.asList(323.213, -9423.5).iterator();
    func.streamDoubles( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .00000001);
    });
    assertFalse(values1.hasNext());

    val.setValues(323.213, -9423.5, 124544.42);
    remover.setExists(false);
    Iterator<Double> values2 = Arrays.asList(323.213, -9423.5, 124544.42).iterator();
    func.streamDoubles( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .00000001);
    });
    assertFalse(values2.hasNext());


    val.setValues(323.213, -9423.5, 124544.42);
    remover.setValue(345.34).setExists(true);
    Iterator<Double> values3 = Arrays.asList(323.213, -9423.5, 124544.42).iterator();
    func.streamDoubles( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value, .00000001);
    });
    assertFalse(values1.hasNext());

    val.setValues(3124.96, 3124.96);
    remover.setValue(3124.96).setExists(true);
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
    TestDateValue remover = new TestDateValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof DateValueStream);
    DateValueStream func = (DateValueStream) uncasted;

    // No values
    val.setValues();
    remover.setExists(false);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    remover.setValue("1700-12-14").setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues("1810-12-02T10:30:15Z", "1931-03-16T18:15:45Z", "2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    remover.setValue("1931-03-16T18:15:45Z").setExists(true);
    Iterator<Long> values1 = Arrays.asList(date1.getTime(), date3.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues("1810-12-02T10:30:15Z", "1931-03-16T18:15:45Z", "2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    remover.setExists(false);
    Iterator<Long> values2 = Arrays.asList(date1.getTime(), date2.getTime(), date3.getTime(), date2.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues("1810-12-02T10:30:15Z", "1931-03-16T18:15:45Z", "2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    remover.setValue("1810-12-02T10:30:16Z").setExists(true);
    Iterator<Long> values3 = Arrays.asList(date1.getTime(), date2.getTime(), date3.getTime(), date2.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().longValue(), value);
    });
    assertFalse(values3.hasNext());

    val.setValues("1810-12-02T10:30:15Z");
    remover.setValue("1810-12-02T10:30:15Z").setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueStringTest() {
    TestStringValueStream val = new TestStringValueStream();
    TestStringValue remover = new TestStringValue();

    AnalyticsValueStream uncasted = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // No values
    val.setValues();
    remover.setExists(false);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    remover.setValue("ads").setExists(true);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues("abc", "123", "456", "abc");
    remover.setValue("abc").setExists(true);
    Iterator<String> values1 = Arrays.asList("123", "456").iterator();
    func.streamStrings( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues("abc", "123", "456", "abc");
    remover.setExists(false);
    Iterator<String> values2 = Arrays.asList("abc", "123", "456", "abc").iterator();
    func.streamStrings( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues("string1", "another string", "the final and last string");
    remover.setValue("not in values").setExists(true);
    Iterator<String> values3 = Arrays.asList("string1", "another string", "the final and last string").iterator();
    func.streamStrings( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());

    val.setValues("abc123", "abc123", "abc123", "abc123");
    remover.setValue("abc123").setExists(true);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }

  @Test
  public void multiValueObjectTest() {
    TestAnalyticsValueStream val = new TestAnalyticsValueStream();
    TestAnalyticsValue remover = new TestAnalyticsValue();

    AnalyticsValueStream func = RemoveFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, remover});

    // No values
    val.setValues();
    remover.setExists(false);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    remover.setValue("doesn't matter").setExists(true);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues("asdfs", new Date(12312), 213123L, new Date(12312));
    remover.setValue(new Date(12312)).setExists(true);
    Iterator<Object> values1 = Arrays.<Object>asList("asdfs", 213123L).iterator();
    func.streamObjects( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues("asdfs", new Date(12312), 213123L, new Date(12312));
    remover.setExists(false);
    Iterator<Object> values2 = Arrays.<Object>asList("asdfs", new Date(12312), 213123L, new Date(12312)).iterator();
    func.streamObjects( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(new Date(3), "3", 3F);
    remover.setValue(new Date(4)).setExists(true);
    Iterator<Object> values3 = Arrays.<Object>asList(new Date(3), "3", 3F).iterator();
    func.streamObjects( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());

    val.setValues(new Date(4));
    remover.setValue(new Date(4)).setExists(true);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });
  }
}
