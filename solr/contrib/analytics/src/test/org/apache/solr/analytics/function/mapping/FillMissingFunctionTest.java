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

public class FillMissingFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void castingTest() {
    assertTrue(FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestStringValue()}) instanceof StringValue);

    assertFalse(FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestIntValue(), new TestFloatValue()}) instanceof IntValue);

    assertFalse(FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestStringValueStream()}) instanceof AnalyticsValue);

    assertFalse(FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestLongValue(), new TestAnalyticsValue()}) instanceof LongValue);

    assertTrue(FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestIntValue(), new TestLongValue()}) instanceof DoubleValue);

    assertFalse(FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestDateValue(), new TestStringValue()}) instanceof DateValue);

    assertFalse(FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValueStream(), new TestStringValue()}) instanceof AnalyticsValue);

    assertTrue(FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestDoubleValue(), new TestIntValueStream()}) instanceof DoubleValueStream);
  }

  @Test
  public void singleValueBooleanTest() {
    TestBooleanValue val = new TestBooleanValue();
    TestBooleanValue filler = new TestBooleanValue();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filler.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    val.setValue(true).setExists(true);
    filler.setValue(false).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(false).setExists(true);
    filler.setValue(false).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(true).setExists(true);
    filler.setExists(false);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(false).setExists(true);
    filler.setExists(false);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    val.setExists(false);
    filler.setValue(false).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    val.setExists(false);
    filler.setValue(true).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueIntTest() {
    TestIntValue val = new TestIntValue();
    TestIntValue filler = new TestIntValue();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof IntValue);
    IntValue func = (IntValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filler.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21).setExists(true);
    filler.setValue(234).setExists(true);
    assertEquals(21, func.getInt());
    assertTrue(func.exists());

    val.setExists(false);
    filler.setValue(-234).setExists(true);
    assertEquals(-234, func.getInt());
    assertTrue(func.exists());

    val.setValue(52334).setExists(true);
    filler.setExists(false);
    assertEquals(52334, func.getInt());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueLongTest() {
    TestLongValue val = new TestLongValue();
    TestLongValue filler = new TestLongValue();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof LongValue);
    LongValue func = (LongValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filler.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21L).setExists(true);
    filler.setValue(234L).setExists(true);
    assertEquals(21L, func.getLong());
    assertTrue(func.exists());

    val.setExists(false);
    filler.setValue(234L).setExists(true);
    assertEquals(234L, func.getLong());
    assertTrue(func.exists());

    val.setValue(-52334L).setExists(true);
    filler.setExists(false);
    assertEquals(-52334L, func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueFloatTest() {
    TestFloatValue val = new TestFloatValue();
    TestFloatValue filler = new TestFloatValue();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof FloatValue);
    FloatValue func = (FloatValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filler.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    // Value exists
    val.setValue(-21.324F).setExists(true);
    filler.setValue(23423.423342F).setExists(true);
    assertEquals(-21.324F, func.getFloat(), .00000001);
    assertTrue(func.exists());

    val.setExists(false);
    filler.setValue(3124123.32F).setExists(true);
    assertEquals(3124123.32F, func.getFloat(), .00000001);
    assertTrue(func.exists());

    val.setValue(2345.345543F).setExists(true);
    filler.setExists(false);
    assertEquals(2345.345543F, func.getFloat(), .00000001);
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDoubleTest() {
    TestDoubleValue val = new TestDoubleValue();
    TestDoubleValue filler = new TestDoubleValue();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filler.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Value exists
    val.setValue(-21.324).setExists(true);
    filler.setValue(23423.423342).setExists(true);
    assertEquals(-21.324, func.getDouble(), .00000001);
    assertTrue(func.exists());

    val.setExists(false);
    filler.setValue(3124123.32).setExists(true);
    assertEquals(3124123.32, func.getDouble(), .00000001);
    assertTrue(func.exists());

    val.setValue(2345.345543).setExists(true);
    filler.setExists(false);
    assertEquals(2345.345543, func.getDouble(), .00000001);
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDateTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1810-12-02T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1950-02-23T14:54:34Z"));
    Date date3 = Date.from(Instant.parse("2023-11-01T20:30:15Z"));

    TestDateValue val = new TestDateValue();
    TestDateValue filler = new TestDateValue();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof DateValue);
    DateValue func = (DateValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filler.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    val.setValue("1810-12-02T10:30:15Z").setExists(true);
    filler.setValue("2023-11-01T20:30:15Z").setExists(true);
    assertEquals(date1.getTime(), func.getLong());
    assertTrue(func.exists());

    val.setExists(false);
    filler.setValue("1950-02-23T14:54:34Z").setExists(true);
    assertEquals(date2.getTime(), func.getLong());
    assertTrue(func.exists());

    val.setValue("2023-11-01T20:30:15Z").setExists(true);
    filler.setExists(false);
    assertEquals(date3.getTime(), func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueStringTest() {
    TestStringValue val = new TestStringValue();
    TestStringValue filler = new TestStringValue();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filler.setExists(false);
    func.getString();
    assertFalse(func.exists());

    // Value exists
    val.setValue("abc123").setExists(true);
    filler.setValue("def456").setExists(true);
    assertEquals("abc123", func.getString());
    assertTrue(func.exists());

    val.setExists(false);
    filler.setValue("abc456").setExists(true);
    assertEquals("abc456", func.getString());
    assertTrue(func.exists());

    val.setValue("def123").setExists(true);
    filler.setExists(false);
    assertEquals("def123", func.getString());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueObjectTest() {
    TestAnalyticsValue val = new TestAnalyticsValue();
    TestAnalyticsValue filler = new TestAnalyticsValue();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof AnalyticsValue);
    AnalyticsValue func = (AnalyticsValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    filler.setExists(false);
    func.getObject();
    assertFalse(func.exists());

    // Value exists
    val.setValue("abc123").setExists(true);
    filler.setValue(new Date(123)).setExists(true);
    assertEquals("abc123", func.getObject());
    assertTrue(func.exists());

    val.setExists(false);
    filler.setValue(Boolean.TRUE).setExists(true);
    assertEquals(Boolean.TRUE, func.getObject());
    assertTrue(func.exists());

    val.setValue(234L).setExists(true);
    filler.setExists(false);
    assertEquals(234L, func.getObject());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueBooleanTest() {
    TestBooleanValueStream val = new TestBooleanValueStream();
    TestBooleanValueStream filler = new TestBooleanValueStream();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val.setValues();
    filler.setValues();
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(true);
    filler.setValues(true, false);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues();
    filler.setValues(true, false);
    Iterator<Boolean> values2 = Arrays.asList(true, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(true, false, true);
    filler.setValues();
    Iterator<Boolean> values3 = Arrays.asList(true, false, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueIntTest() {
    TestIntValueStream val = new TestIntValueStream();
    TestIntValueStream filler = new TestIntValueStream();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof IntValueStream);
    IntValueStream func = (IntValueStream) uncasted;

    // No values
    val.setValues();
    filler.setValues();
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(323, -9423);
    filler.setValues(-1234, 5433, -73843, 1245144);
    Iterator<Integer> values1 = Arrays.asList(323, -9423).iterator();
    func.streamInts( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().intValue(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues();
    filler.setValues(312423, -12343, -23, 2);
    Iterator<Integer> values2 = Arrays.asList(312423, -12343, -23, 2).iterator();
    func.streamInts( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().intValue(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(1423, -413543);
    filler.setValues();
    Iterator<Integer> values3 = Arrays.asList(1423, -413543).iterator();
    func.streamInts( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().intValue(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueLongTest() {
    TestLongValueStream val = new TestLongValueStream();
    TestLongValueStream filler = new TestLongValueStream();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof LongValueStream);
    LongValueStream func = (LongValueStream) uncasted;

    // No values
    val.setValues();
    filler.setValues();
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(323L, -9423L);
    filler.setValues(-1234L, -5433L, -73843L, 1245144L);
    Iterator<Long> values1 = Arrays.asList(323L, -9423L).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues();
    filler.setValues(-312423L, 12343L, 23L, 2L);
    Iterator<Long> values2 = Arrays.asList(-312423L, 12343L, 23L, 2L).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(1423L, -413543L);
    filler.setValues();
    Iterator<Long> values3 = Arrays.asList(1423L, -413543L).iterator();
    func.streamLongs( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().longValue(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueFloatTest() {
    TestFloatValueStream val = new TestFloatValueStream();
    TestFloatValueStream filler = new TestFloatValueStream();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof FloatValueStream);
    FloatValueStream func = (FloatValueStream) uncasted;

    // No values
    val.setValues();
    filler.setValues();
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(323.213F, -9423.5F);
    filler.setValues(-1234.9478F, -5433.234F, -73843F, 1245144.2342F);
    Iterator<Float> values1 = Arrays.asList(323.213F, -9423.5F).iterator();
    func.streamFloats( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .00000001);
    });
    assertFalse(values1.hasNext());

    val.setValues();
    filler.setValues(-312423.2F, 12343.234823F, 23.582F, 2.23F);
    Iterator<Float> values2 = Arrays.asList(-312423.2F, 12343.234823F, 23.582F, 2.23F).iterator();
    func.streamFloats( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .00000001);
    });
    assertFalse(values2.hasNext());

    val.setValues(1423.23039F, -413543F);
    filler.setValues();
    Iterator<Float> values3 = Arrays.asList(1423.23039F, -413543F).iterator();
    func.streamFloats( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value, .00000001);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueDoubleTest() {
    TestDoubleValueStream val = new TestDoubleValueStream();
    TestDoubleValueStream filler = new TestDoubleValueStream();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values
    val.setValues();
    filler.setValues();
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues(323.213, -9423.5);
    filler.setValues(-1234.9478, -5433.234, -73843.0, 1245144.2342);
    Iterator<Double> values1 = Arrays.asList(323.213, -9423.5).iterator();
    func.streamDoubles( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .00000001);
    });
    assertFalse(values1.hasNext());

    val.setValues();
    filler.setValues(-312423.2, 12343.234823, 23.582, 2.23);
    Iterator<Double> values2 = Arrays.asList(-312423.2, 12343.234823, 23.582, 2.23).iterator();
    func.streamDoubles( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .00000001);
    });
    assertFalse(values2.hasNext());

    val.setValues(1423.23039, -413543.0);
    filler.setValues();
    Iterator<Double> values3 = Arrays.asList(1423.23039, -413543.0).iterator();
    func.streamDoubles( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value, .00000001);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueDateTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1810-12-02T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1931-03-16T18:15:45Z"));
    Date date3 = Date.from(Instant.parse("2023-11-01T20:30:15Z"));

    TestDateValueStream val = new TestDateValueStream();
    TestDateValueStream filler = new TestDateValueStream();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof DateValueStream);
    DateValueStream func = (DateValueStream) uncasted;

    // No values
    val.setValues();
    filler.setValues();
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues("1810-12-02T10:30:15Z");
    filler.setValues("1931-03-16T18:15:45Z", "2023-11-01T20:30:15Z");
    Iterator<Long> values1 = Arrays.asList(date1.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues();
    filler.setValues("1931-03-16T18:15:45Z", "1810-12-02T10:30:15Z");
    Iterator<Long> values2 = Arrays.asList(date2.getTime(), date1.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues("2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    filler.setValues();
    Iterator<Long> values3 = Arrays.asList(date3.getTime(), date2.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().longValue(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueStringTest() {
    TestStringValueStream val = new TestStringValueStream();
    TestStringValueStream filler = new TestStringValueStream();

    AnalyticsValueStream uncasted = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // No values
    val.setValues();
    filler.setValues();
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues("abc");
    filler.setValues("123", "def");
    Iterator<String> values1 = Arrays.asList("abc").iterator();
    func.streamStrings( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues();
    filler.setValues("def", "12341");
    Iterator<String> values2 = Arrays.asList("def", "12341").iterator();
    func.streamStrings( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues("string1", "another string", "the final and last string");
    filler.setValues();
    Iterator<String> values3 = Arrays.asList("string1", "another string", "the final and last string").iterator();
    func.streamStrings( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueObjectTest() {
    TestAnalyticsValueStream val = new TestAnalyticsValueStream();
    TestAnalyticsValueStream filler = new TestAnalyticsValueStream();

    AnalyticsValueStream func = FillMissingFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, filler});

    // No values
    val.setValues();
    filler.setValues();
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    val.setValues("asdfs");
    filler.setValues(new Date(12312), 213123L);
    Iterator<Object> values1 = Arrays.<Object>asList("asdfs").iterator();
    func.streamObjects( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    val.setValues();
    filler.setValues(3234.42d, "replacement");
    Iterator<Object> values2 = Arrays.<Object>asList(3234.42d, "replacement").iterator();
    func.streamObjects( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(new Date(3), "3", 3F);
    filler.setValues();
    Iterator<Object> values3 = Arrays.<Object>asList(new Date(3), "3", 3F).iterator();
    func.streamObjects( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());
  }
}
