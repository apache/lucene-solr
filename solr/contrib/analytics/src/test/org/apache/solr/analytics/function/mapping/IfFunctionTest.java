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

public class IfFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void castingTest() {
    assertTrue(IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestStringValue(), new TestStringValue()}) instanceof StringValue);

    assertFalse(IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestFloatValue(), new TestDoubleValue()}) instanceof FloatValueStream);

    assertFalse(IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestDateValue(), new TestLongValue()}) instanceof DateValueStream);

    assertFalse(IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestAnalyticsValue(), new TestStringValue()}) instanceof StringValue);

    assertTrue(IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestLongValue(), new TestFloatValue()}) instanceof DoubleValue);

    assertFalse(IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestStringValue(), new TestLongValue()}) instanceof DateValue);

    assertFalse(IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestStringValue(), new TestStringValue()}) instanceof BooleanValueStream);

    assertFalse(IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestIntValueStream(), new TestLongValue()}) instanceof LongValue);
  }

  @Test
  public void singleValueBooleanTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestBooleanValue then = new TestBooleanValue();
    TestBooleanValue els = new TestBooleanValue();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Condition doesn't exist
    then.setExists(false);
    then.setExists(false);
    els.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    cond.setExists(false);
    then.setValue(false).setExists(true);
    els.setValue(true).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setExists(false);
    els.setValue(false).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(true).setExists(true);
    els.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    cond.setValue(true).setExists(true);
    then.setValue(false).setExists(true);
    els.setValue(true).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    cond.setValue(true).setExists(true);
    then.setValue(true).setExists(true);
    els.setValue(false).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(false).setExists(true);
    els.setValue(true).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(true).setExists(true);
    els.setValue(false).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueIntTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestIntValue then = new TestIntValue();
    TestIntValue els = new TestIntValue();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof IntValue);
    IntValue func = (IntValue) uncasted;

    // Condition doesn't exist
    cond.setExists(false);
    then.setExists(false);
    els.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    cond.setExists(false);
    then.setValue(-234).setExists(true);
    els.setValue(765).setExists(true);
    func.getInt();
    assertFalse(func.exists());

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setExists(false);
    els.setValue(23423).setExists(true);
    func.getInt();
    assertFalse(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(745).setExists(true);
    els.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    // Value exists
    cond.setValue(true).setExists(true);
    then.setValue(234).setExists(true);
    els.setValue(-23423).setExists(true);
    assertEquals(234, func.getInt());
    assertTrue(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(234).setExists(true);
    els.setValue(-23423).setExists(true);
    assertEquals(-23423, func.getInt());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueLongTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestLongValue then = new TestLongValue();
    TestLongValue els = new TestLongValue();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof LongValue);
    LongValue func = (LongValue) uncasted;

    // Condition doesn't exist
    cond.setExists(false);
    then.setExists(false);
    els.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    cond.setExists(false);
    then.setValue(-234L).setExists(true);
    els.setValue(765L).setExists(true);
    func.getLong();
    assertFalse(func.exists());

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setExists(false);
    els.setValue(23423L).setExists(true);
    func.getLong();
    assertFalse(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(745L).setExists(true);
    els.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    cond.setValue(true).setExists(true);
    then.setValue(234L).setExists(true);
    els.setValue(-23423L).setExists(true);
    assertEquals(234L, func.getLong());
    assertTrue(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(234l).setExists(true);
    els.setValue(-23423L).setExists(true);
    assertEquals(-23423L, func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueFloatTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestFloatValue then = new TestFloatValue();
    TestFloatValue els = new TestFloatValue();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof FloatValue);
    FloatValue func = (FloatValue) uncasted;

    // Condition doesn't exist
    cond.setExists(false);
    then.setExists(false);
    els.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    cond.setExists(false);
    then.setValue(234.7565F).setExists(true);
    els.setValue(-234.7565F).setExists(true);
    func.getFloat();
    assertFalse(func.exists());

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setExists(false);
    els.setValue(234.7565F).setExists(true);
    func.getFloat();
    assertFalse(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(-23423.234F).setExists(true);
    els.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    // Value exists
    cond.setValue(true).setExists(true);
    then.setValue(234.7565F).setExists(true);
    els.setValue(-23423.234F).setExists(true);
    assertEquals(234.7565F, func.getFloat(), .0000001);
    assertTrue(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(234.7565F).setExists(true);
    els.setValue(-23423.234F).setExists(true);
    assertEquals(-23423.234F, func.getFloat(), .0000001);
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDoubleTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestDoubleValue then = new TestDoubleValue();
    TestDoubleValue els = new TestDoubleValue();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Condition doesn't exist
    cond.setExists(false);
    then.setExists(false);
    els.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    cond.setExists(false);
    then.setValue(234.7565).setExists(true);
    els.setValue(-234.7565).setExists(true);
    func.getDouble();
    assertFalse(func.exists());

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setExists(false);
    els.setValue(234.7565).setExists(true);
    func.getDouble();
    assertFalse(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(-23423.234).setExists(true);
    els.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Value exists
    cond.setValue(true).setExists(true);
    then.setValue(234.7565).setExists(true);
    els.setValue(-23423.234).setExists(true);
    assertEquals(234.7565, func.getDouble(), .0000001);
    assertTrue(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(234.7565).setExists(true);
    els.setValue(-23423.234).setExists(true);
    assertEquals(-23423.234, func.getDouble(), .0000001);
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDateTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1810-12-02T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1950-02-23T14:54:34Z"));
    Date date3 = Date.from(Instant.parse("2023-11-01T20:30:15Z"));

    TestBooleanValue cond = new TestBooleanValue();
    TestDateValue then = new TestDateValue();
    TestDateValue els = new TestDateValue();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof DateValue);
    DateValue func = (DateValue) uncasted;

    // Condition doesn't exist
    cond.setExists(false);
    then.setExists(false);
    els.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    cond.setExists(false);
    then.setValue("6723-08-01T23:30:15Z").setExists(true);
    els.setValue("2207-11-01T20:30:15Z").setExists(true);
    func.getLong();
    assertFalse(func.exists());

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setExists(false);
    els.setValue("1810-12-02T10:30:15Z").setExists(true);
    func.getLong();
    assertFalse(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue("2023-11-01T20:30:15Z").setExists(true);
    els.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    cond.setValue(true).setExists(true);
    then.setValue("1810-12-02T10:30:15Z").setExists(true);
    els.setValue("2023-11-01T20:30:15Z").setExists(true);
    assertEquals(date1.getTime(), func.getLong());
    assertTrue(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue("1810-12-02T10:30:15Z").setExists(true);
    els.setValue("2023-11-01T20:30:15Z").setExists(true);
    assertEquals(date3.getTime(), func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueStringTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestStringValue then = new TestStringValue();
    TestStringValue els = new TestStringValue();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // Condition doesn't exist
    cond.setExists(false);
    then.setExists(false);
    els.setExists(false);
    func.getString();
    assertFalse(func.exists());

    cond.setExists(false);
    then.setValue("if the value is true").setExists(true);
    els.setValue("when the value is false").setExists(true);
    func.getString();
    assertFalse(func.exists());

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setExists(false);
    els.setValue("not picked").setExists(true);
    func.getString();
    assertFalse(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue("also not picked").setExists(true);
    els.setExists(false);
    func.getString();
    assertFalse(func.exists());

    // Value exists
    cond.setValue(true).setExists(true);
    then.setValue("abc123").setExists(true);
    els.setValue("def456").setExists(true);
    assertEquals("abc123", func.getString());
    assertTrue(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue("abc123").setExists(true);
    els.setValue("def456").setExists(true);
    assertEquals("def456", func.getString());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueObjectTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestAnalyticsValue then = new TestAnalyticsValue();
    TestAnalyticsValue els = new TestAnalyticsValue();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof AnalyticsValue);
    AnalyticsValue func = (AnalyticsValue) uncasted;

    // Condition doesn't exist
    cond.setExists(false);
    then.setExists(false);
    els.setExists(false);
    func.getObject();
    assertFalse(func.exists());

    cond.setExists(false);
    then.setValue("if the value is true").setExists(true);
    els.setValue(Boolean.FALSE).setExists(true);
    func.getObject();
    assertFalse(func.exists());

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setExists(false);
    els.setValue(123421324L).setExists(true);
    func.getObject();
    assertFalse(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue(1234324.454F).setExists(true);
    els.setExists(false);
    func.getObject();
    assertFalse(func.exists());

    // Value exists
    cond.setValue(true).setExists(true);
    then.setValue("abc123").setExists(true);
    els.setValue(new Date(3214)).setExists(true);
    assertEquals("abc123", func.getObject());
    assertTrue(func.exists());

    cond.setValue(false).setExists(true);
    then.setValue("abc123").setExists(true);
    els.setValue(new Date(3214)).setExists(true);
    assertEquals(new Date(3214), func.getObject());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueBooleanTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestBooleanValueStream then = new TestBooleanValueStream();
    TestBooleanValueStream els = new TestBooleanValueStream();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    cond.setExists(false);
    then.setValues();
    els.setValues();
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setExists(false);
    then.setValues(false, true, false);
    els.setValues(true, true, true, false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setValues();
    els.setValues(true, true, true, false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setValue(false).setExists(true);
    then.setValues(true, true, true, false);
    els.setValues();
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    cond.setValue(true).setExists(true);
    then.setValues(true, true, true, false);
    els.setValues(false, false, true, false, true);
    Iterator<Boolean> values1 = Arrays.asList(true, true, true, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    cond.setValue(false).setExists(true);
    then.setValues(true, true, true, false);
    els.setValues(false, false, true, false, true);
    Iterator<Boolean> values2 = Arrays.asList(false, false, true, false, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueIntTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestIntValueStream then = new TestIntValueStream();
    TestIntValueStream els = new TestIntValueStream();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof IntValueStream);
    IntValueStream func = (IntValueStream) uncasted;

    // No values
    cond.setExists(false);
    then.setValues();
    els.setValues();
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setExists(false);
    then.setValues(-132, 41543, 563);
    els.setValues(0, 1, -2, 3);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setValues();
    els.setValues(0, 1, -2, 3);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setValue(false).setExists(true);
    then.setValues(-132, 41543, 563);
    els.setValues();
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    cond.setValue(true).setExists(true);
    then.setValues(-132, 41543, 563);
    els.setValues(0, 1, -2, 3);
    Iterator<Integer> values1 = Arrays.asList(-132, 41543, 563).iterator();
    func.streamInts( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().intValue(), value);
    });
    assertFalse(values1.hasNext());

    cond.setValue(false).setExists(true);
    then.setValues(-132, 41543, 563);
    els.setValues(0, 1, -2, 3);
    Iterator<Integer> values2 = Arrays.asList(0, 1, -2, 3).iterator();
    func.streamInts( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().intValue(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueLongTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestLongValueStream then = new TestLongValueStream();
    TestLongValueStream els = new TestLongValueStream();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof LongValueStream);
    LongValueStream func = (LongValueStream) uncasted;

    // No values
    cond.setExists(false);
    then.setValues();
    els.setValues();
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setExists(false);
    then.setValues(-132L, 41543L, 563L);
    els.setValues(0L, 1L, -2L, 3L);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setValues();
    els.setValues(0L, 1L, -2L, 3L);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setValue(false).setExists(true);
    then.setValues(-132L, 41543L, 563L);
    els.setValues();
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    cond.setValue(true).setExists(true);
    then.setValues(-132L, 41543L, 563L);
    els.setValues(0L, 1L, -2L, 3L);
    Iterator<Long> values1 = Arrays.asList(-132L, 41543L, 563L).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    cond.setValue(false).setExists(true);
    then.setValues(-132L, 41543L, 563L);
    els.setValues(0L, 1L, -2L, 3L);
    Iterator<Long> values2 = Arrays.asList(0L, 1L, -2L, 3L).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueFloatTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestFloatValueStream then = new TestFloatValueStream();
    TestFloatValueStream els = new TestFloatValueStream();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof FloatValueStream);
    FloatValueStream func = (FloatValueStream) uncasted;

    // No values
    cond.setExists(false);
    then.setValues();
    els.setValues();
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setExists(false);
    then.setValues(2134.2345F, -234.23F, 20000.0F);
    els.setValues(.1111F , -.22222F, .333F);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setValues();
    els.setValues(.1111F , -.22222F, .333F);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setValue(false).setExists(true);
    then.setValues(2134.2345F, -234.23F, 20000.0F);
    els.setValues();
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    cond.setValue(true).setExists(true);
    then.setValues(2134.2345F, -234.23F, 20000.0F);
    els.setValues(.1111F , -.22222F, .333F);
    Iterator<Float> values1 = Arrays.asList(2134.2345F, -234.23F, 20000.0F).iterator();
    func.streamFloats( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .000001);
    });
    assertFalse(values1.hasNext());

    cond.setValue(false).setExists(true);
    then.setValues(2134.2345F, -234.23F, 20000.0F);
    els.setValues(.1111F , -.22222F, .333F);
    Iterator<Float> values2 = Arrays.asList(.1111F , -.22222F, .333F).iterator();
    func.streamFloats( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .000001);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueDoubleTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestDoubleValueStream then = new TestDoubleValueStream();
    TestDoubleValueStream els = new TestDoubleValueStream();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values
    cond.setExists(false);
    then.setValues();
    els.setValues();
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setExists(false);
    then.setValues(2134.2345, -234.23, 20000.0);
    els.setValues(.1111 , -.22222, .333);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setValues();
    els.setValues(.1111 , -.22222, .333);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setValue(false).setExists(true);
    then.setValues(2134.2345, -234.23, 20000.0);
    els.setValues();
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    cond.setValue(true).setExists(true);
    then.setValues(2134.2345, -234.23, 20000.0);
    els.setValues(.1111 , -.22222, .333);
    Iterator<Double> values1 = Arrays.asList(2134.2345, -234.23, 20000.0).iterator();
    func.streamDoubles( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .000001);
    });
    assertFalse(values1.hasNext());

    cond.setValue(false).setExists(true);
    then.setValues(2134.2345, -234.23, 20000.0);
    els.setValues(.1111 , -.22222, .333);
    Iterator<Double> values2 = Arrays.asList(.1111 , -.22222, .333).iterator();
    func.streamDoubles( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .000001);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueDateTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1810-12-02T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1931-03-16T18:15:45Z"));
    Date date3 = Date.from(Instant.parse("2023-11-01T20:30:15Z"));

    TestBooleanValue cond = new TestBooleanValue();
    TestDateValueStream then = new TestDateValueStream();
    TestDateValueStream els = new TestDateValueStream();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof DateValueStream);
    DateValueStream func = (DateValueStream) uncasted;

    // No values
    cond.setExists(false);
    then.setValues();
    els.setValues();
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setExists(false);
    then.setValues("1810-12-02T10:30:15Z", "2023-11-01T20:30:15Z", "1810-12-02T10:30:15Z");
    els.setValues("2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setValues();
    els.setValues("2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setValue(false).setExists(true);
    then.setValues("1810-12-02T10:30:15Z", "2023-11-01T20:30:15Z", "1810-12-02T10:30:15Z");
    els.setValues();
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    cond.setValue(true).setExists(true);
    then.setValues("1810-12-02T10:30:15Z", "2023-11-01T20:30:15Z", "1810-12-02T10:30:15Z");
    els.setValues("2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    Iterator<Long> values1 = Arrays.asList(date1.getTime(), date3.getTime(), date1.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    cond.setValue(false).setExists(true);
    then.setValues("1810-12-02T10:30:15Z", "2023-11-01T20:30:15Z", "1810-12-02T10:30:15Z");
    els.setValues("2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    Iterator<Long> values2 = Arrays.asList(date3.getTime(), date2.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueStringTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestStringValueStream then = new TestStringValueStream();
    TestStringValueStream els = new TestStringValueStream();

    AnalyticsValueStream uncasted = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // No values
    cond.setExists(false);
    then.setValues();
    els.setValues();
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setExists(false);
    then.setValues("abc123", "abcsdafasd", "third");
    els.setValues("this", "is", "a", "sentence");
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setValues();
    els.setValues("this", "is", "a", "sentence");
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setValue(false).setExists(true);
    then.setValues("abc123", "abcsdafasd", "third");
    els.setValues();
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    cond.setValue(true).setExists(true);
    then.setValues("abc123", "abcsdafasd", "third");
    els.setValues("this", "is", "a", "sentence");
    Iterator<String> values1 = Arrays.asList("abc123", "abcsdafasd", "third").iterator();
    func.streamStrings( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    cond.setValue(false).setExists(true);
    then.setValues("abc123", "abcsdafasd", "third");
    els.setValues("this", "is", "a", "sentence");
    Iterator<String> values2 = Arrays.asList("this", "is", "a", "sentence").iterator();
    func.streamStrings( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueObjectTest() {
    TestBooleanValue cond = new TestBooleanValue();
    TestAnalyticsValueStream then = new TestAnalyticsValueStream();
    TestAnalyticsValueStream els = new TestAnalyticsValueStream();

    AnalyticsValueStream func = IfFunction.creatorFunction.apply(new AnalyticsValueStream[] {cond, then, els});

    // No values
    cond.setExists(false);
    then.setValues();
    els.setValues();
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setExists(false);
    then.setValues(new Date(142341), "abcsdafasd", 1234.1324123);
    els.setValues(324923.0234F, 123, Boolean.TRUE, "if statement");
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Result doesn't exist
    cond.setValue(true).setExists(true);
    then.setValues();
    els.setValues(324923.0234F, 123, Boolean.TRUE, "if statement");
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    cond.setValue(false).setExists(true);
    then.setValues(new Date(142341), "abcsdafasd", 1234.1324123);
    els.setValues();
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Values exist
    cond.setValue(true).setExists(true);
    then.setValues(new Date(142341), "abcsdafasd", 1234.1324123);
    els.setValues(324923.0234F, 123, Boolean.TRUE, "if statement");
    Iterator<Object> values1 = Arrays.<Object>asList(new Date(142341), "abcsdafasd", 1234.1324123).iterator();
    func.streamObjects( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    cond.setValue(false).setExists(true);
    then.setValues(new Date(142341), "abcsdafasd", 1234.1324123);
    els.setValues(324923.0234F, 123, Boolean.TRUE, "if statement");
    Iterator<Object> values2 = Arrays.<Object>asList(324923.0234F, 123, Boolean.TRUE, "if statement").iterator();
    func.streamObjects( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }
}
