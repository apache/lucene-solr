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

public class ReplaceFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void castingTest() {
    assertTrue(ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValue(), new TestStringValue(), new TestStringValue()}) instanceof StringValue);

    assertFalse(ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestIntValueStream(), new TestFloatValue(), new TestDoubleValue()}) instanceof FloatValueStream);

    assertFalse(ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestLongValueStream(), new TestDateValue(), new TestLongValue()}) instanceof DateValueStream);

    assertFalse(ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestLongValue(), new TestAnalyticsValue(), new TestStringValue()}) instanceof StringValue);

    assertTrue(ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestIntValue(), new TestLongValue(), new TestFloatValue()}) instanceof DoubleValue);

    assertFalse(ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestDateValue(), new TestStringValue(), new TestLongValue()}) instanceof DateValue);

    assertFalse(ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestBooleanValueStream(), new TestStringValue(), new TestStringValue()}) instanceof BooleanValueStream);

    assertFalse(ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestDoubleValue(), new TestIntValue(), new TestLongValue()}) instanceof LongValue);
  }

  @Test
  public void singleValueBooleanTest() {
    TestBooleanValue val = new TestBooleanValue();
    TestBooleanValue comp = new TestBooleanValue();
    TestBooleanValue fill = new TestBooleanValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    comp.setExists(false);
    fill.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    val.setExists(false);
    comp.setValue(false).setExists(true);
    fill.setValue(true).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    // Comp doesn't exist
    val.setValue(true).setExists(true);
    comp.setExists(false);
    fill.setValue(false).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    // Value exists
    val.setValue(true).setExists(true);
    comp.setValue(false).setExists(true);
    fill.setValue(false).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(true).setExists(true);
    comp.setValue(true).setExists(true);
    fill.setValue(false).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(false).setExists(true);
    comp.setValue(false).setExists(true);
    fill.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());
  }

  @Test
  public void singleValueIntTest() {
    TestIntValue val = new TestIntValue();
    TestIntValue comp = new TestIntValue();
    TestIntValue fill = new TestIntValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof IntValue);
    IntValue func = (IntValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    comp.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    val.setExists(false);
    comp.setValue(-234).setExists(true);
    fill.setValue(765).setExists(true);
    func.getInt();
    assertFalse(func.exists());

    // Comp doesn't exist
    val.setValue(745).setExists(true);
    comp.setExists(false);
    fill.setValue(23423).setExists(true);
    assertEquals(745, func.getInt());
    assertTrue(func.exists());

    // Value exists
    // Comp != Val
    val.setValue(21).setExists(true);
    comp.setValue(234).setExists(true);

    fill.setValue(23423).setExists(true);
    assertEquals(21, func.getInt());
    assertTrue(func.exists());

    fill.setExists(false);
    assertEquals(21, func.getInt());
    assertTrue(func.exists());

    // Comp == Val
    val.setValue(-154).setExists(true);
    comp.setValue(-154).setExists(true);

    fill.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    fill.setValue(23423).setExists(true);
    assertEquals(23423, func.getInt());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueLongTest() {
    TestLongValue val = new TestLongValue();
    TestLongValue comp = new TestLongValue();
    TestLongValue fill = new TestLongValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof LongValue);
    LongValue func = (LongValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    comp.setExists(false);
    fill.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    val.setExists(false);
    comp.setValue(234L).setExists(true);
    fill.setValue(232584L).setExists(true);
    func.getLong();
    assertFalse(func.exists());

    // Comp doesn't exist
    val.setValue(745L).setExists(true);
    comp.setExists(false);
    fill.setValue(23423L).setExists(true);
    assertEquals(745L, func.getLong());
    assertTrue(func.exists());

    // Value exists
    // Comp != Val
    val.setValue(21L).setExists(true);
    comp.setValue(234L).setExists(true);

    fill.setValue(23423L).setExists(true);
    assertEquals(21L, func.getLong());
    assertTrue(func.exists());

    fill.setExists(false);
    assertEquals(21L, func.getLong());
    assertTrue(func.exists());

    // Comp == Val
    val.setValue(-154L).setExists(true);
    comp.setValue(-154L).setExists(true);

    fill.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    fill.setValue(23423L).setExists(true);
    assertEquals(23423L, func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueFloatTest() {
    TestFloatValue val = new TestFloatValue();
    TestFloatValue comp = new TestFloatValue();
    TestFloatValue fill = new TestFloatValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof FloatValue);
    FloatValue func = (FloatValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    comp.setExists(false);
    fill.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    val.setExists(false);
    comp.setValue(3124123.32F).setExists(true);
    fill.setValue(-32473.336F).setExists(true);
    func.getFloat();
    assertFalse(func.exists());

    // Comp doesn't exist
    val.setValue(-745.234F).setExists(true);
    comp.setExists(false);
    fill.setValue(23423.324F).setExists(true);
    assertEquals(-745.234F, func.getFloat(), .0000001);
    assertTrue(func.exists());

    // Value exists
    // Comp != Val
    val.setValue(3423.304F).setExists(true);
    comp.setValue(3423.303F).setExists(true);

    fill.setValue(-23.764F).setExists(true);
    assertEquals(3423.304F, func.getFloat(), .0000001);
    assertTrue(func.exists());

    fill.setExists(false);
    assertEquals(3423.304F, func.getFloat(), .0000001);
    assertTrue(func.exists());

    // Comp == Val
    val.setValue(-154.45F).setExists(true);
    comp.setValue(-154.45F).setExists(true);

    fill.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    fill.setValue(4353434.234F).setExists(true);
    assertEquals(4353434.234F, func.getFloat(), .0000001);
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDoubleTest() {
    TestDoubleValue val = new TestDoubleValue();
    TestDoubleValue comp = new TestDoubleValue();
    TestDoubleValue fill = new TestDoubleValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    comp.setExists(false);
    fill.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    val.setExists(false);
    comp.setValue(3124123.32).setExists(true);
    fill.setValue(-13242.34).setExists(true);
    func.getDouble();
    assertFalse(func.exists());

    // Comp doesn't exist
    val.setValue(-745.234).setExists(true);
    comp.setExists(false);
    fill.setValue(23423.324).setExists(true);
    assertEquals(-745.234, func.getDouble(), .0000001);
    assertTrue(func.exists());

    // Value exists
    // Comp != Val
    val.setValue(34923.304).setExists(true);
    comp.setValue(34923.303).setExists(true);

    fill.setValue(-23.764).setExists(true);
    assertEquals(34923.304, func.getDouble(), .0000001);
    assertTrue(func.exists());

    fill.setExists(false);
    assertEquals(34923.304, func.getDouble(), .0000001);
    assertTrue(func.exists());

    // Comp == Val
    val.setValue(-154.45).setExists(true);
    comp.setValue(-154.45).setExists(true);

    fill.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    fill.setValue(4353434.234).setExists(true);
    assertEquals(4353434.234, func.getDouble(), .0000001);
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDateTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1810-12-02T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1950-02-23T14:54:34Z"));

    TestDateValue val = new TestDateValue();
    TestDateValue comp = new TestDateValue();
    TestDateValue fill = new TestDateValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof DateValue);
    DateValue func = (DateValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    comp.setExists(false);
    fill.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    val.setExists(false);
    comp.setValue("1950-02-23T14:54:34Z").setExists(true);
    fill.setValue("1350-02-26T14:34:34Z").setExists(true);
    func.getLong();
    assertFalse(func.exists());

    // Comp doesn't exist
    val.setValue("1950-02-23T14:54:34Z").setExists(true);
    comp.setExists(false);
    fill.setValue("1350-02-26T14:34:34Z").setExists(true);
    assertEquals(date2.getTime(), func.getLong());
    assertTrue(func.exists());

    // Value exists
    // Comp != Val
    val.setValue("1950-02-23T14:54:34Z").setExists(true);
    comp.setValue("1350-02-26T14:34:34Z").setExists(true);

    fill.setValue("2023-11-01T20:30:15Z").setExists(true);
    assertEquals(date2.getTime(), func.getLong());
    assertTrue(func.exists());

    fill.setExists(false);
    assertEquals(date2.getTime(), func.getLong());
    assertTrue(func.exists());

    // Comp == Val
    val.setValue("2023-11-01T20:30:15Z").setExists(true);
    comp.setValue("2023-11-01T20:30:15Z").setExists(true);

    fill.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    fill.setValue("1810-12-02T10:30:15Z").setExists(true);
    assertEquals(date1.getTime(), func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueStringTest() {
    TestStringValue val = new TestStringValue();
    TestStringValue comp = new TestStringValue();
    TestStringValue fill = new TestStringValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    comp.setExists(false);
    fill.setExists(false);
    func.getString();
    assertFalse(func.exists());

    val.setExists(false);
    comp.setValue("abc456").setExists(true);
    fill.setValue("not touched").setExists(true);
    func.getString();
    assertFalse(func.exists());

    // Comp doesn't exist
    val.setValue("abcd").setExists(true);
    comp.setExists(false);
    fill.setValue("12234").setExists(true);
    assertEquals("abcd", func.getString());
    assertTrue(func.exists());

    // Value exists
    // Comp != Val
    val.setValue("original").setExists(true);
    comp.setValue("different").setExists(true);

    fill.setValue("anything").setExists(true);
    assertEquals("original", func.getString());
    assertTrue(func.exists());

    fill.setExists(false);
    assertEquals("original", func.getString());
    assertTrue(func.exists());

    // Comp == Val
    val.setValue("the same").setExists(true);
    comp.setValue("the same").setExists(true);

    fill.setExists(false);
    func.getString();
    assertFalse(func.exists());

    fill.setValue("replacement").setExists(true);
    assertEquals("replacement", func.getString());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueObjectTest() {
    TestAnalyticsValue val = new TestAnalyticsValue();
    TestAnalyticsValue comp = new TestAnalyticsValue();
    TestAnalyticsValue fill = new TestAnalyticsValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof AnalyticsValue);
    AnalyticsValue func = (AnalyticsValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    comp.setExists(false);
    fill.setExists(false);
    func.getObject();
    assertFalse(func.exists());

    val.setExists(false);
    comp.setValue(Boolean.TRUE).setExists(true);
    fill.setValue("abc").setExists(true);
    func.getObject();
    assertFalse(func.exists());

    // Comp doesn't exist
    val.setValue("abcd").setExists(true);
    comp.setExists(false);
    fill.setValue(new Date(123321)).setExists(true);
    assertEquals("abcd", func.getObject());
    assertTrue(func.exists());

    // Value exists
    // Comp != Val
    val.setValue(new Date(1234)).setExists(true);
    comp.setValue(1234).setExists(true);

    fill.setValue("not used").setExists(true);
    assertEquals(new Date(1234), func.getObject());
    assertTrue(func.exists());

    fill.setExists(false);
    assertEquals(new Date(1234), func.getObject());
    assertTrue(func.exists());

    // Comp == Val
    val.setValue(2342.324F).setExists(true);
    comp.setValue(2342.324F).setExists(true);

    fill.setExists(false);
    func.getObject();
    assertFalse(func.exists());

    fill.setValue("replacement").setExists(true);
    assertEquals("replacement", func.getObject());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueBooleanTest() {
    TestBooleanValueStream val = new TestBooleanValueStream();
    TestBooleanValue comp = new TestBooleanValue();
    TestBooleanValue fill = new TestBooleanValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val.setValues();
    comp.setExists(false);
    fill.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    comp.setValue(true).setExists(true);
    fill.setValue(false).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Comp doesn't exist
    val.setValues(false, true, false, true, true);
    comp.setExists(false);
    fill.setValue(true).setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(false, true, false, true, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Values exist
    val.setValues(false, true, false, true, true);
    comp.setValue(true).setExists(true);
    fill.setValue(false).setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(false, false, false, false, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(false, true, false, true, true);
    comp.setValue(false).setExists(true);
    fill.setExists(false);
    Iterator<Boolean> values3 = Arrays.asList(true, true, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueIntTest() {
    TestIntValueStream val = new TestIntValueStream();
    TestIntValue comp = new TestIntValue();
    TestIntValue fill = new TestIntValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof IntValueStream);
    IntValueStream func = (IntValueStream) uncasted;

    // No values
    val.setValues();
    comp.setExists(false);
    fill.setExists(false);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    comp.setValue(324).setExists(true);
    fill.setValue(52341).setExists(true);
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Comp doesn't exist
    val.setValues(1, 234, -234, 4439, -234, -3245);
    comp.setExists(false);
    fill.setValue(52135).setExists(true);
    Iterator<Integer> values1 = Arrays.asList(1, 234, -234, 4439, -234, -3245).iterator();
    func.streamInts( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().intValue(), value);
    });
    assertFalse(values1.hasNext());

    // Values exist
    val.setValues(1, 234, -234, 4439, -234, -3245);
    comp.setValue(-234).setExists(true);
    fill.setExists(false);
    Iterator<Integer> values2 = Arrays.asList(1, 234, 4439, -3245).iterator();
    func.streamInts( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().intValue(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(1, 234, -234, 4439, -234, -3245);
    comp.setValue(4439).setExists(true);
    fill.setValue(-1000).setExists(true);
    Iterator<Integer> values3 = Arrays.asList(1, 234, -234, -1000, -234, -3245).iterator();
    func.streamInts( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().intValue(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueLongTest() {
    TestLongValueStream val = new TestLongValueStream();
    TestLongValue comp = new TestLongValue();
    TestLongValue fill = new TestLongValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof LongValueStream);
    LongValueStream func = (LongValueStream) uncasted;

    // No values
    val.setValues();
    comp.setExists(false);
    fill.setExists(false);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    comp.setValue(2323L).setExists(true);
    fill.setValue(-5943L).setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Comp doesn't exist
    val.setValues(1L, 234L, -234L, 4439L, -234L, -3245L);
    comp.setExists(false);
    fill.setValue(52135L).setExists(true);
    Iterator<Long> values1 = Arrays.asList(1L, 234L, -234L, 4439L, -234L, -3245L).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    // Values exist
    val.setValues(1L, 234L, -234L, 4439L, -234L, -3245L);
    comp.setValue(-234L).setExists(true);
    fill.setExists(false);
    Iterator<Long> values2 = Arrays.asList(1L, 234L, 4439L, -3245L).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues(1L, 234L, -234L, 4439L, -234L, -3245L);
    comp.setValue(4439L).setExists(true);
    fill.setValue(-1000L).setExists(true);
    Iterator<Long> values3 = Arrays.asList(1L, 234L, -234L, -1000L, -234L, -3245L).iterator();
    func.streamLongs( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().longValue(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueFloatTest() {
    TestFloatValueStream val = new TestFloatValueStream();
    TestFloatValue comp = new TestFloatValue();
    TestFloatValue fill = new TestFloatValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof FloatValueStream);
    FloatValueStream func = (FloatValueStream) uncasted;

    // No values
    val.setValues();
    comp.setExists(false);
    fill.setExists(false);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    comp.setValue(-230.32F).setExists(true);
    fill.setValue(9459.3458F).setExists(true);
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Comp doesn't exist
    val.setValues(1423.3F, 423.4323F, -2349.2F, -343.43934F, 423.4323F);
    comp.setExists(false);
    fill.setValue(234.234F).setExists(true);
    Iterator<Float> values1 = Arrays.asList(1423.3F, 423.4323F, -2349.2F, -343.43934F, 423.4323F).iterator();
    func.streamFloats( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .000001);
    });
    assertFalse(values1.hasNext());

    // Values exist
    val.setValues(1423.3F, 423.4323F, -2349.2F, -343.43934F, 423.4323F);
    comp.setValue(423.4323F).setExists(true);
    fill.setExists(false);
    Iterator<Float> values2 = Arrays.asList(1423.3F, -2349.2F, -343.43934F).iterator();
    func.streamFloats( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .000001);
    });
    assertFalse(values2.hasNext());

    val.setValues(1423.3F, 423.4323F, -2349.2F, -343.43934F, 423.4323F);
    comp.setValue(423.4323F).setExists(true);
    fill.setValue(-1000F).setExists(true);
    Iterator<Float> values3 = Arrays.asList(1423.3F, -1000F, -2349.2F, -343.43934F, -1000F).iterator();
    func.streamFloats( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value, .000001);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueDoubleTest() {
    TestDoubleValueStream val = new TestDoubleValueStream();
    TestDoubleValue comp = new TestDoubleValue();
    TestDoubleValue fill = new TestDoubleValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values
    val.setValues();
    comp.setExists(false);
    fill.setExists(false);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    comp.setValue(234237.67).setExists(true);
    fill.setValue(-234.312).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Comp doesn't exist
    val.setValues(1423.3, 423.4323, -2349.2, -343.43934, 423.4323);
    comp.setExists(false);
    fill.setValue(234.234).setExists(true);
    Iterator<Double> values1 = Arrays.asList(1423.3, 423.4323, -2349.2, -343.43934, 423.4323).iterator();
    func.streamDoubles( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .000001);
    });
    assertFalse(values1.hasNext());

    // Values exist
    val.setValues(1423.3, 423.4323, -2349.2, -343.43934, 423.4323);
    comp.setValue(423.4323).setExists(true);
    fill.setExists(false);
    Iterator<Double> values2 = Arrays.asList(1423.3, -2349.2, -343.43934).iterator();
    func.streamDoubles( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .000001);
    });
    assertFalse(values2.hasNext());

    val.setValues(1423.3, 423.4323, -2349.2, -343.43934, 423.4323);
    comp.setValue(423.4323).setExists(true);
    fill.setValue(-1000.0).setExists(true);
    Iterator<Double> values3 = Arrays.asList(1423.3, -1000.0, -2349.2, -343.43934, -1000.0).iterator();
    func.streamDoubles( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value, .000001);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueDateTest() throws DateTimeParseException {
    Date date1 = Date.from(Instant.parse("1810-12-02T10:30:15Z"));
    Date date2 = Date.from(Instant.parse("1931-03-16T18:15:45Z"));
    Date date3 = Date.from(Instant.parse("2023-11-01T20:30:15Z"));

    TestDateValueStream val = new TestDateValueStream();
    TestDateValue comp = new TestDateValue();
    TestDateValue fill = new TestDateValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof DateValueStream);
    DateValueStream func = (DateValueStream) uncasted;

    // No values
    val.setValues();
    comp.setExists(false);
    fill.setExists(false);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    comp.setValue("1700-12-14").setExists(true);
    fill.setValue("3450-04-23").setExists(true);
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Comp doesn't exist
    val.setValues("1810-12-02T10:30:15Z", "1931-03-16T18:15:45Z", "2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    comp.setExists(false);
    fill.setValue("1810-12-02T10:30:15Z").setExists(true);
    Iterator<Long> values1 = Arrays.asList(date1.getTime(), date2.getTime(), date3.getTime(), date2.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    // Values exist
    val.setValues("1810-12-02T10:30:15Z", "1931-03-16T18:15:45Z", "2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    comp.setValue("1931-03-16T18:15:45Z").setExists(true);
    fill.setExists(false);
    Iterator<Long> values2 = Arrays.asList(date1.getTime(), date3.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues("1810-12-02T10:30:15Z", "1931-03-16T18:15:45Z", "2023-11-01T20:30:15Z", "1931-03-16T18:15:45Z");
    comp.setValue("1931-03-16T18:15:45Z").setExists(true);
    fill.setValue("1810-12-02T10:30:15Z").setExists(true);
    Iterator<Long> values3 = Arrays.asList(date1.getTime(), date1.getTime(), date3.getTime(), date1.getTime()).iterator();
    func.streamLongs( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next().longValue(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueStringTest() {
    TestStringValueStream val = new TestStringValueStream();
    TestStringValue comp = new TestStringValue();
    TestStringValue fill = new TestStringValue();

    AnalyticsValueStream uncasted = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // No values
    val.setValues();
    comp.setExists(false);
    fill.setExists(false);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    comp.setValue("ads").setExists(true);
    fill.setValue("empty").setExists(true);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Comp doesn't exist
    val.setValues("abc", "123", "456", "def", "123");
    comp.setExists(false);
    fill.setValue("won't show up").setExists(true);
    Iterator<String> values1 = Arrays.asList("abc", "123", "456", "def", "123").iterator();
    func.streamStrings( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Values exist
    val.setValues("abc", "123", "456", "def", "123");
    comp.setValue("123").setExists(true);
    fill.setExists(false);
    Iterator<String> values2 = Arrays.asList("abc", "456", "def").iterator();
    func.streamStrings( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues("abc", "123", "456", "def", "123");
    comp.setValue("456").setExists(true);
    fill.setValue("changed").setExists(true);
    Iterator<String> values3 = Arrays.asList("abc", "123", "changed", "def", "123").iterator();
    func.streamStrings( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());
  }

  @Test
  public void multiValueObjectTest() {
    TestAnalyticsValueStream val = new TestAnalyticsValueStream();
    TestAnalyticsValue comp = new TestAnalyticsValue();
    TestAnalyticsValue fill = new TestAnalyticsValue();

    AnalyticsValueStream func = ReplaceFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, comp, fill});

    // No values
    val.setValues();
    comp.setExists(false);
    fill.setExists(false);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val.setValues();
    comp.setValue("doesn't matter").setExists(true);
    fill.setValue("won't show up").setExists(true);
    func.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Comp doesn't exist
    val.setValues("asdfs", new Date(12312), 213123L, new Date(12312));
    comp.setExists(false);
    fill.setValue("won't show up").setExists(true);
    Iterator<Object> values1 = Arrays.<Object>asList("asdfs", new Date(12312), 213123L, new Date(12312)).iterator();
    func.streamObjects( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Values exist
    val.setValues("asdfs", new Date(12312), 213123L, new Date(12312));
    comp.setValue("asdfs").setExists(true);
    fill.setExists(false);
    Iterator<Object> values2 = Arrays.<Object>asList(new Date(12312), 213123L, new Date(12312)).iterator();
    func.streamObjects( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());

    val.setValues("asdfs", new Date(12312), 213123L, new Date(12312));
    comp.setValue(new Date(12312)).setExists(true);
    fill.setValue(Boolean.FALSE).setExists(true);
    Iterator<Object> values3 = Arrays.<Object>asList("asdfs", Boolean.FALSE, 213123L, Boolean.FALSE).iterator();
    func.streamObjects( value -> {
      assertTrue(values3.hasNext());
      assertEquals(values3.next(), value);
    });
    assertFalse(values3.hasNext());
  }
}
