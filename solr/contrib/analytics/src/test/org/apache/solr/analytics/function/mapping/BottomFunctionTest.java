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
import java.util.Date;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DoubleValue;
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
import org.apache.solr.analytics.value.FloatValue;
import org.apache.solr.analytics.value.IntValue;
import org.apache.solr.analytics.value.LongValue;
import org.apache.solr.analytics.value.StringValue;
import org.junit.Test;

public class BottomFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void multiValueIntTest() {
    TestIntValueStream val = new TestIntValueStream();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof IntValue);
    IntValue func = (IntValue) uncasted;

    // Neither exists
    val.setValues();
    func.getInt();
    assertFalse(func.exists());

    // One exists
    val.setValues(30);
    assertEquals(30, func.getInt());
    assertTrue(func.exists());

    // Both exist
    val.setValues(30, 20, -10, 59);
    assertEquals(-10, func.getInt());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueLongTest() {
    TestLongValueStream val = new TestLongValueStream();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof LongValue);
    LongValue func = (LongValue) uncasted;

    // Neither exists
    val.setValues();
    func.getLong();
    assertFalse(func.exists());

    // One exists
    val.setValues(30L);
    assertEquals(30L, func.getLong());
    assertTrue(func.exists());

    // Both exist
    val.setValues(30L, 20L, -10L, 59L);
    assertEquals(-10L, func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueFloatTest() {
    TestFloatValueStream val = new TestFloatValueStream();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof FloatValue);
    FloatValue func = (FloatValue) uncasted;

    // Neither exists
    val.setValues();
    func.getFloat();
    assertFalse(func.exists());

    // One exists
    val.setValues(30.0F);
    assertEquals(30.0F, func.getFloat(), .000001);
    assertTrue(func.exists());

    // Both exist
    val.setValues(30.5F, 20.01F, -10.49F, -10.48F);
    assertEquals(-10.49F, func.getFloat(), .000001);
    assertTrue(func.exists());
  }

  @Test
  public void multiValueDoubleTest() {
    TestDoubleValueStream val = new TestDoubleValueStream();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Neither exists
    val.setValues();
    func.getDouble();
    assertFalse(func.exists());

    // One exists
    val.setValues(30.0);
    assertEquals(30.0, func.getDouble(), .000001);
    assertTrue(func.exists());

    // Both exist
    val.setValues(30.5, 20.01, -10.49, -10.48);
    assertEquals(-10.49, func.getDouble(), .000001);
    assertTrue(func.exists());
  }

  @Test
  public void multiValueDateTest() throws DateTimeParseException {
    TestDateValueStream val = new TestDateValueStream();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DateValue);
    DateValue func = (DateValue) uncasted;

    // Neither exists
    val.setValues();
    func.getDate();
    assertFalse(func.exists());

    // One exists
    val.setValues("1950-05-03T10:30:50Z");
    assertEquals(Date.from(Instant.parse("1950-05-03T10:30:50Z")), func.getDate());
    assertTrue(func.exists());

    // Both exist
    val.setValues("1950-05-03T10:30:50Z", "2200-01-01T10:00:50Z", "1800-12-31T11:30:50Z", "1930-05-020T10:45:50Z");
    assertEquals(Date.from(Instant.parse("1800-12-31T11:30:50Z")), func.getDate());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueStringTest() {
    TestStringValueStream val = new TestStringValueStream();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // Neither exists
    val.setValues();
    func.getString();
    assertFalse(func.exists());

    // One exists
    val.setValues("abc");
    assertEquals("abc", func.getString());
    assertTrue(func.exists());

    // Both exist
    val.setValues("1abcdef", "abc", "def", "1abc");
    assertEquals("1abc", func.getString());
    assertTrue(func.exists());
  }

  @Test
  public void multipleSingleValueIntTest() {
    TestIntValue val1 = new TestIntValue();
    TestIntValue val2 = new TestIntValue();
    TestIntValue val3 = new TestIntValue();
    TestIntValue val4 = new TestIntValue();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2, val3, val4});
    assertTrue(uncasted instanceof IntValue);
    IntValue func = (IntValue) uncasted;

    // None exist
    val1.setExists(false);
    val2.setExists(false);
    val3.setExists(false);
    val4.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    // Some exist
    val1.setValue(1000).setExists(false);
    val2.setValue(30).setExists(true);
    val3.setValue(-1000).setExists(false);
    val4.setValue(12).setExists(true);
    assertEquals(12, func.getInt());
    assertTrue(func.exists());

    // All exist values, one value
    val1.setValue(45).setExists(true);
    val2.setValue(30).setExists(true);
    val3.setValue(-2).setExists(true);
    val4.setValue(12).setExists(true);
    assertEquals(-2, func.getInt());
    assertTrue(func.exists());
  }

  @Test
  public void multipleSingleValueLongTest() {
    TestLongValue val1 = new TestLongValue();
    TestLongValue val2 = new TestLongValue();
    TestLongValue val3 = new TestLongValue();
    TestLongValue val4 = new TestLongValue();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2, val3, val4});
    assertTrue(uncasted instanceof LongValue);
    LongValue func = (LongValue) uncasted;

    // None exist
    val1.setExists(false);
    val2.setExists(false);
    val3.setExists(false);
    val4.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    // Some exist
    val1.setValue(1000L).setExists(false);
    val2.setValue(30L).setExists(true);
    val3.setValue(-1000L).setExists(false);
    val4.setValue(12L).setExists(true);
    assertEquals(12L, func.getLong());
    assertTrue(func.exists());

    // All exist values, one value
    val1.setValue(45L).setExists(true);
    val2.setValue(30L).setExists(true);
    val3.setValue(-2L).setExists(true);
    val4.setValue(12L).setExists(true);
    assertEquals(-2L, func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void multipleSingleValueFloatTest() {
    TestFloatValue val1 = new TestFloatValue();
    TestFloatValue val2 = new TestFloatValue();
    TestFloatValue val3 = new TestFloatValue();
    TestFloatValue val4 = new TestFloatValue();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2, val3, val4});
    assertTrue(uncasted instanceof FloatValue);
    FloatValue func = (FloatValue) uncasted;

    // None exist
    val1.setExists(false);
    val2.setExists(false);
    val3.setExists(false);
    val4.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    // Some exist
    val1.setValue(1000.1233F).setExists(false);
    val2.setValue(30.34F).setExists(true);
    val3.setValue(-1000.3241F).setExists(false);
    val4.setValue(12.123F).setExists(true);
    assertEquals(12.123F, func.getFloat(), .000001);
    assertTrue(func.exists());

    // All exist values, one value
    val1.setValue(45.43F).setExists(true);
    val2.setValue(30.231F).setExists(true);
    val3.setValue(-2.33F).setExists(true);
    val4.setValue(12.5F).setExists(true);
    assertEquals(-2.33F, func.getFloat(), .000001);
    assertTrue(func.exists());
  }

  @Test
  public void multipleSingleValueDoubleTest() {
    TestDoubleValue val1 = new TestDoubleValue();
    TestDoubleValue val2 = new TestDoubleValue();
    TestDoubleValue val3 = new TestDoubleValue();
    TestDoubleValue val4 = new TestDoubleValue();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2, val3, val4});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // None exist
    val1.setExists(false);
    val2.setExists(false);
    val3.setExists(false);
    val4.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Some exist
    val1.setValue(1000.1233).setExists(false);
    val2.setValue(30.34).setExists(true);
    val3.setValue(-1000.3241).setExists(false);
    val4.setValue(12.123).setExists(true);
    assertEquals(12.123, func.getDouble(), .000001);
    assertTrue(func.exists());

    // All exist values, one value
    val1.setValue(45.43).setExists(true);
    val2.setValue(30.231).setExists(true);
    val3.setValue(-2.33).setExists(true);
    val4.setValue(12.5).setExists(true);
    assertEquals(-2.33, func.getDouble(), .000001);
    assertTrue(func.exists());
  }

  @Test
  public void multipleSingleValueDateTest() throws DateTimeParseException {
    TestDateValue val1 = new TestDateValue();
    TestDateValue val2 = new TestDateValue();
    TestDateValue val3 = new TestDateValue();
    TestDateValue val4 = new TestDateValue();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2, val3, val4});
    assertTrue(uncasted instanceof DateValue);
    DateValue func = (DateValue) uncasted;

    // None exist
    val1.setExists(false);
    val2.setExists(false);
    val3.setExists(false);
    val4.setExists(false);
    func.getDate();
    assertFalse(func.exists());

    // Some exist
    val1.setValue("9999-05-03T10:30:50Z").setExists(false);
    val2.setValue("1950-05-03T10:30:50Z").setExists(true);
    val3.setValue("0000-05-03T10:30:50Z").setExists(false);
    val4.setValue("1850-05-03T10:30:50Z").setExists(true);
    assertEquals(Date.from(Instant.parse("1850-05-03T10:30:50Z")), func.getDate());
    assertTrue(func.exists());

    // All exist values, one value
    val1.setValue("2200-05-03T10:30:50Z").setExists(true);
    val2.setValue("1950-05-03T10:30:50Z").setExists(true);
    val3.setValue("1700-05-03T10:30:50Z").setExists(true);
    val4.setValue("1850-05-03T10:30:50Z").setExists(true);
    assertEquals(Date.from(Instant.parse("1700-05-03T10:30:50Z")), func.getDate());
    assertTrue(func.exists());
  }

  @Test
  public void multipleStringValueDateTest() {
    TestStringValue val1 = new TestStringValue();
    TestStringValue val2 = new TestStringValue();
    TestStringValue val3 = new TestStringValue();
    TestStringValue val4 = new TestStringValue();

    AnalyticsValueStream uncasted = BottomFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2, val3, val4});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // None exist
    val1.setExists(false);
    val2.setExists(false);
    val3.setExists(false);
    val4.setExists(false);
    func.getString();
    assertFalse(func.exists());

    // Some exist
    val1.setValue("abc").setExists(true);
    val2.setValue("1111").setExists(false);
    val3.setValue("asdfads").setExists(true);
    val4.setValue("zzzzzzzz").setExists(false);
    assertEquals("abc", func.getString());
    assertTrue(func.exists());

    // All exist values, one value
    val1.setValue("abc").setExists(true);
    val2.setValue("abc1234").setExists(true);
    val3.setValue("asdfads").setExists(true);
    val4.setValue("fdgsfg").setExists(true);
    assertEquals("abc", func.getString());
    assertTrue(func.exists());
  }
}
