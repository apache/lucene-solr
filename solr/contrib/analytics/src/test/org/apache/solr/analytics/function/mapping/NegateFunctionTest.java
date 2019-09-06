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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream;
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValue;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValue;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValue;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValue;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValue;
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestIntValue;
import org.apache.solr.analytics.value.FillableTestValue.TestIntValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValue;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValueStream;
import org.junit.Test;

public class NegateFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void singleValueBooleanTest() {
    TestBooleanValue val = new TestBooleanValue();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    val.setValue(true).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    val.setValue(false).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueIntTest() {
    TestIntValue val = new TestIntValue();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof IntValue);
    IntValue func = (IntValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    func.getInt();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21).setExists(true);
    assertEquals(-21, func.getInt());
    assertTrue(func.exists());

    val.setValue(-100).setExists(true);
    assertEquals(100, func.getInt());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueLongTest() {
    TestLongValue val = new TestLongValue();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof LongValue);
    LongValue func = (LongValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    func.getLong();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21L).setExists(true);
    assertEquals(-21, func.getLong());
    assertTrue(func.exists());

    val.setValue(-100L).setExists(true);
    assertEquals(100L, func.getLong());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueFloatTest() {
    TestFloatValue val = new TestFloatValue();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof FloatValue);
    FloatValue func = (FloatValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    func.getFloat();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21.56F).setExists(true);
    assertEquals(-21.56F, func.getFloat(), 0.00001);
    assertTrue(func.exists());

    val.setValue(-100F).setExists(true);
    assertEquals(100F, func.getFloat(), 0.00001);
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDoubleTest() {
    TestDoubleValue val = new TestDoubleValue();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21.56).setExists(true);
    assertEquals(-21.56, func.getDouble(), 0.00001);
    assertTrue(func.exists());

    val.setValue(-100.1).setExists(true);
    assertEquals(100.1, func.getDouble(), 0.00001);
    assertTrue(func.exists());
  }

  @Test
  public void multiValueBooleanTest() {
    TestBooleanValueStream val = new TestBooleanValueStream();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val.setValues();
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(true);
    Iterator<Boolean> values1 = Arrays.asList(false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val.setValues(true, false, true, false);
    Iterator<Boolean> values2 = Arrays.asList(false, true, false, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueIntTest() {
    TestIntValueStream val = new TestIntValueStream();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof IntValueStream);
    IntValueStream func = (IntValueStream) uncasted;

    // No values
    val.setValues();
    func.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(-4);
    Iterator<Integer> values1 = Arrays.asList(4).iterator();
    func.streamInts( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().intValue(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val.setValues(4, -10, 50, -74);
    Iterator<Integer> values2 = Arrays.asList(-4, 10, -50, 74).iterator();
    func.streamInts( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().intValue(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueLongTest() {
    TestLongValueStream val = new TestLongValueStream();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof LongValueStream);
    LongValueStream func = (LongValueStream) uncasted;

    // No values
    val.setValues();
    func.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(-4L);
    Iterator<Long> values1 = Arrays.asList(4L).iterator();
    func.streamLongs( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next().longValue(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val.setValues(4L, -10L, 50L, -74L);
    Iterator<Long> values2 = Arrays.asList(-4L, 10L, -50L, 74L).iterator();
    func.streamLongs( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next().longValue(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueFloatTest() {
    TestFloatValueStream val = new TestFloatValueStream();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof FloatValueStream);
    FloatValueStream func = (FloatValueStream) uncasted;

    // No values
    val.setValues();
    func.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(-4.3F);
    Iterator<Float> values1 = Arrays.asList(4.3F).iterator();
    func.streamFloats( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .000001);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val.setValues(4.3F, -10F, 50.75F, -74.4446F);
    Iterator<Float> values2 = Arrays.asList(-4.3F, 10F, -50.75F, 74.4446F).iterator();
    func.streamFloats( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .000001);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multiValueDoubleTest() {
    TestDoubleValueStream val = new TestDoubleValueStream();

    AnalyticsValueStream uncasted = NegateFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values
    val.setValues();
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(-4.3);
    Iterator<Double> values1 = Arrays.asList(4.3).iterator();
    func.streamDoubles( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .000001);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val.setValues(4.3, -10, 50.75, -74.4446);
    Iterator<Double> values2 = Arrays.asList(-4.3, 10.0, -50.75, 74.4446).iterator();
    func.streamDoubles( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .000001);
    });
    assertFalse(values2.hasNext());
  }
}
