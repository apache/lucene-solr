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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.AnalyticsValueStream.ExpressionType;
import org.apache.solr.analytics.value.FillableTestValue.TestIntValue;
import org.apache.solr.analytics.value.constant.ConstantIntValue;
import org.junit.Test;

public class CastingIntValueTest extends SolrTestCaseJ4 {

  @Test
  public void longCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof LongValue);
    LongValue casted = (LongValue)val;

    val.setValue(20).setExists(true);
    assertEquals(20L, casted.getLong());
    assertTrue(casted.exists());

    val.setValue(1234).setExists(true);
    assertEquals(1234L, casted.getLong());
    assertTrue(casted.exists());
  }

  @Test
  public void floatCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof FloatValue);
    FloatValue casted = (FloatValue)val;

    val.setValue(20).setExists(true);
    assertEquals(20F, casted.getFloat(), .00001);
    assertTrue(casted.exists());

    val.setValue(1234).setExists(true);
    assertEquals(1234F, casted.getFloat(), .00001);
    assertTrue(casted.exists());
  }

  @Test
  public void doubleCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof DoubleValue);
    DoubleValue casted = (DoubleValue)val;

    val.setValue(20).setExists(true);
    assertEquals(20.0, casted.getDouble(), .00001);
    assertTrue(casted.exists());

    val.setValue(1234).setExists(true);
    assertEquals(1234.0, casted.getDouble(), .00001);
    assertTrue(casted.exists());
  }

  @Test
  public void stringCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof StringValue);
    StringValue casted = (StringValue)val;

    val.setValue(20).setExists(true);
    assertEquals("20", casted.getString());
    assertTrue(casted.exists());

    val.setValue(1234).setExists(true);
    assertEquals("1234", casted.getString());
    assertTrue(casted.exists());
  }

  @Test
  public void objectCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof AnalyticsValue);
    AnalyticsValue casted = (AnalyticsValue)val;

    val.setValue(20).setExists(true);
    assertEquals(20, casted.getObject());
    assertTrue(casted.exists());

    val.setValue(1234).setExists(true);
    assertEquals(1234, casted.getObject());
    assertTrue(casted.exists());
  }

  @Test
  public void intStreamCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof IntValueStream);
    IntValueStream casted = (IntValueStream)val;

    // No values
    val.setExists(false);
    casted.streamInts( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<Integer> values = Arrays.asList(20).iterator();
    casted.streamInts( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next().intValue(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void longStreamCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof LongValueStream);
    LongValueStream casted = (LongValueStream)val;

    // No values
    val.setExists(false);
    casted.streamLongs( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<Long> values = Arrays.asList(20L).iterator();
    casted.streamLongs( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next().longValue(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void floatStreamCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof FloatValueStream);
    FloatValueStream casted = (FloatValueStream)val;

    // No values
    val.setExists(false);
    casted.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<Float> values = Arrays.asList(20F).iterator();
    casted.streamFloats( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, .00001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void doubleStreamCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof DoubleValueStream);
    DoubleValueStream casted = (DoubleValueStream)val;

    // No values
    val.setExists(false);
    casted.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<Double> values = Arrays.asList(20.0).iterator();
    casted.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, .00001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void stringStreamCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof StringValueStream);
    StringValueStream casted = (StringValueStream)val;

    // No values
    val.setExists(false);
    casted.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<String> values = Arrays.asList("20").iterator();
    casted.streamStrings( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void objectStreamCastingTest() {
    TestIntValue val = new TestIntValue();

    assertTrue(val instanceof AnalyticsValueStream);
    AnalyticsValueStream casted = (AnalyticsValueStream)val;

    // No values
    val.setExists(false);
    casted.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<Object> values = Arrays.<Object>asList(20).iterator();
    casted.streamObjects( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void constantConversionTest() {
    TestIntValue val = new TestIntValue(ExpressionType.CONST);
    val.setValue(1234).setExists(true);
    AnalyticsValueStream conv = val.convertToConstant();
    assertTrue(conv instanceof ConstantIntValue);
    assertEquals(1234, ((ConstantIntValue)conv).getInt());

    val = new TestIntValue(ExpressionType.FIELD);
    val.setValue(1234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestIntValue(ExpressionType.UNREDUCED_MAPPING);
    val.setValue(1234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestIntValue(ExpressionType.REDUCTION);
    val.setValue(1234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestIntValue(ExpressionType.REDUCED_MAPPING);
    val.setValue(1234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);
  }
}
