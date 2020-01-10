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
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValue;
import org.apache.solr.analytics.value.constant.ConstantFloatValue;
import org.junit.Test;

public class CastingFloatValueTest extends SolrTestCaseJ4 {

  @Test
  public void doubleCastingTest() {
    TestFloatValue val = new TestFloatValue();

    assertTrue(val instanceof DoubleValue);
    DoubleValue casted = (DoubleValue)val;

    val.setValue(20F).setExists(true);
    assertEquals(20.0, casted.getDouble(), .00001);
    assertTrue(casted.exists());

    val.setValue(1234F).setExists(true);
    assertEquals(1234.0, casted.getDouble(), .00001);
    assertTrue(casted.exists());
  }

  @Test
  public void stringCastingTest() {
    TestFloatValue val = new TestFloatValue();

    assertTrue(val instanceof StringValue);
    StringValue casted = (StringValue)val;

    val.setValue(20F).setExists(true);
    assertEquals("20.0", casted.getString());
    assertTrue(casted.exists());

    val.setValue(1234F).setExists(true);
    assertEquals("1234.0", casted.getString());
    assertTrue(casted.exists());
  }

  @Test
  public void objectCastingTest() {
    TestFloatValue val = new TestFloatValue();

    assertTrue(val instanceof AnalyticsValue);
    AnalyticsValue casted = (AnalyticsValue)val;

    val.setValue(20F).setExists(true);
    assertEquals(20F, casted.getObject());
    assertTrue(casted.exists());

    val.setValue(1234F).setExists(true);
    assertEquals(1234F, casted.getObject());
    assertTrue(casted.exists());
  }

  @Test
  public void floatStreamCastingTest() {
    TestFloatValue val = new TestFloatValue();

    assertTrue(val instanceof FloatValueStream);
    FloatValueStream casted = (FloatValueStream)val;

    // No values
    val.setExists(false);
    casted.streamFloats( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20F).setExists(true);
    Iterator<Float> values = Arrays.asList(20F).iterator();
    casted.streamFloats( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, .00001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void doubleStreamCastingTest() {
    TestFloatValue val = new TestFloatValue();

    assertTrue(val instanceof DoubleValueStream);
    DoubleValueStream casted = (DoubleValueStream)val;

    // No values
    val.setExists(false);
    casted.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20F).setExists(true);
    Iterator<Double> values = Arrays.asList(20.0).iterator();
    casted.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, .00001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void stringStreamCastingTest() {
    TestFloatValue val = new TestFloatValue();

    assertTrue(val instanceof StringValueStream);
    StringValueStream casted = (StringValueStream)val;

    // No values
    val.setExists(false);
    casted.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20F).setExists(true);
    Iterator<String> values = Arrays.asList("20.0").iterator();
    casted.streamStrings( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void objectStreamCastingTest() {
    TestFloatValue val = new TestFloatValue();

    assertTrue(val instanceof AnalyticsValueStream);
    AnalyticsValueStream casted = (AnalyticsValueStream)val;

    // No values
    val.setExists(false);
    casted.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20F).setExists(true);
    Iterator<Object> values = Arrays.<Object>asList(20F).iterator();
    casted.streamObjects( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void constantConversionTest() {
    TestFloatValue val = new TestFloatValue(ExpressionType.CONST);
    val.setValue(12354.234F).setExists(true);
    AnalyticsValueStream conv = val.convertToConstant();
    assertTrue(conv instanceof ConstantFloatValue);
    assertEquals(12354.234F, ((ConstantFloatValue)conv).getFloat(), .0000001);

    val = new TestFloatValue(ExpressionType.FIELD);
    val.setValue(12354.234F).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestFloatValue(ExpressionType.UNREDUCED_MAPPING);
    val.setValue(12354.234F).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestFloatValue(ExpressionType.REDUCTION);
    val.setValue(12354.234F).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestFloatValue(ExpressionType.REDUCED_MAPPING);
    val.setValue(12354.234F).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);
  }
}
