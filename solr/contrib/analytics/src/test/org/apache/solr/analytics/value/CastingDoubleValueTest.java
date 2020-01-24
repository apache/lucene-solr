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
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValue;
import org.apache.solr.analytics.value.constant.ConstantDoubleValue;
import org.junit.Test;

public class CastingDoubleValueTest extends SolrTestCaseJ4 {

  @Test
  public void stringCastingTest() {
    TestDoubleValue val = new TestDoubleValue();

    assertTrue(val instanceof StringValue);
    StringValue casted = (StringValue)val;

    val.setValue(20.0).setExists(true);
    assertEquals("20.0", casted.getString());
    assertTrue(casted.exists());

    val.setValue(1234.0).setExists(true);
    assertEquals("1234.0", casted.getString());
    assertTrue(casted.exists());
  }

  @Test
  public void objectCastingTest() {
    TestDoubleValue val = new TestDoubleValue();

    assertTrue(val instanceof AnalyticsValue);
    AnalyticsValue casted = (AnalyticsValue)val;

    val.setValue(20.0).setExists(true);
    assertEquals(20.0d, casted.getObject());
    assertTrue(casted.exists());

    val.setValue(1234.0).setExists(true);
    assertEquals(1234.0d, casted.getObject());
    assertTrue(casted.exists());
  }

  @Test
  public void doubleStreamCastingTest() {
    TestDoubleValue val = new TestDoubleValue();

    assertTrue(val instanceof DoubleValueStream);
    DoubleValueStream casted = (DoubleValueStream)val;

    // No values
    val.setExists(false);
    casted.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20.0).setExists(true);
    Iterator<Double> values = Arrays.asList(20.0).iterator();
    casted.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, .00001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void stringStreamCastingTest() {
    TestDoubleValue val = new TestDoubleValue();

    assertTrue(val instanceof StringValueStream);
    StringValueStream casted = (StringValueStream)val;

    // No values
    val.setExists(false);
    casted.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20.0).setExists(true);
    Iterator<String> values = Arrays.asList("20.0").iterator();
    casted.streamStrings( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void objectStreamCastingTest() {
    TestDoubleValue val = new TestDoubleValue();

    assertTrue(val instanceof AnalyticsValueStream);
    AnalyticsValueStream casted = (AnalyticsValueStream)val;

    // No values
    val.setExists(false);
    casted.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(20.0).setExists(true);
    Iterator<Object> values = Arrays.<Object>asList(20.0d).iterator();
    casted.streamObjects( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void constantConversionTest() {
    TestDoubleValue val = new TestDoubleValue(ExpressionType.CONST);
    val.setValue(12354.234).setExists(true);
    AnalyticsValueStream conv = val.convertToConstant();
    assertTrue(conv instanceof ConstantDoubleValue);
    assertEquals(12354.234, ((ConstantDoubleValue)conv).getDouble(), .0000001);

    val = new TestDoubleValue(ExpressionType.FIELD);
    val.setValue(12354.234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestDoubleValue(ExpressionType.UNREDUCED_MAPPING);
    val.setValue(12354.234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestDoubleValue(ExpressionType.REDUCTION);
    val.setValue(12354.234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestDoubleValue(ExpressionType.REDUCED_MAPPING);
    val.setValue(12354.234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);
  }
}
