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
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValue;
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValue;
import org.apache.solr.analytics.value.FillableTestValue.TestIntValue;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValue;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValueStream;
import org.junit.Test;

public class AddFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void twoSingleValueParametersTest() {
    TestIntValue val1 = new TestIntValue();
    TestFloatValue val2 = new TestFloatValue();

    AnalyticsValueStream uncasted = AddFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Neither exists
    val1.setExists(false);
    val2.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // One exists
    val1.setValue(30).setExists(true);
    val2.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Both exist
    val1.setValue(30).setExists(true);
    val2.setValue(21.56F).setExists(true);
    assertEquals(51.56, func.getDouble(), 0.000001);
    assertTrue(func.exists());
  }

  @Test
  public void oneMultiValueParameterTest() {
    TestLongValueStream val = new TestLongValueStream();

    AnalyticsValueStream uncasted = AddFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // No values
    val.setValues();
    func.getDouble();
    assertFalse(func.exists());

    // One value
    val.setValues(30L);
    assertEquals(30, func.getDouble(), 0.000001);
    assertTrue(func.exists());

    // Multiple values
    val.setValues(30L, 20L, 55L, 61L);
    assertEquals(166, func.getDouble(), 0.000001);
    assertTrue(func.exists());
  }

  @Test
  public void oneMultiOneSingleValueParameterTest() {
    TestLongValueStream val1 = new TestLongValueStream();
    TestDoubleValue val2 = new TestDoubleValue();

    AnalyticsValueStream uncasted = AddFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values, One value
    val1.setValues();
    val2.setValue(21.56F).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    val1.setValues(4L, 10023L);
    val2.setExists(false);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    val1.setValues(4L, 10023L, 48L);
    val2.setValue(21.56F).setExists(true);
    Iterator<Double> values = Arrays.asList(25.56, 10044.56, 69.56).iterator();
    func.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, 0.000001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void multipleSingleValueParameterTest() {
    TestLongValue val1 = new TestLongValue();
    TestDoubleValue val2 = new TestDoubleValue();
    TestFloatValue val3 = new TestFloatValue();
    TestIntValue val4 = new TestIntValue();

    AnalyticsValueStream uncasted = AddFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2, val3, val4});
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
    val1.setExists(false);
    val2.setValue(30.56).setExists(true);
    val3.setExists(false);
    val4.setValue(12).setExists(true);
    func.getDouble();
    assertFalse(func.exists());

    // All exist values, one value
    val1.setValue(45L).setExists(true);
    val2.setValue(30.56).setExists(true);
    val3.setValue(2.5F).setExists(true);
    val4.setValue(12).setExists(true);
    assertEquals(90.06, func.getDouble(), 0.000001);
    assertTrue(func.exists());
  }
}
