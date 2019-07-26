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
import org.apache.solr.analytics.value.FillableTestValue.TestIntValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValueStream;
import org.junit.Test;

public class LogFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void singleValueParameterTest() {
    TestFloatValue val = new TestFloatValue();

    AnalyticsValueStream uncasted = LogFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21.56F).setExists(true);
    assertEquals(Math.log(21.56F), func.getDouble(), 0.00001);
    assertTrue(func.exists());

    val.setValue(-100F).setExists(true);
    assertEquals(Math.log(-100F), func.getDouble(), 0.00001);
    assertTrue(func.exists());
  }

  @Test
  public void multiValueParameterTest() {
    TestIntValueStream val = new TestIntValueStream();

    AnalyticsValueStream uncasted = LogFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values
    val.setValues();
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(4);
    Iterator<Double> values1 = Arrays.asList(Math.log(4)).iterator();
    func.streamDoubles( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value, .00001);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val.setValues(2, 103, 502, 743);
    Iterator<Double> values2 = Arrays.asList(Math.log(2), Math.log(103), Math.log(502), Math.log(743)).iterator();
    func.streamDoubles( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value, .00001);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void twoSingleValueParametersTest() {
    TestIntValue base = new TestIntValue();
    TestFloatValue val = new TestFloatValue();

    AnalyticsValueStream uncasted = LogFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, base});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Neither exists
    base.setExists(false);
    val.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // One exists
    base.setValue(30).setExists(true);
    val.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Both exist
    base.setValue(6).setExists(true);
    val.setValue(2.56F).setExists(true);
    assertEquals(Math.log(2.56F)/Math.log(6), func.getDouble(), 0.0001);
    assertTrue(func.exists());
  }

  @Test
  public void oneMultiOneSingleValueParameterTest() {
    TestLongValueStream base = new TestLongValueStream();
    TestDoubleValue val = new TestDoubleValue();

    AnalyticsValueStream uncasted = LogFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, base});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values, One value
    base.setValues();
    val.setValue(21.56F).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    base.setValues(4L, 10023L);
    val.setExists(false);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    base.setValues(4L, 123L, 2L);
    val.setValue(4.56F).setExists(true);
    Iterator<Double> values = Arrays.asList(Math.log(4.56F)/Math.log(4L), Math.log(4.56F)/Math.log(123L), Math.log(4.56F)/Math.log(2L)).iterator();
    func.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, 0.0001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void oneSingleOneMultiValueParameterTest() {
    TestDoubleValue base = new TestDoubleValue();
    TestLongValueStream val = new TestLongValueStream();

    AnalyticsValueStream uncasted = LogFunction.creatorFunction.apply(new AnalyticsValueStream[] {val, base});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values, One value
    base.setValue(21.56F).setExists(true);
    val.setValues();
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    base.setExists(false);
    val.setValues(4L, 10023L);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    base.setValue(4.56F).setExists(true);
    val.setValues(2L, 50L, 3L);
    Iterator<Double> values = Arrays.asList(Math.log(2L)/Math.log(4.56F), Math.log(50L)/Math.log(4.56F), Math.log(3L)/Math.log(4.56F)).iterator();
    func.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, 0.0001);
    });
    assertFalse(values.hasNext());
  }
}
