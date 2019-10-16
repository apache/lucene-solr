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
import org.apache.solr.analytics.value.FillableTestValue.TestLongValueStream;
import org.junit.Test;

public class SubtractFunctionTest extends SolrTestCaseJ4 {
  @Test
  public void twoSingleValueParametersTest() {
    TestIntValue minuend = new TestIntValue();
    TestFloatValue subtrahend = new TestFloatValue();

    AnalyticsValueStream uncasted = SubtractFunction.creatorFunction.apply(new AnalyticsValueStream[] {minuend, subtrahend});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Neither exists
    minuend.setExists(false);
    subtrahend.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // One exists
    minuend.setValue(30).setExists(true);
    subtrahend.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Both exist
    minuend.setValue(60).setExists(true);
    subtrahend.setValue(23.56F).setExists(true);
    assertEquals(36.44, func.getDouble(), 0.00001);
    assertTrue(func.exists());
  }

  @Test
  public void oneMultiOneSingleValueParameterTest() {
    TestLongValueStream minuend = new TestLongValueStream();
    TestDoubleValue subtrahend = new TestDoubleValue();

    AnalyticsValueStream uncasted = SubtractFunction.creatorFunction.apply(new AnalyticsValueStream[] {minuend, subtrahend});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values, One value
    minuend.setValues();
    subtrahend.setValue(21.56F).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    minuend.setValues(4L, 10023L);
    subtrahend.setExists(false);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    minuend.setValues(20L, 5L, 234L);
    subtrahend.setValue(44.56F).setExists(true);
    Iterator<Double> values = Arrays.asList(-24.56, -39.56, 189.44).iterator();
    func.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, 0.00001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void oneSingleOneMultiValueParameterTest() {
    TestDoubleValue minuend = new TestDoubleValue();
    TestLongValueStream subtrahend = new TestLongValueStream();

    AnalyticsValueStream uncasted = SubtractFunction.creatorFunction.apply(new AnalyticsValueStream[] {minuend, subtrahend});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values, One value
    minuend.setValue(21.56F).setExists(true);
    subtrahend.setValues();
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    minuend.setExists(false);
    subtrahend.setValues(4L, 10023L);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    minuend.setValue(44.56F).setExists(true);
    subtrahend.setValues(20L, 5L, 234L);
    Iterator<Double> values = Arrays.asList(24.56, 39.56, -189.44).iterator();
    func.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, 0.00001);
    });
    assertFalse(values.hasNext());
  }
}
