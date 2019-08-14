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

public class PowerFunctionTest extends SolrTestCaseJ4 {
  @Test
  public void twoSingleValueParametersTest() {
    TestIntValue base = new TestIntValue();
    TestFloatValue pow = new TestFloatValue();

    AnalyticsValueStream uncasted = PowerFunction.creatorFunction.apply(new AnalyticsValueStream[] {base, pow});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Neither exists
    base.setExists(false);
    pow.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // One exists
    base.setValue(30).setExists(true);
    pow.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Both exist
    base.setValue(6).setExists(true);
    pow.setValue(2.56F).setExists(true);
    assertEquals(98.1899, func.getDouble(), 0.0001);
    assertTrue(func.exists());
  }

  @Test
  public void oneMultiOneSingleValueParameterTest() {
    TestLongValueStream base = new TestLongValueStream();
    TestDoubleValue pow = new TestDoubleValue();

    AnalyticsValueStream uncasted = PowerFunction.creatorFunction.apply(new AnalyticsValueStream[] {base, pow});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values, One value
    base.setValues();
    pow.setValue(21.56F).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    base.setValues(4L, 10023L);
    pow.setExists(false);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    base.setValues(4L, 123L, 10L);
    pow.setValue(2.5F).setExists(true);
    Iterator<Double> values = Arrays.asList(32.0, 167788.7268, 316.2277).iterator();
    func.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, 0.001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void oneSingleOneMultiValueParameterTest() {
    TestDoubleValue base = new TestDoubleValue();
    TestLongValueStream pow = new TestLongValueStream();

    AnalyticsValueStream uncasted = PowerFunction.creatorFunction.apply(new AnalyticsValueStream[] {base, pow});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values, One value
    base.setValue(21.56F).setExists(true);
    pow.setValues();
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    base.setExists(false);
    pow.setValues(4L, 10023L);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    base.setValue(4.56F).setExists(true);
    pow.setValues(2L, 5L, 3L);
    Iterator<Double> values = Arrays.asList(20.7936, 1971.6245, 94.8188).iterator();
    func.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, 0.0001);
    });
    assertFalse(values.hasNext());
  }
}
