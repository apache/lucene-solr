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

public class DivideFunctionTest extends SolrTestCaseJ4 {
  @Test
  public void twoSingleValueParametersTest() {
    TestIntValue dividend = new TestIntValue();
    TestFloatValue divisor = new TestFloatValue();

    AnalyticsValueStream uncasted = DivideFunction.creatorFunction.apply(new AnalyticsValueStream[] {dividend, divisor});
    assertTrue(uncasted instanceof DoubleValue);
    DoubleValue func = (DoubleValue) uncasted;

    // Neither exists
    dividend.setExists(false);
    divisor.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // One exists
    dividend.setValue(30).setExists(true);
    divisor.setExists(false);
    func.getDouble();
    assertFalse(func.exists());

    // Both exist
    dividend.setValue(60).setExists(true);
    divisor.setValue(23.56F).setExists(true);
    assertEquals(2.546689, func.getDouble(), 0.00001);
    assertTrue(func.exists());
  }

  @Test
  public void oneMultiOneSingleValueParameterTest() {
    TestLongValueStream dividend = new TestLongValueStream();
    TestDoubleValue divisor = new TestDoubleValue();

    AnalyticsValueStream uncasted = DivideFunction.creatorFunction.apply(new AnalyticsValueStream[] {dividend, divisor});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values, One value
    dividend.setValues();
    divisor.setValue(21.56F).setExists(true);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    dividend.setValues(4L, 10023L);
    divisor.setExists(false);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    dividend.setValues(20L, 5L, -234L);
    divisor.setValue(44.56F).setExists(true);
    Iterator<Double> values = Arrays.asList(0.448833, 0.112208, -5.251346).iterator();
    func.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, 0.00001);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void oneSingleOneMultiValueParameterTest() {
    TestDoubleValue dividend = new TestDoubleValue();
    TestLongValueStream divisor = new TestLongValueStream();

    AnalyticsValueStream uncasted = DivideFunction.creatorFunction.apply(new AnalyticsValueStream[] {dividend, divisor});
    assertTrue(uncasted instanceof DoubleValueStream);
    DoubleValueStream func = (DoubleValueStream) uncasted;

    // No values, One value
    dividend.setValue(21.56F).setExists(true);
    divisor.setValues();
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    dividend.setExists(false);
    divisor.setValues(4L, 10023L);
    func.streamDoubles( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    dividend.setValue(44.56F).setExists(true);
    divisor.setValues(20L, 5L, -234L);
    Iterator<Double> values = Arrays.asList(2.228, 8.912, -0.190427).iterator();
    func.streamDoubles( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value, 0.00001);
    });
    assertFalse(values.hasNext());
  }
}
