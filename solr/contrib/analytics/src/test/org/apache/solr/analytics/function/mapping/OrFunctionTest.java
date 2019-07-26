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
import org.apache.solr.analytics.function.mapping.LogicFunction.OrFunction;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValue;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValueStream;
import org.junit.Test;

public class OrFunctionTest extends SolrTestCaseJ4 {
  @Test
  public void twoSingleValueParametersTest() {
    TestBooleanValue val1 = new TestBooleanValue();
    TestBooleanValue val2 = new TestBooleanValue();

    AnalyticsValueStream uncasted = OrFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Neither exists
    val1.setExists(false);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // One exists
    val1.setValue(true).setExists(true);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Both exist
    val1.setValue(false).setExists(true);
    val2.setValue(false).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    val1.setValue(true).setExists(true);
    val2.setValue(false).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val1.setValue(false).setExists(true);
    val2.setValue(true).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val1.setValue(true).setExists(true);
    val2.setValue(true).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void oneMultiValueParameterTest() {
    TestBooleanValueStream val = new TestBooleanValueStream();

    AnalyticsValueStream uncasted = OrFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // No values
    val.setValues();
    func.getBoolean();
    assertFalse(func.exists());

    // One value
    val.setValues(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val.setValues(false);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    // Multiple values
    val.setValues(false, false, true, false);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val.setValues(false, false, false);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void oneMultiOneSingleValueParameterTest() {
    TestBooleanValue val1 = new TestBooleanValue();
    TestBooleanValueStream val2 = new TestBooleanValueStream();

    AnalyticsValueStream uncasted = OrFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values, One value
    val1.setValue(false).setExists(true);
    val2.setValues();
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, no value
    val1.setExists(false);
    val2.setValues(true, false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple values, one value
    val1.setValue(false).setExists(true);
    val2.setValues(true, false, true);
    Iterator<Boolean> values1 = Arrays.asList(true, false, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    val1.setValue(true).setExists(true);
    val2.setValues(true, false, true);
    Iterator<Boolean> values2 = Arrays.asList(true, true, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multipleSingleValueParameterTest() {
    TestBooleanValue val1 = new TestBooleanValue();
    TestBooleanValue val2 = new TestBooleanValue();
    TestBooleanValue val3 = new TestBooleanValue();
    TestBooleanValue val4 = new TestBooleanValue();

    AnalyticsValueStream uncasted = OrFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2, val3, val4});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // None exist
    val1.setExists(false);
    val2.setExists(false);
    val3.setExists(false);
    val4.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Some exist
    val1.setExists(false);
    val2.setValue(true).setExists(true);
    val3.setExists(false);
    val4.setValue(false).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    // All exist values, one value
    val1.setValue(false).setExists(true);
    val2.setValue(true).setExists(true);
    val3.setValue(false).setExists(true);
    val4.setValue(false).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val1.setValue(false).setExists(true);
    val2.setValue(false).setExists(true);
    val3.setValue(false).setExists(true);
    val4.setValue(false).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }
}
