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
import org.apache.solr.analytics.function.mapping.ComparisonFunction.LTFunction;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestDateValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDateValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValueStream;
import org.junit.Test;

public class LTFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void singleValueNumericParameterTest() {
    TestDoubleValue base = new TestDoubleValue();
    TestDoubleValue comp = new TestDoubleValue();

    AnalyticsValueStream uncasted = LTFunction.creatorFunction.apply(new AnalyticsValueStream[] {base, comp});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    base.setExists(false);
    comp.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    base.setExists(false);
    comp.setValue(3.3).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    base.setValue(3.3).setExists(true);
    comp.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    base.setValue(21.56).setExists(true);
    comp.setValue(21.56).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    base.setValue(21.56).setExists(true);
    comp.setValue(21.57).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    base.setValue(21.56).setExists(true);
    comp.setValue(-21.57).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDateParameterTest() {
    TestDateValue base = new TestDateValue();
    TestDateValue comp = new TestDateValue();

    AnalyticsValueStream uncasted = LTFunction.creatorFunction.apply(new AnalyticsValueStream[] {base, comp});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    base.setExists(false);
    comp.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    base.setExists(false);
    comp.setValue("1800-01-02T10:20:30Z").setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    base.setValue("1800-01-02T10:20:30Z").setExists(true);
    comp.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    base.setValue("1800-01-02T10:20:30Z").setExists(true);
    comp.setValue("1800-01-02T10:20:30Z").setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());

    base.setValue("1800-01-02T10:20:30Z").setExists(true);
    comp.setValue("1800-01-02T10:20:31Z").setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    base.setValue("1800-01-02T10:20:30Z").setExists(true);
    comp.setValue("1000-01-02T10:20:31Z").setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void oneSingleOneMultiValueNumericParameterTest() {
    TestDoubleValue base = new TestDoubleValue();
    TestDoubleValueStream comp = new TestDoubleValueStream();

    AnalyticsValueStream uncasted = LTFunction.creatorFunction.apply(new AnalyticsValueStream[] {base, comp});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    base.setValue(-4.2).setExists(true);
    comp.setValues();
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    base.setExists(false);
    comp.setValues(-4.2);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    base.setValue(-4.2).setExists(true);
    comp.setValues(-4);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    base.setValue(4).setExists(true);
    comp.setValues(4, -10, 2345, -74, 4.0001);
    Iterator<Boolean> values2 = Arrays.asList(false, false, true, false, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneMultiOneSingleValueNumericParameterTest() {
    TestDoubleValueStream base = new TestDoubleValueStream();
    TestDoubleValue comp = new TestDoubleValue();

    AnalyticsValueStream uncasted = LTFunction.creatorFunction.apply(new AnalyticsValueStream[] {base, comp});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    base.setValues();
    comp.setValue(-4.2).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    base.setValues(-4.2);
    comp.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    base.setValues(-4.2);
    comp.setValue(-4).setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    base.setValues(4, -10, 2345, -74, 4.0001);
    comp.setValue(4).setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(false, true, false, true, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneSingleOneMultiValueDateParameterTest() {
    TestDateValue base = new TestDateValue();
    TestDateValueStream comp = new TestDateValueStream();

    AnalyticsValueStream uncasted = LTFunction.creatorFunction.apply(new AnalyticsValueStream[] {base, comp});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    base.setValue("1800-01-02T10:20:30Z").setExists(true);
    comp.setValues();
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    base.setExists(false);
    comp.setValues("1800-01-02T10:20:30Z");
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    base.setValue("1803-01-02T10:20:30Z").setExists(true);
    comp.setValues("1800-01-02T10:20:30Z");
    Iterator<Boolean> values1 = Arrays.asList(false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    base.setValue("1800-01-02T10:20:30Z").setExists(true);
    comp.setValues("1800-03-02T10:20:30Z", "1799-01-01T10:20:29Z", "1800-01-02T10:20:31Z", "1800-01-02T10:20:30Z", "1800-01-02T10:20:29Z");
    Iterator<Boolean> values2 = Arrays.asList(true, false, true, false, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneMultiOneSingleValueDateParameterTest() {
    TestDateValueStream base = new TestDateValueStream();
    TestDateValue comp = new TestDateValue();

    AnalyticsValueStream uncasted = LTFunction.creatorFunction.apply(new AnalyticsValueStream[] {base, comp});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    base.setValues();
    comp.setValue("1800-01-02T10:20:30Z").setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    base.setValues("1800-01-02T10:20:30Z");
    comp.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    base.setValues("1800-01-02T10:20:30Z");
    comp.setValue("1803-01-02T10:20:30Z").setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    base.setValues("1800-03-02T10:20:30Z", "1799-01-01T10:20:29Z", "1800-01-02T10:20:31Z", "1800-01-02T10:20:30Z", "1800-01-02T10:20:29Z");
    comp.setValue("1800-01-02T10:20:30Z").setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(false, true, false, false, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }
}
