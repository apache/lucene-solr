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
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValue;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestDateValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDateValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestStringValue;
import org.apache.solr.analytics.value.FillableTestValue.TestStringValueStream;
import org.junit.Test;

public class EqualFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void singleValueBooleanParameterTest() {
    TestBooleanValue val1 = new TestBooleanValue();
    TestBooleanValue val2 = new TestBooleanValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    val1.setExists(false);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    val1.setExists(false);
    val2.setValue(false).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    val1.setValue(true).setExists(true);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    val1.setValue(false).setExists(true);
    val2.setValue(false).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val1.setValue(false).setExists(true);
    val2.setValue(true).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueNumericParameterTest() {
    TestDoubleValue val1 = new TestDoubleValue();
    TestDoubleValue val2 = new TestDoubleValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    val1.setExists(false);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    val1.setExists(false);
    val2.setValue(3.3).setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    val1.setValue(3.3).setExists(true);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    val1.setValue(21.56).setExists(true);
    val2.setValue(21.56).setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val1.setValue(21.56).setExists(true);
    val2.setValue(21.57).setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueDateParameterTest() {
    TestDateValue val1 = new TestDateValue();
    TestDateValue val2 = new TestDateValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    val1.setExists(false);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    val1.setExists(false);
    val2.setValue("1800-01-02T10:20:30Z").setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    val1.setValue("1800-01-02T10:20:30Z").setExists(true);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    val1.setValue("1800-01-02T10:20:30Z").setExists(true);
    val2.setValue("1800-01-02T10:20:30Z").setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val1.setValue("1800-01-02T10:20:30Z").setExists(true);
    val2.setValue("1800-01-02T10:20:31Z").setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void singleValueStringParameterTest() {
    TestStringValue val1 = new TestStringValue();
    TestStringValue val2 = new TestStringValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    // Value doesn't exist
    val1.setExists(false);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    val1.setExists(false);
    val2.setValue("abcdefghi").setExists(true);
    func.getBoolean();
    assertFalse(func.exists());

    val1.setValue("abcdefghi").setExists(true);
    val2.setExists(false);
    func.getBoolean();
    assertFalse(func.exists());

    // Value exists
    val1.setValue("abcdefghi").setExists(true);
    val2.setValue("abcdefghi").setExists(true);
    assertEquals(true, func.getBoolean());
    assertTrue(func.exists());

    val1.setValue("abcdefghi1").setExists(true);
    val2.setValue("abcdefghi2").setExists(true);
    assertEquals(false, func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void oneSingleOneMultiValueBooleanParameterTest() {
    TestBooleanValueStream val1 = new TestBooleanValueStream();
    TestBooleanValue val2 = new TestBooleanValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val1.setValues();
    val2.setValue(false).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues(true);
    val2.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val1.setValues(false);
    val2.setValue(false).setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val1.setValues(false, true, true, false, false);
    val2.setValue(false).setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(true, false, false, true, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneMultiOneSingleValueBooleanParameterTest() {
    TestBooleanValueStream val1 = new TestBooleanValueStream();
    TestBooleanValue val2 = new TestBooleanValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val2, val1});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val1.setValues();
    val2.setValue(false).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues(true);
    val2.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val1.setValues(false);
    val2.setValue(false).setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val1.setValues(false, true, true, false, false);
    val2.setValue(false).setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(true, false, false, true, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneSingleOneMultiValueNumericParameterTest() {
    TestDoubleValueStream val1 = new TestDoubleValueStream();
    TestDoubleValue val2 = new TestDoubleValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val1.setValues();
    val2.setValue(-4.2).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues(-4.2);
    val2.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val1.setValues(-4.2);
    val2.setValue(-4.2).setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val1.setValues(4, -10, 4, -74, 4.0001);
    val2.setValue(4).setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(true, false, true, false, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneMultiOneSingleValueNumericParameterTest() {
    TestDoubleValueStream val1 = new TestDoubleValueStream();
    TestDoubleValue val2 = new TestDoubleValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val2, val1});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val1.setValues();
    val2.setValue(-4.2).setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues(-4.2);
    val2.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val1.setValues(-4.2);
    val2.setValue(-4.2).setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val1.setValues(4, -10, 4, -74, 4.0001);
    val2.setValue(4).setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(true, false, true, false, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneSingleOneMultiValueDateParameterTest() {
    TestDateValueStream val1 = new TestDateValueStream();
    TestDateValue val2 = new TestDateValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val1.setValues();
    val2.setValue("1800-01-02T10:20:30Z").setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues("1800-01-02T10:20:30Z");
    val2.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val1.setValues("1800-01-02T10:20:30Z");
    val2.setValue("1800-01-02T10:20:30Z").setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val1.setValues("1800-01-02T10:20:30Z", "1800-01-02T10:20:31Z", "1800-01-02T10:20:30Z", "1801-01-02T10:20:30Z");
    val2.setValue("1800-01-02T10:20:30Z").setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(true, false, true, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneMultiOneSingleValueDateParameterTest() {
    TestDateValueStream val1 = new TestDateValueStream();
    TestDateValue val2 = new TestDateValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val2, val1});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val1.setValues();
    val2.setValue("1800-01-02T10:20:30Z").setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues("1800-01-02T10:20:30Z");
    val2.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val1.setValues("1800-01-02T10:20:30Z");
    val2.setValue("1800-01-02T10:20:30Z").setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val1.setValues("1800-01-02T10:20:30Z", "1800-01-02T10:20:31Z", "1800-01-02T10:20:30Z", "9999-01-02T10:20:30Z");
    val2.setValue("1800-01-02T10:20:30Z").setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(true, false, true, false).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneSingleOneMultiValueStringParameterTest() {
    TestStringValueStream val1 = new TestStringValueStream();
    TestStringValue val2 = new TestStringValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val1.setValues();
    val2.setValue("abcdefghi").setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues("abcdefghi");
    val2.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val1.setValues("abcdefghi");
    val2.setValue("abcdefghi").setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val1.setValues("abcdefghi", "abcdefghi1", "sbcdefghi", "abcdefghi");
    val2.setValue("abcdefghi").setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(true, false, false, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneMultiOneSingleValueStringParameterTest() {
    TestStringValueStream val1 = new TestStringValueStream();
    TestStringValue val2 = new TestStringValue();

    AnalyticsValueStream uncasted = EqualFunction.creatorFunction.apply(new AnalyticsValueStream[] {val2, val1});
    assertTrue(uncasted instanceof BooleanValueStream);
    BooleanValueStream func = (BooleanValueStream) uncasted;

    // No values
    val1.setValues();
    val2.setValue("abcdefghi").setExists(true);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues("abcdefghi");
    val2.setExists(false);
    func.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val1.setValues("abcdefghi");
    val2.setValue("abcdefghi").setExists(true);
    Iterator<Boolean> values1 = Arrays.asList(true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val1.setValues("abcdefghi", "abcdefghi1", "sbcdefghi", "abcdefghi");
    val2.setValue("abcdefghi").setExists(true);
    Iterator<Boolean> values2 = Arrays.asList(true, false, false, true).iterator();
    func.streamBooleans( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }
}
