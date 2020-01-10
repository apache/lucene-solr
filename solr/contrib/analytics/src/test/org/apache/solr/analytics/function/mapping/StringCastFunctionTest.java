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
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestIntValue;
import org.junit.Test;

public class StringCastFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void singleValueParameterTest() {
    TestIntValue val = new TestIntValue();

    AnalyticsValueStream uncasted = StringCastFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // Value doesn't exist
    val.setExists(false);
    func.getObject();
    assertFalse(func.exists());

    // Value exists
    val.setValue(21).setExists(true);
    assertEquals("21", func.getObject());
    assertTrue(func.exists());

    val.setValue(-100).setExists(true);
    assertEquals("-100", func.getObject());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueParameterTest() {
    TestBooleanValueStream val = new TestBooleanValueStream();

    AnalyticsValueStream uncasted = StringCastFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // No values
    val.setValues();
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One value
    val.setValues(true);
    Iterator<String> values1 = Arrays.asList("true").iterator();
    func.streamObjects( value -> {
      assertTrue(values1.hasNext());
      assertEquals(values1.next(), value);
    });
    assertFalse(values1.hasNext());

    // Multiple values
    val.setValues(true, true, false, false);
    Iterator<String> values2 = Arrays.asList("true", "true", "false", "false").iterator();
    func.streamObjects( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }
}
