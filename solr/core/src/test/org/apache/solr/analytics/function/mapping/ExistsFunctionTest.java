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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValueStream;
import org.junit.Test;

public class ExistsFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void singleValueParameterTest() {
    TestBooleanValue val = new TestBooleanValue();

    AnalyticsValueStream uncasted = ExistsFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    val.setExists(false);
    assertFalse(func.getBoolean());
    assertTrue(func.exists());

    val.setValue(true).setExists(true);
    assertTrue(func.getBoolean());
    assertTrue(func.exists());

    val.setValue(false).setExists(true);
    assertTrue(func.getBoolean());
    assertTrue(func.exists());
  }

  @Test
  public void multiValueParameterTest() {
    TestDoubleValueStream val = new TestDoubleValueStream();

    AnalyticsValueStream uncasted = ExistsFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof BooleanValue);
    BooleanValue func = (BooleanValue) uncasted;

    val.setValues(2, 30.0);
    assertTrue(func.getBoolean());
    assertTrue(func.exists());

    val.setValues();
    assertFalse(func.getBoolean());
    assertTrue(func.exists());

    val.setValues(21234.65);
    assertTrue(func.getBoolean());
    assertTrue(func.exists());

    val.setValues();
    assertFalse(func.getBoolean());
    assertTrue(func.exists());
  }
}
