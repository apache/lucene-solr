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
package org.apache.solr.analytics.value;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.AnalyticsValueStream.ExpressionType;
import org.apache.solr.analytics.value.FillableTestValue.TestStringValue;
import org.apache.solr.analytics.value.constant.ConstantStringValue;
import org.junit.Test;

public class CastingStringValueTest extends SolrTestCaseJ4 {

  @Test
  public void objectCastingTest() {
    TestStringValue val = new TestStringValue();

    assertTrue(val instanceof AnalyticsValue);
    AnalyticsValue casted = (AnalyticsValue)val;

    val.setValue("string 1").setExists(true);
    assertEquals("string 1", casted.getObject());
    assertTrue(casted.exists());

    val.setValue("abc").setExists(true);
    assertEquals("abc", casted.getObject());
    assertTrue(casted.exists());
  }

  @Test
  public void stringStreamCastingTest() {
    TestStringValue val = new TestStringValue();

    assertTrue(val instanceof StringValueStream);
    StringValueStream casted = (StringValueStream)val;

    // No values
    val.setExists(false);
    casted.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue("abcd").setExists(true);
    Iterator<String> values = Arrays.asList("abcd").iterator();
    casted.streamStrings( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void objectStreamCastingTest() {
    TestStringValue val = new TestStringValue();

    assertTrue(val instanceof AnalyticsValueStream);
    AnalyticsValueStream casted = (AnalyticsValueStream)val;

    // No values
    val.setExists(false);
    casted.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue("abcd").setExists(true);
    Iterator<Object> values = Arrays.<Object>asList("abcd").iterator();
    casted.streamObjects( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void constantConversionTest() {
    TestStringValue val = new TestStringValue(ExpressionType.CONST);
    val.setValue("asd23n23").setExists(true);
    AnalyticsValueStream conv = val.convertToConstant();
    assertTrue(conv instanceof ConstantStringValue);
    assertEquals("asd23n23", ((ConstantStringValue)conv).getString());

    val = new TestStringValue(ExpressionType.FIELD);
    val.setValue("asd23n23").setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestStringValue(ExpressionType.UNREDUCED_MAPPING);
    val.setValue("asd23n23").setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestStringValue(ExpressionType.REDUCTION);
    val.setValue("asd23n23").setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestStringValue(ExpressionType.REDUCED_MAPPING);
    val.setValue("asd23n23").setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);
  }
}
