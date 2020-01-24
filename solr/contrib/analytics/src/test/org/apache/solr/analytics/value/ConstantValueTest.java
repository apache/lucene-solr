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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Date;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.FillableTestValue.TestIntValue;
import org.apache.solr.analytics.value.constant.ConstantBooleanValue;
import org.apache.solr.analytics.value.constant.ConstantDateValue;
import org.apache.solr.analytics.value.constant.ConstantDoubleValue;
import org.apache.solr.analytics.value.constant.ConstantFloatValue;
import org.apache.solr.analytics.value.constant.ConstantIntValue;
import org.apache.solr.analytics.value.constant.ConstantLongValue;
import org.apache.solr.analytics.value.constant.ConstantStringValue;
import org.apache.solr.analytics.value.constant.ConstantValue;
import org.junit.Test;

public class ConstantValueTest extends SolrTestCaseJ4 {


  @Test
  public void constantParsingTest() throws DateTimeParseException {
    // Int
    AnalyticsValueStream uncasted = ConstantValue.creatorFunction.apply("1234");
    assertTrue(uncasted instanceof ConstantIntValue);
    assertEquals(1234, ((ConstantIntValue)uncasted).getInt());

    // Long
    uncasted = ConstantValue.creatorFunction.apply("1234123412341234");
    assertTrue(uncasted instanceof ConstantLongValue);
    assertEquals(1234123412341234L, ((ConstantLongValue)uncasted).getLong());

    // Floats cannot currently be implicitly created

    // Double
    uncasted = ConstantValue.creatorFunction.apply("12341234.12341234");
    assertTrue(uncasted instanceof ConstantDoubleValue);
    assertEquals(12341234.12341234, ((ConstantDoubleValue)uncasted).getDouble(), .000000001);

    // String
    uncasted = ConstantValue.creatorFunction.apply("'abcdef'");
    assertTrue(uncasted instanceof ConstantStringValue);
    assertEquals("abcdef", ((ConstantStringValue)uncasted).getString());

    uncasted = ConstantValue.creatorFunction.apply("\"abcdef\"");
    assertTrue(uncasted instanceof ConstantStringValue);
    assertEquals("abcdef", ((ConstantStringValue)uncasted).getString());

    // Date
    uncasted = ConstantValue.creatorFunction.apply("1800-01-01T10:30:15.33Z");
    assertTrue(uncasted instanceof ConstantDateValue);
    assertEquals(Date.from(Instant.parse("1800-01-01T10:30:15.33Z")), ((ConstantDateValue)uncasted).getDate());
  }

  @Test
  public void constantConversionTest() {
    AnalyticsValueStream val = new ConstantBooleanValue(true);
    assertSame(val, val.convertToConstant());

    val = new ConstantIntValue(123);
    assertSame(val, val.convertToConstant());

    val = new ConstantLongValue(123L);
    assertSame(val, val.convertToConstant());

    val = new ConstantFloatValue(123F);
    assertSame(val, val.convertToConstant());

    val = new ConstantDoubleValue(123.0);
    assertSame(val, val.convertToConstant());

    val = new ConstantDateValue(123L);
    assertSame(val, val.convertToConstant());

    val = new ConstantStringValue("123");
    assertSame(val, val.convertToConstant());
  }

  @Test
  public void constantBooleanTest() {
    ConstantBooleanValue val = new ConstantBooleanValue(true);

    assertTrue(val.exists());
    assertEquals(true, val.getBoolean());
    assertEquals("true", val.getString());
    assertEquals(Boolean.TRUE, val.getObject());

    TestIntValue counter = new TestIntValue();
    counter.setValue(0);
    val.streamBooleans( value -> {
      assertEquals(true, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamStrings( value -> {
      assertEquals("true", value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamObjects( value -> {
      assertEquals(Boolean.TRUE, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });


    val = new ConstantBooleanValue(false);

    assertTrue(val.exists());
    assertEquals(false, val.getBoolean());
    assertEquals("false", val.getString());
    assertEquals(Boolean.FALSE, val.getObject());

    counter.setValue(0);
    val.streamBooleans( value -> {
      assertEquals(false, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamStrings( value -> {
      assertEquals("false", value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamObjects( value -> {
      assertEquals(Boolean.FALSE, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
  }

  @Test
  public void constantIntTest() {
    ConstantIntValue val = new ConstantIntValue(24);

    assertTrue(val.exists());
    assertEquals(24, val.getInt());
    assertEquals(24L, val.getLong());
    assertEquals(24F, val.getFloat(), .00001);
    assertEquals(24.0, val.getDouble(), .00001);
    assertEquals("24", val.getString());
    assertEquals(24, val.getObject());

    TestIntValue counter = new TestIntValue();
    counter.setValue(0);
    val.streamInts( value -> {
      assertEquals(24, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamLongs( value -> {
      assertEquals(24L, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamFloats( value -> {
      assertEquals(24F, value, .00001);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamDoubles( value -> {
      assertEquals(24.0, value, .00001);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamStrings( value -> {
      assertEquals("24", value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamObjects( value -> {
      assertEquals(24, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
  }

  @Test
  public void constantLongTest() {
    ConstantLongValue val = new ConstantLongValue(24L);

    assertTrue(val.exists());
    assertEquals(24L, val.getLong());
    assertEquals(24.0, val.getDouble(), .00001);
    assertEquals("24", val.getString());
    assertEquals(24L, val.getObject());

    TestIntValue counter = new TestIntValue();
    counter.setValue(0);
    val.streamLongs( value -> {
      assertEquals(24L, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamDoubles( value -> {
      assertEquals(24.0, value, .00001);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamStrings( value -> {
      assertEquals("24", value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamObjects( value -> {
      assertEquals(24L, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
  }

  @Test
  public void constantFloatTest() {
    ConstantFloatValue val = new ConstantFloatValue(24F);

    assertTrue(val.exists());
    assertEquals(24F, val.getFloat(), .00001);
    assertEquals(24.0, val.getDouble(), .00001);
    assertEquals("24.0", val.getString());
    assertEquals(24F, val.getObject());

    TestIntValue counter = new TestIntValue();
    counter.setValue(0);
    val.streamFloats( value -> {
      assertEquals(24F, value, .00001);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamDoubles( value -> {
      assertEquals(24.0, value, .00001);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamStrings( value -> {
      assertEquals("24.0", value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamObjects( value -> {
      assertEquals(24F, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
  }

  @Test
  public void constantDoubleTest() {
    ConstantDoubleValue val = new ConstantDoubleValue(24.0);

    assertTrue(val.exists());
    assertEquals(24.0, val.getDouble(), .00001);
    assertEquals("24.0", val.getString());
    assertEquals(24.0, val.getObject());

    TestIntValue counter = new TestIntValue();
    counter.setValue(0);
    val.streamDoubles( value -> {
      assertEquals(24.0, value, .00001);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamStrings( value -> {
      assertEquals("24.0", value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamObjects( value -> {
      assertEquals(24.0, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
  }

  @Test
  public void constantDateTest() throws DateTimeParseException {
    Date date = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    ConstantDateValue val = new ConstantDateValue(date.getTime());

    assertTrue(val.exists());
    assertEquals(date.getTime(), val.getLong());
    assertEquals(date, val.getDate());
    assertEquals("1800-01-01T10:30:15Z", val.getString());
    assertEquals(date, val.getObject());

    TestIntValue counter = new TestIntValue();
    counter.setValue(0);
    val.streamLongs( value -> {
      assertEquals(date.getTime(), value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamDates( value -> {
      assertEquals(date, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamStrings( value -> {
      assertEquals("1800-01-01T10:30:15Z", value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamObjects( value -> {
      assertEquals(date, value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
  }

  @Test
  public void constantStringTest() {
    ConstantStringValue val = new ConstantStringValue("abcdef");

    assertTrue(val.exists());
    assertEquals("abcdef", val.getString());
    assertEquals("abcdef", val.getObject());

    TestIntValue counter = new TestIntValue();
    counter.setValue(0);
    val.streamStrings( value -> {
      assertEquals("abcdef", value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
    counter.setValue(0);
    val.streamObjects( value -> {
      assertEquals("abcdef", value);
      assertEquals(0, counter.getInt());
      counter.setValue(1);
    });
  }
}
