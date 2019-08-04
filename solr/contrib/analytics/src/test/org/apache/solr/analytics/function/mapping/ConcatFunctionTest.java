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
import org.apache.solr.analytics.function.mapping.ConcatFunction.SeparatedConcatFunction;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestStringValue;
import org.apache.solr.analytics.value.FillableTestValue.TestStringValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.constant.ConstantStringValue;
import org.junit.Test;

public class ConcatFunctionTest extends SolrTestCaseJ4 {

  @Test
  public void oneMultiValueParameterUnseparatedTest() {
    TestStringValueStream val = new TestStringValueStream();

    AnalyticsValueStream uncasted = ConcatFunction.creatorFunction.apply(new AnalyticsValueStream[] {val});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // No values
    val.setValues();
    func.getString();
    assertFalse(func.exists());

    // One value
    val.setValues("abc");
    assertEquals("abc", func.getString());
    assertTrue(func.exists());

    // Multiple values
    val.setValues("abc", "def", "ghi");
    assertEquals("abcdefghi", func.getString());
    assertTrue(func.exists());
  }

  @Test
  public void oneMultiValueParameterSeparatedTest() {
    TestStringValueStream val = new TestStringValueStream();
    ConstantStringValue sep = new ConstantStringValue("===:---");

    AnalyticsValueStream uncasted = SeparatedConcatFunction.creatorFunction.apply(new AnalyticsValueStream[] {sep, val});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // No values
    val.setValues();
    func.getString();
    assertFalse(func.exists());

    // One value
    val.setValues("abc");
    assertEquals("abc", func.getString());
    assertTrue(func.exists());

    // Multiple values
    val.setValues("abc", "def", "ghi");
    assertEquals("abc===:---def===:---ghi", func.getString());
    assertTrue(func.exists());
  }

  @Test
  public void oneSingleOneMultiValueParameterUnseperatedTest() {
    TestStringValue val1 = new TestStringValue();
    TestStringValueStream val2 = new TestStringValueStream();

    AnalyticsValueStream uncasted = ConcatFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // None exist
    val1.setExists(false);
    val2.setValues();
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One exists
    val1.setExists(false);
    val2.setValues("def");
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValue("abc").setExists(true);
    val2.setValues();
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Both exist
    val1.setValue("abc").setExists(true);
    val2.setValues("def", "ghi", "jkl");
    Iterator<String> values2 = Arrays.asList("abcdef", "abcghi", "abcjkl").iterator();
    func.streamStrings( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneSingleOneMultiValueParameterSeperatedTest() {
    TestStringValue val1 = new TestStringValue();
    TestStringValueStream val2 = new TestStringValueStream();
    ConstantStringValue sep = new ConstantStringValue("+-;");

    AnalyticsValueStream uncasted = SeparatedConcatFunction.creatorFunction.apply(new AnalyticsValueStream[] {sep, val1, val2});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // None exist
    val1.setExists(false);
    val2.setValues();
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One exists
    val1.setExists(false);
    val2.setValues("def");
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValue("abc").setExists(true);
    val2.setValues();
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Both exist
    val1.setValue("abc").setExists(true);
    val2.setValues("def", "ghi", "jkl");
    Iterator<String> values2 = Arrays.asList("abc+-;def", "abc+-;ghi", "abc+-;jkl").iterator();
    func.streamStrings( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneMultiOneSingleValueParameterUnseperatedTest() {
    TestStringValueStream val1 = new TestStringValueStream();
    TestStringValue val2 = new TestStringValue();

    AnalyticsValueStream uncasted = ConcatFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // None exist
    val1.setValues();
    val2.setExists(false);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One exists
    val1.setValues("def");
    val2.setExists(false);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues();
    val2.setValue("abc").setExists(true);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Both exist
    val1.setValues("def", "ghi", "jkl");
    val2.setValue("abc").setExists(true);
    Iterator<String> values2 = Arrays.asList("defabc", "ghiabc", "jklabc").iterator();
    func.streamStrings( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void oneMultiOneSingleValueParameterSeperatedTest() {
    TestStringValueStream val1 = new TestStringValueStream();
    TestStringValue val2 = new TestStringValue();
    ConstantStringValue sep = new ConstantStringValue("<");

    AnalyticsValueStream uncasted = SeparatedConcatFunction.creatorFunction.apply(new AnalyticsValueStream[] {sep, val1, val2});
    assertTrue(uncasted instanceof StringValueStream);
    StringValueStream func = (StringValueStream) uncasted;

    // None exist
    val1.setValues();
    val2.setExists(false);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // One exists
    val1.setValues("def");
    val2.setExists(false);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    val1.setValues();
    val2.setValue("abc").setExists(true);
    func.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Both exist
    val1.setValues("def", "ghi", "jkl");
    val2.setValue("abc").setExists(true);
    Iterator<String> values2 = Arrays.asList("def<abc", "ghi<abc", "jkl<abc").iterator();
    func.streamStrings( value -> {
      assertTrue(values2.hasNext());
      assertEquals(values2.next(), value);
    });
    assertFalse(values2.hasNext());
  }

  @Test
  public void multipleSingleValueParameterUnseperatedTest() {
    TestStringValue val1 = new TestStringValue();
    TestStringValue val2 = new TestStringValue();
    TestStringValue val3 = new TestStringValue();
    TestStringValue val4 = new TestStringValue();

    AnalyticsValueStream uncasted = ConcatFunction.creatorFunction.apply(new AnalyticsValueStream[] {val1, val2, val3, val4});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // None exist
    val1.setExists(false);
    val2.setExists(false);
    val3.setExists(false);
    val4.setExists(false);
    func.getString();
    assertFalse(func.exists());

    // Some exist
    val1.setExists(false);
    val2.setValue("def").setExists(true);
    val3.setExists(false);
    val4.setValue("jkl").setExists(true);
    assertEquals("defjkl", func.getString());
    assertTrue(func.exists());

    // All exist values, one value
    val1.setValue("abc").setExists(true);
    val2.setValue("def").setExists(true);
    val3.setValue("ghi").setExists(true);
    val4.setValue("jkl").setExists(true);
    assertEquals("abcdefghijkl", func.getString());
    assertTrue(func.exists());
  }

  @Test
  public void multipleSingleValueParameterSeperatedTest() {
    TestStringValue val1 = new TestStringValue();
    TestStringValue val2 = new TestStringValue();
    TestStringValue val3 = new TestStringValue();
    TestStringValue val4 = new TestStringValue();
    ConstantStringValue sep = new ConstantStringValue("+:-");

    AnalyticsValueStream uncasted = SeparatedConcatFunction.creatorFunction.apply(new AnalyticsValueStream[] {sep, val1, val2, val3, val4});
    assertTrue(uncasted instanceof StringValue);
    StringValue func = (StringValue) uncasted;

    // None exist
    val1.setExists(false);
    val2.setExists(false);
    val3.setExists(false);
    val4.setExists(false);
    func.getString();
    assertFalse(func.exists());

    // Some exist
    val1.setExists(false);
    val2.setValue("def").setExists(true);
    val3.setExists(false);
    val4.setValue("jkl").setExists(true);
    assertEquals("def+:-jkl", func.getString());
    assertTrue(func.exists());

    // All exist values, one value
    val1.setValue("abc").setExists(true);
    val2.setValue("def").setExists(true);
    val3.setValue("ghi").setExists(true);
    val4.setValue("jkl").setExists(true);
    assertEquals("abc+:-def+:-ghi+:-jkl", func.getString());
    assertTrue(func.exists());
  }
}
