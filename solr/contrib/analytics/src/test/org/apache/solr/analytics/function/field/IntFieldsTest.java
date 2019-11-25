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
package org.apache.solr.analytics.function.field;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.analytics.ExpressionFactory;
import org.junit.Test;

public class IntFieldsTest extends AbstractAnalyticsFieldTest {

  @Test
  public void expressionFactoryCreationTest() {
    ExpressionFactory fact = getExpressionFactory();

    assertTrue(fact.createExpression("int_i_t") instanceof IntField);
    assertTrue(fact.createExpression("int_i_p") instanceof IntField);
    assertTrue(fact.createExpression("int_im_t") instanceof IntMultiTrieField);
    assertTrue(fact.createExpression("int_im_p") instanceof IntMultiPointField);
  }

  @Test
  public void singleValuedTrieIntTest() throws IOException {
    IntField valueField = new IntField("int_i_t");
    Map<String,Integer> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      int value = valueField.getInt();
      if (valueField.exists()) {
        values.put(id, value);
      }
      return valueField.exists();
    });

    checkSingleFieldValues(singleInts, values, missing);
  }

  @Test
  public void singleValuedPointIntTest() throws IOException {
    IntField valueField = new IntField("int_i_p");
    Map<String,Integer> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      int value = valueField.getInt();
      if (valueField.exists()) {
        values.put(id, value);
      }
      return valueField.exists();
    });

    checkSingleFieldValues(singleInts, values, missing);
  }

  @Test
  public void multiValuedTrieIntTest() throws IOException {
    IntMultiTrieField valueField = new IntMultiTrieField("int_im_t");
    Map<String,Map<Integer,Integer>> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      Map<Integer, Integer> doc = new HashMap<>();
      valueField.streamInts( value -> {
        doc.put(value, doc.getOrDefault(value, 0) + 1);
      });
      if (doc.size() > 0) {
        values.put(id, doc);
      }
      return doc.size() > 0;
    });

    checkMultiFieldValues(multiInts, values, missing, true);
  }

  @Test
  public void multiValuedPointIntTest() throws IOException {
    IntMultiPointField valueField = new IntMultiPointField("int_im_p");
    Map<String,Map<Integer,Integer>> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      Map<Integer, Integer> doc = new HashMap<>();
      valueField.streamInts( value -> {
        doc.put(value, doc.getOrDefault(value, 0) + 1);
      });
      if (doc.size() > 0) {
        values.put(id, doc);
      }
      return doc.size() > 0;
    });

    checkMultiFieldValues(multiInts, values, missing, false);
  }
}
