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

public class DoubleFieldsTest extends AbstractAnalyticsFieldTest {

  @Test
  public void expressionFactoryCreationTest() {
    ExpressionFactory fact = getExpressionFactory();

    assertTrue(fact.createExpression("double_d_t") instanceof DoubleField);
    assertTrue(fact.createExpression("double_d_p") instanceof DoubleField);
    assertTrue(fact.createExpression("double_dm_t") instanceof DoubleMultiTrieField);
    assertTrue(fact.createExpression("double_dm_p") instanceof DoubleMultiPointField);
  }

  @Test
  public void singleValuedTrieDoubleTest() throws IOException {
    DoubleField valueField = new DoubleField("double_d_t");
    Map<String,Double> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      double value = valueField.getDouble();
      if (valueField.exists()) {
        values.put(id, value);
      }
      return valueField.exists();
    });

    checkSingleFieldValues(singleDoubles, values, missing);
  }

  @Test
  public void singleValuedPointDoubleTest() throws IOException {
    DoubleField valueField = new DoubleField("double_d_p");
    Map<String,Double> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      double value = valueField.getDouble();
      if (valueField.exists()) {
        values.put(id, value);
      }
      return valueField.exists();
    });

    checkSingleFieldValues(singleDoubles, values, missing);
  }

  @Test
  public void multiValuedTrieDoubleTest() throws IOException {
    DoubleMultiTrieField valueField = new DoubleMultiTrieField("double_dm_t");
    Map<String,Map<Double,Integer>> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      Map<Double, Integer> doc = new HashMap<>();
      valueField.streamDoubles( value -> {
        doc.put(value, doc.getOrDefault(value, 0) + 1);
      });
      if (doc.size() > 0) {
        values.put(id, doc);
      }
      return doc.size() > 0;
    });

    checkMultiFieldValues(multiDoubles, values, missing, true);
  }

  @Test
  public void multiValuedPointDoubleTest() throws IOException {
    DoubleMultiPointField valueField = new DoubleMultiPointField("double_dm_p");
    Map<String,Map<Double,Integer>> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      Map<Double, Integer> doc = new HashMap<>();
      valueField.streamDoubles( value -> {
        doc.put(value, doc.getOrDefault(value, 0) + 1);
      });
      if (doc.size() > 0) {
        values.put(id, doc);
      }
      return doc.size() > 0;
    });

    checkMultiFieldValues(multiDoubles, values, missing, false);
  }
}
