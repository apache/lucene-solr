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

public class BooleanFieldsTest extends AbstractAnalyticsFieldTest {

  @Test
  public void expressionFactoryCreationTest() {
    ExpressionFactory fact = getExpressionFactory();

    assertTrue(fact.createExpression("boolean_b") instanceof BooleanField);
    assertTrue(fact.createExpression("boolean_bm") instanceof BooleanMultiField);
  }

  @Test
  public void singleValuedBooleanTest() throws IOException {
    BooleanField valueField = new BooleanField("boolean_b");
    Map<String,Boolean> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      boolean value = valueField.getBoolean();
      if (valueField.exists()) {
        values.put(id, value);
      }
      return valueField.exists();
    });

    checkSingleFieldValues(singleBooleans, values, missing);
  }

  @Test
  public void multiValuedBooleanTest() throws IOException {
    BooleanMultiField valueField = new BooleanMultiField("boolean_bm");
    Map<String,Map<Boolean,Integer>> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      Map<Boolean, Integer> doc = new HashMap<>();
      valueField.streamBooleans( value -> {
        doc.put(value, doc.getOrDefault(value, 0) + 1);
      });
      if (doc.size() > 0) {
        values.put(id, doc);
      }
      return doc.size() > 0;
    });

    checkMultiFieldValues(multiBooleans, values, missing, true);
  }
}
