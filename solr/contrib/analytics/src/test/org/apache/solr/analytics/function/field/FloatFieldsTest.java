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

public class FloatFieldsTest extends AbstractAnalyticsFieldTest {

  @Test
  public void expressionFactoryCreationTest() {
    ExpressionFactory fact = getExpressionFactory();

    assertTrue(fact.createExpression("float_f_t") instanceof FloatField);
    assertTrue(fact.createExpression("float_f_p") instanceof FloatField);
    assertTrue(fact.createExpression("float_fm_t") instanceof FloatMultiTrieField);
    assertTrue(fact.createExpression("float_fm_p") instanceof FloatMultiPointField);
  }

  @Test
  public void singleValuedTrieFloatTest() throws IOException {
    FloatField valueField = new FloatField("float_f_t");
    Map<String,Float> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      float value = valueField.getFloat();
      if (valueField.exists()) {
        values.put(id, value);
      }
      return valueField.exists();
    });

    checkSingleFieldValues(singleFloats, values, missing);
  }

  @Test
  public void singleValuedPointFloatTest() throws IOException {
    FloatField valueField = new FloatField("float_f_p");
    Map<String,Float> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      float value = valueField.getFloat();
      if (valueField.exists()) {
        values.put(id, value);
      }
      return valueField.exists();
    });

    checkSingleFieldValues(singleFloats, values, missing);
  }

  @Test
  public void multiValuedTrieFloatTest() throws IOException {
    FloatMultiTrieField valueField = new FloatMultiTrieField("float_fm_t");
    Map<String,Map<Float,Integer>> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      Map<Float, Integer> doc = new HashMap<>();
      valueField.streamFloats( value -> {
        doc.put(value, doc.getOrDefault(value, 0) + 1);
      });
      if (doc.size() > 0) {
        values.put(id, doc);
      }
      return doc.size() > 0;
    });

    checkMultiFieldValues(multiFloats, values, missing, true);
  }

  @Test
  public void multiValuedPointFloatTest() throws IOException {
    FloatMultiPointField valueField = new FloatMultiPointField("float_fm_p");
    Map<String,Map<Float,Integer>> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      Map<Float, Integer> doc = new HashMap<>();
      valueField.streamFloats( value -> {
        doc.put(value, doc.getOrDefault(value, 0) + 1);
      });
      if (doc.size() > 0) {
        values.put(id, doc);
      }
      return doc.size() > 0;
    });

    checkMultiFieldValues(multiFloats, values, missing, false);
  }
}
