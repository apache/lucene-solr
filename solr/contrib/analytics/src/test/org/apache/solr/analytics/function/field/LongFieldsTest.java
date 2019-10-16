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

public class LongFieldsTest extends AbstractAnalyticsFieldTest {

  @Test
  public void expressionFactoryCreationTest() {
    ExpressionFactory fact = getExpressionFactory();

    assertTrue(fact.createExpression("long_l_t") instanceof LongField);
    assertTrue(fact.createExpression("long_l_p") instanceof LongField);
    assertTrue(fact.createExpression("long_lm_t") instanceof LongMultiTrieField);
    assertTrue(fact.createExpression("long_lm_p") instanceof LongMultiPointField);
  }

  @Test
  public void singleValuedTrieLongTest() throws IOException {
    LongField valueField = new LongField("long_l_t");
    Map<String,Long> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      long value = valueField.getLong();
      if (valueField.exists()) {
        values.put(id, value);
      }
      return valueField.exists();
    });

    checkSingleFieldValues(singleLongs, values, missing);
  }

  @Test
  public void singleValuedPointLongTest() throws IOException {
    LongField valueField = new LongField("long_l_p");
    Map<String,Long> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      long value = valueField.getLong();
      if (valueField.exists()) {
        values.put(id, value);
      }
      return valueField.exists();
    });

    checkSingleFieldValues(singleLongs, values, missing);
  }

  @Test
  public void multiValuedTrieLongTest() throws IOException {
    LongMultiTrieField valueField = new LongMultiTrieField("long_lm_t");
    Map<String,Map<Long,Integer>> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      Map<Long, Integer> doc = new HashMap<>();
      valueField.streamLongs( value -> {
        doc.put(value, doc.getOrDefault(value, 0) + 1);
      });
      if (doc.size() > 0) {
        values.put(id, doc);
      }
      return doc.size() > 0;
    });

    checkMultiFieldValues(multiLongs, values, missing, true);
  }

  @Test
  public void multiValuedPointLongTest() throws IOException {
    LongMultiPointField valueField = new LongMultiPointField("long_lm_p");
    Map<String,Map<Long,Integer>> values = new HashMap<>();

    Set<String> missing = collectFieldValues(valueField, id -> {
      Map<Long, Integer> doc = new HashMap<>();
      valueField.streamLongs( value -> {
        doc.put(value, doc.getOrDefault(value, 0) + 1);
      });
      if (doc.size() > 0) {
        values.put(id, doc);
      }
      return doc.size() > 0;
    });

    checkMultiFieldValues(multiLongs, values, missing, false);
  }
}
