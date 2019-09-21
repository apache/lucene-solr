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

package org.apache.solr.search.function;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ComparisonBoolFunction;

/**
 * Compares two values for equality.
 * It should work on not only numbers but strings and custom things.
 *
 * @since 7.4
 */
public class EqualFunction extends ComparisonBoolFunction {

  public EqualFunction(ValueSource lhs, ValueSource rhs, String name) {
    super(lhs, rhs, name);
  }

  @Override
  public boolean compare(int doc, FunctionValues lhs, FunctionValues rhs) throws IOException {
    Object objL = lhs.objectVal(doc);
    Object objR = rhs.objectVal(doc);
    if (isNumeric(objL) && isNumeric(objR)) {
      if (isInteger(objL) && isInteger(objR)) {
        return Long.compare(((Number)objL).longValue(), ((Number)objR).longValue()) == 0;
      } else {
        return Double.compare(((Number)objL).doubleValue(), ((Number)objR).doubleValue()) == 0;
      }
    } else {
      return Objects.equals(objL, objR);
    }
  }

  private static boolean isInteger(Object obj) {
    return obj instanceof Integer || obj instanceof Long;
  }

  private static boolean isNumeric(Object obj) {
    return obj instanceof Number;
  }
}
