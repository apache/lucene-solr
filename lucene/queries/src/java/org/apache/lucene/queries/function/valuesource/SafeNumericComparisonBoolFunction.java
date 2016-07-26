package org.apache.lucene.queries.function.valuesource;

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

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.queries.function.docvalues.LongDocValues;

public abstract class SafeNumericComparisonBoolFunction extends ComparisonBoolFunction {

  public SafeNumericComparisonBoolFunction(ValueSource lhs, ValueSource rhs, String name) {
    super(lhs, rhs, name);
  }

  public abstract <T extends Comparable<T>> boolean compare(T lhs, T rhs);

  @Override
  public boolean compare(int doc, FunctionValues lhs, FunctionValues rhs) {
    // performs the safest possible numeric comparison, if both lhs and rhs are Longs, then
    // we perform a Long comparison to avoid the issues with precision when casting to doubles
    boolean lhsAnInt = (lhs instanceof LongDocValues || lhs instanceof IntDocValues);
    boolean rhsAnInt = (rhs instanceof LongDocValues || rhs instanceof IntDocValues);
    if (lhsAnInt && rhsAnInt) {
      return compare(lhs.longVal(doc), rhs.longVal(doc));
    }
    return compare(lhs.doubleVal(doc), rhs.doubleVal(doc));
  }



}
