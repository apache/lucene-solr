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

package org.apache.lucene.queries.function.valuesource;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/** Function to raise the base "a" to the power "b"
 */
public class PowFloatFunction extends DualFloatFunction {
 /**
   * @param   a  the base.
   * @param   b  the exponent.
   */
  public PowFloatFunction(ValueSource a, ValueSource b) {
    super(a,b);
  }

  @Override
  protected String name() {
    return "pow";
  }

  @Override
  protected float func(int doc, FunctionValues aVals, FunctionValues bVals) {
    return (float)Math.pow(aVals.floatVal(doc), bVals.floatVal(doc));
  }
}


