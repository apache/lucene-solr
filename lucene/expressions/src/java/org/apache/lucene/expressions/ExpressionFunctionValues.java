package org.apache.lucene.expressions;
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
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;

/** A {@link FunctionValues} which evaluates an expression */
class ExpressionFunctionValues extends DoubleDocValues {
  final Expression expression;
  final FunctionValues[] functionValues;
  
  int currentDocument = -1;
  double currentValue;
  
  ExpressionFunctionValues(ValueSource parent, Expression expression, FunctionValues[] functionValues) {
    super(parent);
    if (expression == null) {
      throw new NullPointerException();
    }
    if (functionValues == null) {
      throw new NullPointerException();
    }
    this.expression = expression;
    this.functionValues = functionValues;
  }
  
  @Override
  public double doubleVal(int document) {
    if (currentDocument != document) {
      currentDocument = document;
      currentValue = expression.evaluate(document, functionValues);
    }
    
    return currentValue;
  }
}
