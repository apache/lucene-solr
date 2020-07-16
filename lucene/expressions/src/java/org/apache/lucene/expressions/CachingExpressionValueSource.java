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

package org.apache.lucene.expressions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;

/**
 * This expression value source shares one value cache when generating {@link ExpressionFunctionValues}
 * such that only one value along the whole generation tree is corresponding to one name
 */
final class CachingExpressionValueSource extends ExpressionValueSource {

  CachingExpressionValueSource(Bindings bindings, Expression expression) {
    super(bindings, expression);
  }

  CachingExpressionValueSource(DoubleValuesSource[] variables, Expression expression, boolean needsScores) {
    super(variables, expression, needsScores);
  }

  public CachingExpressionValueSource(ExpressionValueSource expressionValueSource) {
    super(expressionValueSource.variables, expressionValueSource.expression, expressionValueSource.needsScores);
  }

  @Override
  public DoubleValues getValues(LeafReaderContext readerContext, DoubleValues scores) throws IOException {
    return getValuesWithCache(readerContext, scores, new HashMap<>());
  }

  private DoubleValues getValuesWithCache(LeafReaderContext readerContext, DoubleValues scores,
                                                  Map<String, DoubleValues> valuesCache) throws IOException {
    DoubleValues[] externalValues = new DoubleValues[expression.variables.length];

    for (int i = 0; i < variables.length; ++i) {
      String externalName = expression.variables[i];
      DoubleValues values = valuesCache.get(externalName);
      if (values == null) {
        if (variables[i] instanceof CachingExpressionValueSource) {
          values = ((CachingExpressionValueSource) variables[i]).getValuesWithCache(readerContext, scores, valuesCache);
        } else {
          values = variables[i].getValues(readerContext, scores);
        }
        if (values == null) {
          throw new RuntimeException("Unrecognized variable (" + externalName + ") referenced in expression (" +
              expression.sourceText + ").");
        }
        valuesCache.put(externalName, values);
      }
      externalValues[i] = zeroWhenUnpositioned(values);
    }

    return new ExpressionFunctionValues(expression, externalValues);
  }
}
