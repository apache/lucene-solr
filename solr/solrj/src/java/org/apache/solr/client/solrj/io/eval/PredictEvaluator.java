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
package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class PredictEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public PredictEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object first, Object second) throws IOException {
    if (null == first) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the first value", toExpression(constructingFactory)));
    }
    if (null == second) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - null found for the second value", toExpression(constructingFactory)));
    }

    if (!(first instanceof VectorFunction) && !(first instanceof RegressionEvaluator.RegressionTuple) && !(first instanceof OLSRegressionEvaluator.MultipleRegressionTuple)) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the first value, expecting a RegressionTuple", toExpression(constructingFactory), first.getClass().getSimpleName()));
    }

    if (!(second instanceof Number) && !(second instanceof List<?>) && !(second instanceof Matrix)) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - found type %s for the second value, expecting a Number, Array or Matrix", toExpression(constructingFactory), first.getClass().getSimpleName()));
    }

    if (first instanceof RegressionEvaluator.RegressionTuple) {

      RegressionEvaluator.RegressionTuple regressedTuple = (RegressionEvaluator.RegressionTuple) first;
      if (second instanceof Number) {
        return regressedTuple.predict(((Number) second).doubleValue());
      } else {
        return ((List<?>) second).stream().map(value -> regressedTuple.predict(((Number) value).doubleValue())).collect(Collectors.toList());
      }

    } else if (first instanceof OLSRegressionEvaluator.MultipleRegressionTuple) {

      OLSRegressionEvaluator.MultipleRegressionTuple regressedTuple = (OLSRegressionEvaluator.MultipleRegressionTuple) first;
      if (second instanceof List) {
        List<Number> list = (List<Number>) second;
        double[] predictors = new double[list.size()];

        for (int i = 0; i < list.size(); i++) {
          predictors[i] = list.get(i).doubleValue();
        }

        return regressedTuple.predict(predictors);
      } else if (second instanceof Matrix) {

        Matrix m = (Matrix) second;
        double[][] data = m.getData();
        List<Number> predictions = new ArrayList();
        for (double[] predictors : data) {
          predictions.add(regressedTuple.predict(predictors));
        }
        return predictions;
      }

    } else if (first instanceof VectorFunction) {
      VectorFunction vectorFunction = (VectorFunction) first;
      UnivariateFunction univariateFunction = (UnivariateFunction)vectorFunction.getFunction();
      if (second instanceof Number) {
        double x = ((Number)second).doubleValue();
        return univariateFunction.value(x);
      } else {
        return ((List<?>) second).stream().map(value -> univariateFunction.value(((Number) value).doubleValue())).collect(Collectors.toList());
      }
    }

    return null;
  }
}
