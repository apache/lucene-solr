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

import org.apache.commons.math3.analysis.BivariateFunction;
import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class PredictEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public PredictEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object ... objects) throws IOException {
    if(objects.length != 2 && objects.length != 3) {
      throw new IOException("The predict function expects 2 or 3 parameters.");
    }

    Object first = objects[0];
    Object second = objects[1];

    if (!(first instanceof BivariateFunction) &&
        !(first instanceof VectorFunction) &&
        !(first instanceof RegressionEvaluator.RegressionTuple) &&
        !(first instanceof OLSRegressionEvaluator.MultipleRegressionTuple) &&
        !(first instanceof KnnRegressionEvaluator.KnnRegressionTuple)) {
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
        @SuppressWarnings({"unchecked"})
        List<Number> list = (List<Number>) second;
        double[] predictors = new double[list.size()];

        for (int i = 0; i < list.size(); i++) {
          predictors[i] = list.get(i).doubleValue();
        }

        return regressedTuple.predict(predictors);
      } else if (second instanceof Matrix) {

        Matrix m = (Matrix) second;
        double[][] data = m.getData();
        List<Number> predictions = new ArrayList<>();
        for (double[] predictors : data) {
          predictions.add(regressedTuple.predict(predictors));
        }
        return predictions;
      }

    } else if (first instanceof KnnRegressionEvaluator.KnnRegressionTuple) {
      KnnRegressionEvaluator.KnnRegressionTuple regressedTuple = (KnnRegressionEvaluator.KnnRegressionTuple) first;

      if(regressedTuple.getBivariate()) {
        //Handle bi-variate regression
        if(second instanceof Number) {
          double[] predictors = new double[1];
          predictors[0] = ((Number)second).doubleValue();
          return regressedTuple.predict(predictors);
        } else if(second instanceof List) {
          @SuppressWarnings({"unchecked"})
          List<Number> vec = (List<Number>)second;
          List<Number> predictions = new ArrayList<>();
          for(Number num : vec) {
            double[] predictors = new double[1];
            predictors[0] = num.doubleValue();
            predictions.add(regressedTuple.predict(predictors));
          }
          return predictions;
        }
      } else {
        //Handle multi-variate regression
        if (second instanceof List) {
          @SuppressWarnings({"unchecked"})
          List<Number> list = (List<Number>) second;
          double[] predictors = new double[list.size()];

          for (int i = 0; i < list.size(); i++) {
            predictors[i] = list.get(i).doubleValue();
          }

          if (regressedTuple.getScale()) {
            predictors = regressedTuple.scale(predictors);
          }

          return regressedTuple.predict(predictors);
        } else if (second instanceof Matrix) {

          Matrix m = (Matrix) second;
          if (regressedTuple.getScale()) {
            m = regressedTuple.scale(m);
          }
          double[][] data = m.getData();
          List<Number> predictions = new ArrayList<>();
          for (double[] predictors : data) {
            predictions.add(regressedTuple.predict(predictors));
          }
          return predictions;
        }
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
    } else if(first instanceof BivariateFunction) {
      BivariateFunction bivariateFunction = (BivariateFunction) first;
      if (objects.length == 3) {
        Object third = objects[2];
        double x = 0.0;
        double y = 0.0;
        if (second instanceof Number && third instanceof Number) {
          x = ((Number) second).doubleValue();
          y = ((Number) third).doubleValue();
          return bivariateFunction.value(x, y);
        } else {
          throw new IOException("BivariateFunction requires two numberic parameters.");
        }
      } else if (objects.length == 2) {
        if (second instanceof Matrix) {
          Matrix m = (Matrix) second;
          double[][] data = m.getData();
          if (data[0].length == 2) {
            List<Number> out = new ArrayList<>();
            for (double[] row : data) {
              out.add(bivariateFunction.value(row[0], row[1]));
            }
            return out;
          } else {
            throw new IOException("Bivariate Function expects a matrix with two columns");
          }
        } else {
          throw new IOException("Bivariate Function requires a matrix parameter.");
        }
      }
    }
    return null;
  }
}
