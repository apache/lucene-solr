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
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.math3.stat.regression.MultipleLinearRegression;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class OLSRegressionEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  public OLSRegressionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object ... values) throws IOException {

    Matrix observations = null;
    List<Number> outcomes = null;

    if(values[0] instanceof Matrix) {
      observations = (Matrix)values[0];
    } else {
      throw new IOException("The first parameter for olsRegress should be the observation matrix.");
    }

    if(values[1] instanceof List) {
      outcomes = (List) values[1];
    } else {
      throw new IOException("The second parameter for olsRegress should be outcome array. ");
    }

    double[][] observationData = observations.getData();
    double[] outcomeData = new double[outcomes.size()];
    for(int i=0; i<outcomeData.length; i++) {
      outcomeData[i] = outcomes.get(i).doubleValue();
    }

    OLSMultipleLinearRegression multipleLinearRegression = (OLSMultipleLinearRegression)regress(observationData, outcomeData);

    @SuppressWarnings({"rawtypes"})
    Map map = new HashMap();

    map.put("regressandVariance", multipleLinearRegression.estimateRegressandVariance());
    map.put("regressionParameters", list(multipleLinearRegression.estimateRegressionParameters()));
    map.put("RSquared", multipleLinearRegression.calculateRSquared());
    map.put("adjustedRSquared", multipleLinearRegression.calculateAdjustedRSquared());
    map.put("residualSumSquares", multipleLinearRegression.calculateResidualSumOfSquares());

    try {
      map.put("regressionParametersStandardErrors", list(multipleLinearRegression.estimateRegressionParametersStandardErrors()));
      map.put("regressionParametersVariance", new Matrix(multipleLinearRegression.estimateRegressionParametersVariance()));
    } catch (Exception e) {
      //Exception is thrown if the matrix is singular
    }

    return new MultipleRegressionTuple(multipleLinearRegression, map);
  }

  @SuppressWarnings({"unchecked"})
  private List<Number> list(double[] values) {
    @SuppressWarnings({"rawtypes"})
    List list = new ArrayList();
    for(double d : values) {
      list.add(d);
    }
    return list;
  }

  protected MultipleLinearRegression regress(double[][] observations, double[] outcomes) {
    OLSMultipleLinearRegression olsMultipleLinearRegression = new OLSMultipleLinearRegression();
    olsMultipleLinearRegression.newSampleData(outcomes, observations);
    return olsMultipleLinearRegression;
  }

  public static class MultipleRegressionTuple extends Tuple {

    private MultipleLinearRegression multipleLinearRegression;


    public MultipleRegressionTuple(MultipleLinearRegression multipleLinearRegression, Map<?,?> map) {
      super(map);
      this.multipleLinearRegression = multipleLinearRegression;
    }

    public double predict(double[] values) {
      @SuppressWarnings({"unchecked"})
      List<Number> weights = (List<Number>)get("regressionParameters");
      double prediction = 0.0;
      List<Number> predictors = new ArrayList<>();
      predictors.add(1.0D);
      for(double d : values) {
        predictors.add(d);
      }
      for(int i=0; i< predictors.size(); i++) {
        prediction += weights.get(i).doubleValue()*predictors.get(i).doubleValue();
      }

      return prediction;
    }
  }
}

