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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class RegressionEvaluator extends ComplexEvaluator implements Expressible {

  private static final long serialVersionUID = 1;

  public RegressionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    
    if(2 != subEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two columns but found %d",expression,subEvaluators.size()));
    }

  }

  public Tuple evaluate(Tuple tuple) throws IOException {

    StreamEvaluator colEval1 = subEvaluators.get(0);
    StreamEvaluator colEval2 = subEvaluators.get(1);

    List<Number> numbers1 = (List<Number>)colEval1.evaluate(tuple);
    List<Number> numbers2 = (List<Number>)colEval2.evaluate(tuple);
    double[] column1 = new double[numbers1.size()];
    double[] column2 = new double[numbers2.size()];

    for(int i=0; i<numbers1.size(); i++) {
      column1[i] = numbers1.get(i).doubleValue();
    }

    for(int i=0; i<numbers2.size(); i++) {
      column2[i] = numbers2.get(i).doubleValue();
    }

    SimpleRegression regression = new SimpleRegression();
    for(int i=0; i<column1.length; i++) {
      regression.addData(column1[i], column2[i]);
    }

    Map map = new HashMap();
    map.put("slope", regression.getSlope());
    map.put("intercept", regression.getIntercept());
    map.put("R", regression.getR());
    map.put("N", regression.getN());
    map.put("regressionSumSquares", regression.getRegressionSumSquares());
    map.put("slopeConfidenceInterval", regression.getSlopeConfidenceInterval());
    map.put("interceptStdErr", regression.getInterceptStdErr());
    map.put("totalSumSquares", regression.getTotalSumSquares());
    map.put("significance", regression.getSignificance());
    map.put("meanSquareError", regression.getMeanSquareError());
    return new RegressionTuple(regression, map);
  }

  public static class RegressionTuple extends Tuple {

    private SimpleRegression simpleRegression;

    public RegressionTuple(SimpleRegression simpleRegression, Map map) {
      super(map);
      this.simpleRegression = simpleRegression;
    }

    public double predict(double d) {
      return this.simpleRegression.predict(d);
    }
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(nodeId.toString())
        .withExpressionType(ExpressionType.EVALUATOR)
        .withFunctionName(factory.getFunctionName(getClass()))
        .withImplementingClass(getClass().getName())
        .withExpression(toExpression(factory).toString());
  }
}