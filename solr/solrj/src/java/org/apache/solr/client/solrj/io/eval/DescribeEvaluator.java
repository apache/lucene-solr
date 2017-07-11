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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class DescribeEvaluator extends ComplexEvaluator implements Expressible {

  private static final long serialVersionUID = 1;

  public DescribeEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    
    if(1 != subEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting one column but found %d",expression,subEvaluators.size()));
    }

  }

  public Tuple evaluate(Tuple tuple) throws IOException {

    StreamEvaluator colEval = subEvaluators.get(0);

    List<Number> numbers = (List<Number>)colEval.evaluate(tuple);
    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

    for(Number n : numbers) {
      descriptiveStatistics.addValue(n.doubleValue());
    }


    Map map = new HashMap();

    map.put("max", descriptiveStatistics.getMax());
    map.put("mean", descriptiveStatistics.getMean());
    map.put("min", descriptiveStatistics.getMin());
    map.put("stdev", descriptiveStatistics.getStandardDeviation());
    map.put("sum", descriptiveStatistics.getSum());
    map.put("N", descriptiveStatistics.getN());
    map.put("var", descriptiveStatistics.getVariance());
    map.put("kurtosis", descriptiveStatistics.getKurtosis());
    map.put("skewness", descriptiveStatistics.getSkewness());
    map.put("popVar", descriptiveStatistics.getPopulationVariance());
    map.put("geometricMean", descriptiveStatistics.getGeometricMean());
    map.put("sumsq", descriptiveStatistics.getSumsq());

    return new Tuple(map);
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