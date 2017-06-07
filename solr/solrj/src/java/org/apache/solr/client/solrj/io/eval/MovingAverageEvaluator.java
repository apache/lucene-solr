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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class MovingAverageEvaluator extends ComplexEvaluator implements Expressible {

  private static final long serialVersionUID = 1;

  public MovingAverageEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  public List<Number> evaluate(Tuple tuple) throws IOException {

    if(subEvaluators.size() != 2) {
      throw new IOException("Moving average evaluator expects 2 parameters found: "+subEvaluators.size());
    }

    StreamEvaluator colEval = subEvaluators.get(0);
    StreamEvaluator windowEval = subEvaluators.get(1);

    int window = ((Number)windowEval.evaluate(tuple)).intValue();
    List<Number> numbers = (List<Number>)colEval.evaluate(tuple);

    if(window > numbers.size()) {
      throw new IOException("The window size cannot be larger then the array");
    }

    List<Number> moving = new ArrayList();

    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics(window);
    for(int i=0; i<numbers.size(); i++) {
      descriptiveStatistics.addValue(numbers.get(i).doubleValue());
      if(descriptiveStatistics.getN() >= window) {
        moving.add(descriptiveStatistics.getMean());
      }
    }

    return moving;
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