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
import java.util.Arrays;

import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class EmpiricalDistributionEvaluator extends ComplexEvaluator implements Expressible {

  private static final long serialVersionUID = 1;

  public EmpiricalDistributionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    
    if(1 != subEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting one column but found %d",expression,subEvaluators.size()));
    }
  }

  public Tuple evaluate(Tuple tuple) throws IOException {

    StreamEvaluator colEval1 = subEvaluators.get(0);

    List<Number> numbers1 = (List<Number>)colEval1.evaluate(tuple);
    double[] column1 = new double[numbers1.size()];

    for(int i=0; i<numbers1.size(); i++) {
      column1[i] = numbers1.get(i).doubleValue();
    }

    Arrays.sort(column1);
    EmpiricalDistribution empiricalDistribution = new EmpiricalDistribution();
    empiricalDistribution.load(column1);

    Map map = new HashMap();
    StatisticalSummary statisticalSummary = empiricalDistribution.getSampleStats();

    map.put("max", statisticalSummary.getMax());
    map.put("mean", statisticalSummary.getMean());
    map.put("min", statisticalSummary.getMin());
    map.put("stdev", statisticalSummary.getStandardDeviation());
    map.put("sum", statisticalSummary.getSum());
    map.put("N", statisticalSummary.getN());
    map.put("var", statisticalSummary.getVariance());

    return new EmpiricalDistributionTuple(empiricalDistribution, column1, map);
  }

  public static class EmpiricalDistributionTuple extends Tuple {

    private EmpiricalDistribution empiricalDistribution;
    private double[] backingArray;

    public EmpiricalDistributionTuple(EmpiricalDistribution empiricalDistribution, double[] backingArray, Map map) {
      super(map);
      this.empiricalDistribution = empiricalDistribution;
      this.backingArray = backingArray;
    }

    public double percentile(double d) {
      int slot = Arrays.binarySearch(backingArray, d);

      if(slot == 0) {
        return 0.0;
      }

      if(slot < 0) {
        if(slot == -1) {
          return 0.0D;
        } else {
          //Not a direct hit
          slot = Math.abs(slot);
          --slot;
          if(slot == backingArray.length) {
            return 1.0D;
          } else {
            return (this.empiricalDistribution.cumulativeProbability(backingArray[slot]));
          }
        }
      } else {
        return this.empiricalDistribution.cumulativeProbability(backingArray[slot]);
      }
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