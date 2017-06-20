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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class HistogramEvaluator extends ComplexEvaluator implements Expressible {

  private static final long serialVersionUID = 1;

  public HistogramEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    
    if(2 != subEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two values but found %d",expression,subEvaluators.size()));
    }
  }

  public List<Map> evaluate(Tuple tuple) throws IOException {

    StreamEvaluator colEval1 = subEvaluators.get(0);

    List<Number> numbers1 = (List<Number>)colEval1.evaluate(tuple);
    double[] column1 = new double[numbers1.size()];

    for(int i=0; i<numbers1.size(); i++) {
      column1[i] = numbers1.get(i).doubleValue();
    }

    int bins = 10;
    if(subEvaluators.size() == 2) {
      StreamEvaluator binsEval = subEvaluators.get(1);
      Number binsNum = (Number) binsEval.evaluate(tuple);
      bins = binsNum.intValue();
    }

    EmpiricalDistribution empiricalDistribution = new EmpiricalDistribution(bins);
    empiricalDistribution.load(column1);

    List<Map> binList = new ArrayList();

    List<SummaryStatistics> summaries = empiricalDistribution.getBinStats();
    for(SummaryStatistics statisticalSummary : summaries) {
      Map map = new HashMap();
      map.put("max", statisticalSummary.getMax());
      map.put("mean", statisticalSummary.getMean());
      map.put("min", statisticalSummary.getMin());
      map.put("stdev", statisticalSummary.getStandardDeviation());
      map.put("sum", statisticalSummary.getSum());
      map.put("N", statisticalSummary.getN());
      map.put("var", statisticalSummary.getVariance());
      binList.add(map);
    }

    return binList;
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