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
import java.util.List;
import java.util.Locale;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class KolmogorovSmirnovEvaluator extends ComplexEvaluator implements Expressible {

  private static final long serialVersionUID = 1;

  public KolmogorovSmirnovEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);

    if(subEvaluators.size() != 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least two values but found %d",expression,subEvaluators.size()));
    }
  }

  public Tuple evaluate(Tuple tuple) throws IOException {

    StreamEvaluator se1 = subEvaluators.get(0);
    StreamEvaluator se2 = subEvaluators.get(1);

    KolmogorovSmirnovTest ks = new KolmogorovSmirnovTest();
    List<Number> sample = (List<Number>)se2.evaluate(tuple);
    double[] data = new double[sample.size()];

    for(int i=0; i<data.length; i++) {
      data[i] = sample.get(i).doubleValue();
    }

    Object o = se1.evaluate(tuple);

    if(o instanceof RealDistribution) {
      RealDistribution realDistribution = (RealDistribution)o;
      double d = ks.kolmogorovSmirnovStatistic(realDistribution, data);
      double p = ks.kolmogorovSmirnovTest(realDistribution, data);


      Map m = new HashMap();
      m.put("p-value", p);
      m.put("d-statistic", d);
      return new Tuple(m);
    } else {
      List<Number> sample2 = (List<Number>)o;
      double[] data2 = new double[sample2.size()];
      for(int i=0; i<data2.length; i++) {
        data2[i] = sample2.get(i).doubleValue();
      }

      double d = ks.kolmogorovSmirnovStatistic(data, data2);
      //double p = ks.(data, data2);
      Map m = new HashMap();
      //m.put("p-value", p);
      m.put("d-statistic", d);
      return new Tuple(m);
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