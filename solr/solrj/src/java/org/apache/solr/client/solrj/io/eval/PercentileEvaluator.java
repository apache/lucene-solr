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

import java.util.List;
import java.util.Locale;
import java.io.IOException;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

public class PercentileEvaluator extends ComplexEvaluator implements Expressible {

  private static final long serialVersionUID = 1;

  public PercentileEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    
    if(2 != subEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two values (array and number) but found %d",expression,subEvaluators.size()));
    }
  }

  public Number evaluate(Tuple tuple) throws IOException {
    StreamEvaluator colEval = subEvaluators.get(0);
    List<Number> column = (List<Number>)colEval.evaluate(tuple);

    double[] data = new double[column.size()];
    for(int i=0; i<data.length; i++) {
      data[i] = column.get(i).doubleValue();
    }

    Percentile percentile = new Percentile();
    percentile.setData(data);
    StreamEvaluator numEval = subEvaluators.get(1);
    Number num  = (Number)numEval.evaluate(tuple);
    return percentile.evaluate(num.doubleValue());
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