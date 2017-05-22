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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.ComplexEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class CopyOfRangeEvaluator extends ComplexEvaluator implements Expressible {

  private static final long serialVersionUID = 1;

  public CopyOfRangeEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
  }

  public List<Number> evaluate(Tuple tuple) throws IOException {
    StreamEvaluator colEval1 = subEvaluators.get(0);

    List<Number> numbers1 = (List<Number>)colEval1.evaluate(tuple);
    double[] vals = new double[numbers1.size()];

    for(int i=0; i<vals.length; i++) {
      vals[i] = numbers1.get(i).doubleValue();
    }

    StreamEvaluator startIndexEval = subEvaluators.get(1);
    Number startIndexNum = (Number)startIndexEval.evaluate(tuple);
    int startIndex = startIndexNum.intValue();

    StreamEvaluator endIndexEval = subEvaluators.get(2);
    Number endIndexNum = (Number)endIndexEval.evaluate(tuple);
    int endIndex = endIndexNum.intValue();

    vals = Arrays.copyOfRange(vals, startIndex, endIndex);

    List<Number> copyOf = new ArrayList(vals.length);

    for(int i=0; i<vals.length; i++) {
      copyOf.add(vals[i]);
    }

    return copyOf;
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