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

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class RawValueEvaluator extends SourceEvaluator {
  private static final long serialVersionUID = 1L;
  
  private Object value;
  
  public RawValueEvaluator(Object value){
    init(value);
  }
  
  public RawValueEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    // We have to do this because order of the parameters matter
    List<StreamExpressionParameter> parameters = factory.getOperandsOfType(expression, StreamExpressionValue.class);
    
    if(expression.getParameters().size() != parameters.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found - expecting only raw values", expression));
    }
    
    if(1 != parameters.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - only 1 value can exist in a %s(...) evaluator", expression, factory.getFunctionName(getClass())));
    }
    
    init(factory.constructPrimitiveObject(((StreamExpressionValue)parameters.get(0)).getValue()));
  }
  
  private void init(Object value){
    if(value instanceof Integer){
      this.value = ((Integer)value).longValue();
    }
    else if(value instanceof Float){
      this.value = ((Float)value).doubleValue();
    }
    else{
      this.value = value;
    }
  }
  
  @Override
  public Object evaluate(Tuple tuple) {
    return value;
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));
    expression.addParameter(new StreamExpressionValue(value.toString()));
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(nodeId.toString())
      .withExpressionType(ExpressionType.EVALUATOR)
      .withImplementingClass(getClass().getName())
      .withExpression(toExpression(factory).toString());
  }

}
