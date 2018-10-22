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
package org.apache.solr.client.solrj.io.ops;

import java.io.IOException;
import java.util.Locale;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;

/**
 * Implementation of replace(...., withValue="some value")
 * See ReplaceOperation for description.
 */
public class ReplaceWithValueOperation implements StreamOperation {

  private static final long serialVersionUID = 1;
  private UUID operationNodeId = UUID.randomUUID();
  
  private boolean wasBuiltWithFieldName;
  private String fieldName;
  private Object original;
  private Object replacement;
  
  public ReplaceWithValueOperation(String forField, StreamExpression expression, StreamFactory factory) throws IOException {
    
    if(2 == expression.getParameters().size()){
      wasBuiltWithFieldName = false;
      
      this.fieldName = forField;
      this.original = factory.constructPrimitiveObject(factory.getValueOperand(expression, 0));

    }
    else if(3 == expression.getParameters().size()){
      wasBuiltWithFieldName = true;
      
      this.fieldName = factory.getValueOperand(expression, 0);
      this.original = factory.constructPrimitiveObject(factory.getValueOperand(expression, 1));
    }
    else{
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    StreamExpressionNamedParameter replacementParameter = factory.getNamedOperand(expression, "withValue");
    if(null == replacementParameter){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a parameter named 'withValue' but didn't find one.", expression));
    }
    if(!(replacementParameter.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting parameter named 'withValue' to be a primitive type.", expression));      
    }
    
    this.replacement = factory.constructPrimitiveObject(((StreamExpressionValue)replacementParameter.getParameter()).getValue());
  }
  
  @Override
  public void operate(Tuple tuple) {
    if(matchesOriginal(tuple)){
      replace(tuple);
    }
  }
  
  private boolean matchesOriginal(Tuple tuple){
    Object value = tuple.get(fieldName);
    
    if(null == value){
      return null == original;
    }
    else if(null != original){
      return original.equals(value);
    }
    
    return false;    
  }
  
  private void replace(Tuple tuple){
    if(null == replacement){
      tuple.remove(fieldName);
    }
    else{
      tuple.put(fieldName, replacement);
    }
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    if(wasBuiltWithFieldName){
      expression.addParameter(fieldName);
    }
    
    expression.addParameter(null == original ? "null" : original.toString());
    expression.addParameter(new StreamExpressionNamedParameter("withValue", null == replacement ? "null" : replacement.toString()));
    
    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(operationNodeId.toString())
      .withExpressionType(ExpressionType.OPERATION)
      .withFunctionName(factory.getFunctionName(getClass()))
      .withImplementingClass(getClass().getName())
      .withExpression(toExpression(factory).toString());
  }
}
