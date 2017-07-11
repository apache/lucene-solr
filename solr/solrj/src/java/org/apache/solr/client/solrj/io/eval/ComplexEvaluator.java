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
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class ComplexEvaluator implements StreamEvaluator {
  protected static final long serialVersionUID = 1L;
  protected StreamContext streamContext;
  
  protected UUID nodeId = UUID.randomUUID();
  
  protected StreamFactory constructingFactory;
  protected List<StreamEvaluator> subEvaluators = new ArrayList<StreamEvaluator>();
  
  public ComplexEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    this(expression, factory, new ArrayList<>());
  }
  
  public ComplexEvaluator(StreamExpression expression, StreamFactory factory, List<String> ignoredNamedParameters) throws IOException{
    constructingFactory = factory;
    
    // We have to do this because order of the parameters matter
    List<StreamExpressionParameter> parameters = factory.getOperandsOfType(expression, StreamExpressionParameter.class);
    
    for(StreamExpressionParameter parameter : parameters){
      if(parameter instanceof StreamExpression){
        // possible evaluator
        StreamExpression streamExpression = (StreamExpression)parameter;
        if(factory.doesRepresentTypes(streamExpression, ComplexEvaluator.class)){
          subEvaluators.add(factory.constructEvaluator(streamExpression));
        }
        else if(factory.doesRepresentTypes(streamExpression, SimpleEvaluator.class)){
          subEvaluators.add(factory.constructEvaluator(streamExpression));
        }
        else{
          // Will be treated as a field name
          subEvaluators.add(new FieldEvaluator(streamExpression.toString()));
        }
      }
      else if(parameter instanceof StreamExpressionValue){
        if(0 != ((StreamExpressionValue)parameter).getValue().length()){
          // special case - if evaluates to a number, boolean, or null then we'll treat it 
          // as a RawValueEvaluator
          Object value = factory.constructPrimitiveObject(((StreamExpressionValue)parameter).getValue());
          if(null == value || value instanceof Boolean || value instanceof Number){
            subEvaluators.add(new RawValueEvaluator(value));
          }
          else if(value instanceof String){
            subEvaluators.add(new FieldEvaluator((String)value));
          }
        }
      }
    }
    
    Set<String> namedParameters = factory.getNamedOperands(expression).stream().map(param -> param.getName()).collect(Collectors.toSet());
    long ignorableCount = ignoredNamedParameters.stream().filter(name -> namedParameters.contains(name)).count();
    
    if(0 != expression.getParameters().size() - subEvaluators.size() - ignorableCount){
      if(namedParameters.isEmpty()){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found - expecting only StreamEvaluators or field names", expression));
      }
      else{
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found - expecting only StreamEvaluators, field names, or named parameters [%s]", expression, namedParameters.stream().collect(Collectors.joining(","))));
      }
    }
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));
    
    for(StreamEvaluator evaluator : subEvaluators){
      expression.addParameter(evaluator.toExpression(factory));
    }
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
  
  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
    
    for(StreamEvaluator subEvaluator : subEvaluators){
      subEvaluator.setStreamContext(context);
    }
  }
  public StreamContext getStreamContext(){
    return streamContext;
  }
}
