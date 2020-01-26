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
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class RecursiveEvaluator implements StreamEvaluator, ValueWorker {
  protected static final long serialVersionUID = 1L;
  protected StreamContext streamContext;
  
  protected UUID nodeId = UUID.randomUUID();
  
  protected StreamFactory constructingFactory;
  
  protected List<StreamEvaluator> containedEvaluators = new ArrayList<StreamEvaluator>();
  
  public RecursiveEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    this(expression, factory, new ArrayList<>());
  }
  
  protected Object normalizeInputType(Object value){
    if(null == value){
      return null;
    } else if (value instanceof VectorFunction) {
      return value;
    }
    else if(value instanceof Double){
      if(Double.isNaN((Double)value)){
        return null;
      }
      return new BigDecimal(value.toString());
    }
    else if(value instanceof BigDecimal){
      return (BigDecimal)value;
    }
    else if(value instanceof Number){
      return new BigDecimal(value.toString());
    }
    else if(value instanceof Collection){
      //Let's first check to see if we have a List of Strings.
      //If we do let's try and convert to a list of doubles and see what happens
      try {
        List<Number> vector = new ArrayList();
        boolean allDoubles = true;
        for(Object o : (Collection)value) {
          if(o instanceof String) {
            Double d = Double.parseDouble(o.toString());
            vector.add(d);
          } else {
            allDoubles = false;
            break;
          }
        }
        if(allDoubles) {
          return vector;
        }
      } catch(Exception e) {

      }

      return ((Collection<?>)value).stream().map(innerValue -> normalizeInputType(innerValue)).collect(Collectors.toList());
    }
    else if(value.getClass().isArray()){
      Stream<?> stream = Stream.empty();
      if(value instanceof double[]){
        stream = Arrays.stream((double[])value).boxed();
      }
      else if(value instanceof int[]){
        stream = Arrays.stream((int[])value).boxed();
      }
      else if(value instanceof long[]){
        stream = Arrays.stream((long[])value).boxed();
      }
      else if(value instanceof String[]){
        stream = Arrays.stream((String[])value);
      }      
      return stream.map(innerValue -> normalizeInputType(innerValue)).collect(Collectors.toList());
    }
    else{
      // anything else can just be returned as is
      return value;
    }

  }
  
  protected Object normalizeOutputType(Object value) {
    if(null == value){
      return null;
    } else if (value instanceof VectorFunction) {
      return value;
    } else if(value instanceof BigDecimal){
      BigDecimal bd = (BigDecimal)value;
      return bd.doubleValue();
    }
    else if(value instanceof Long || value instanceof Integer) {
      return ((Number) value).longValue();
    }
    else if(value instanceof Double){
      return value;
    }
    else if(value instanceof Number){
      return ((Number) value).doubleValue();
    }
    else if(value instanceof List){
      // normalize each value in the list
      return ((List<?>)value).stream().map(innerValue -> normalizeOutputType(innerValue)).collect(Collectors.toList());
    } else if(value instanceof Tuple && value.getClass().getEnclosingClass() == null) {
      //If its a tuple and not a inner class that has extended tuple, which is done in a number of cases so that mathematical models
      //can be contained within a tuple.

      Tuple tuple = (Tuple)value;
      Map map = new HashMap();
      for(Object o : tuple.fields.keySet()) {
        Object v = tuple.fields.get(o);
        map.put(o, normalizeOutputType(v));
      }
      return new Tuple(map);
    }
    else{
      // anything else can just be returned as is
      return value;
    }

  }
    
  public RecursiveEvaluator(StreamExpression expression, StreamFactory factory, List<String> ignoredNamedParameters) throws IOException{
    this.constructingFactory = factory;
    
    // We have to do this because order of the parameters matter
    List<StreamExpressionParameter> parameters = factory.getOperandsOfType(expression, StreamExpressionParameter.class);
    
    for(StreamExpressionParameter parameter : parameters){
      if(parameter instanceof StreamExpression){
        // possible evaluator
        StreamExpression streamExpression = (StreamExpression)parameter;
        if(factory.doesRepresentTypes(streamExpression, RecursiveEvaluator.class)){
          containedEvaluators.add(factory.constructEvaluator(streamExpression));
        }
        else if(factory.doesRepresentTypes(streamExpression, SourceEvaluator.class)){
          containedEvaluators.add(factory.constructEvaluator(streamExpression));
        }
        else{
          // Will be treated as a field name
          containedEvaluators.add(new FieldValueEvaluator(streamExpression.toString()));
        }
      }
      else if(parameter instanceof StreamExpressionValue){
        if(0 != ((StreamExpressionValue)parameter).getValue().length()){
          // special case - if evaluates to a number, boolean, or null then we'll treat it 
          // as a RawValueEvaluator
          Object value = factory.constructPrimitiveObject(((StreamExpressionValue)parameter).getValue());
          if(null == value || value instanceof Boolean || value instanceof Number){
            containedEvaluators.add(new RawValueEvaluator(value));
          }
          else if(value instanceof String){
            containedEvaluators.add(new FieldValueEvaluator((String)value));
          }
        }
      }
    }
    
    Set<String> namedParameters = factory.getNamedOperands(expression).stream().map(param -> param.getName()).collect(Collectors.toSet());
    long ignorableCount = ignoredNamedParameters.stream().filter(name -> namedParameters.contains(name)).count();
    /*
    if(0 != expression.getParameters().size() - containedEvaluators.size() - ignorableCount){
      if(namedParameters.isEmpty()){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found - expecting only StreamEvaluators or field names", expression));
      }
      else{
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found - expecting only StreamEvaluators, field names, or named parameters [%s]", expression, namedParameters.stream().collect(Collectors.joining(","))));
      }
    }
    */
  }
  
  @Override
  public Object evaluate(Tuple tuple) throws IOException {    
    try{
      List<Object> containedResults = recursivelyEvaluate(tuple);
      // this needs to be treated as an array of objects when going into doWork(Object ... values)
      return normalizeOutputType(doWork(containedResults.toArray()));
    }
    catch(UncheckedIOException e){
      throw e.getCause();
    }
  }  
  
  public List<Object> recursivelyEvaluate(Tuple tuple) throws IOException {
    List<Object> results = new ArrayList<>();
    try{
      for(StreamEvaluator containedEvaluator : containedEvaluators){
        results.add(normalizeInputType(containedEvaluator.evaluate(tuple)));
      }
    }
    catch(StreamEvaluatorException e){
      throw new IOException(String.format(Locale.ROOT, "Failed to evaluate expression %s - %s", toExpression(constructingFactory), e.getMessage()), e);
    }
    
    return results;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));
    
    for(StreamEvaluator evaluator : containedEvaluators){
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
    
    for(StreamEvaluator containedEvaluator : containedEvaluators){
      containedEvaluator.setStreamContext(context);
    }
  }
  public StreamContext getStreamContext(){
    return streamContext;
  }
}
