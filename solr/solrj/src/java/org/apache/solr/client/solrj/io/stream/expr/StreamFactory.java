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
package org.apache.solr.client.solrj.io.stream.expr;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;

/**
 * Used to convert strings into stream expressions
 */
public class StreamFactory implements Serializable {
  
  private transient HashMap<String,String> collectionZkHosts;
  private transient HashMap<String,Class<? extends Expressible>> functionNames;
  private transient String defaultZkHost;
  
  public StreamFactory(){
    collectionZkHosts = new HashMap<>();
    functionNames = new HashMap<>();
  }
  
  public StreamFactory withCollectionZkHost(String collectionName, String zkHost){
    this.collectionZkHosts.put(collectionName, zkHost);
    return this;
  }

  public StreamFactory withDefaultZkHost(String zkHost) {
    this.defaultZkHost = zkHost;
    return this;
  }

  public String getDefaultZkHost() {
    return this.defaultZkHost;
  }

  public String getCollectionZkHost(String collectionName){
    if(this.collectionZkHosts.containsKey(collectionName)){
      return this.collectionZkHosts.get(collectionName);
    }
    return null;
  }
  
  public Map<String,Class<? extends Expressible>> getFunctionNames(){
    return functionNames;
  }
  public StreamFactory withFunctionName(String functionName, Class<? extends Expressible> clazz){
    this.functionNames.put(functionName, clazz);
    return this;
  }
  
  public StreamExpressionParameter getOperand(StreamExpression expression, int parameterIndex){
    if(null == expression.getParameters() || parameterIndex >= expression.getParameters().size()){
      return null;
    }
    
    return expression.getParameters().get(parameterIndex);
  }
  
  /** Given an expression, will return the value parameter at the given index, or null if doesn't exist */
  public String getValueOperand(StreamExpression expression, int parameterIndex){
    StreamExpressionParameter parameter = getOperand(expression, parameterIndex);
    if(null != parameter){ 
      if(parameter instanceof StreamExpressionValue){
        return ((StreamExpressionValue)parameter).getValue();
      }
    }
    
    return null;
  }
  
  public List<StreamExpressionNamedParameter> getNamedOperands(StreamExpression expression){
    List<StreamExpressionNamedParameter> namedParameters = new ArrayList<>();
    for(StreamExpressionParameter parameter : getOperandsOfType(expression, StreamExpressionNamedParameter.class)){
      namedParameters.add((StreamExpressionNamedParameter)parameter);
    }
    
    return namedParameters;
  }
  public StreamExpressionNamedParameter getNamedOperand(StreamExpression expression, String name){
    List<StreamExpressionNamedParameter> namedParameters = getNamedOperands(expression);
    for(StreamExpressionNamedParameter param : namedParameters){
      if(param.getName().equals(name)){
        return param;
      }
    }
    
    return null;
  }
  
  public List<StreamExpression> getExpressionOperands(StreamExpression expression){
    List<StreamExpression> namedParameters = new ArrayList<>();
    for(StreamExpressionParameter parameter : getOperandsOfType(expression, StreamExpression.class)){
      namedParameters.add((StreamExpression)parameter);
    }
    
    return namedParameters;
  }
  public List<StreamExpression> getExpressionOperands(StreamExpression expression, String functionName){
    List<StreamExpression> namedParameters = new ArrayList<>();
    for(StreamExpressionParameter parameter : getOperandsOfType(expression, StreamExpression.class)){
      StreamExpression expressionOperand = (StreamExpression)parameter;
      if(expressionOperand.getFunctionName().equals(functionName)){
        namedParameters.add(expressionOperand);
      }
    }
    
    return namedParameters;
  }
  public List<StreamExpressionParameter> getOperandsOfType(StreamExpression expression, Class ... clazzes){
    List<StreamExpressionParameter> parameters = new ArrayList<>();
    
    parameterLoop:
     for(StreamExpressionParameter parameter : expression.getParameters()){
      for(Class clazz : clazzes){
        if(!clazz.isAssignableFrom(parameter.getClass())){
          continue parameterLoop; // go to the next parameter since this parameter cannot be assigned to at least one of the classes
        }
      }
      
      parameters.add(parameter);
    }
    
    return parameters;
  }
  
  public List<StreamExpression> getExpressionOperandsRepresentingTypes(StreamExpression expression, Class ... clazzes){
    List<StreamExpression> matchingStreamExpressions = new ArrayList<>();
    List<StreamExpression> allStreamExpressions = getExpressionOperands(expression);
    
    parameterLoop:
    for(StreamExpression streamExpression : allStreamExpressions){
      if(functionNames.containsKey(streamExpression.getFunctionName())){
        for(Class clazz : clazzes){
          if(!clazz.isAssignableFrom(functionNames.get(streamExpression.getFunctionName()))){
            continue parameterLoop;
          }
        }
        
        matchingStreamExpressions.add(streamExpression);
      }
    }
    
    return matchingStreamExpressions;   
  }
  
  public int getIntOperand(StreamExpression expression, String paramName, Integer defaultValue) throws IOException{
    StreamExpressionNamedParameter param = getNamedOperand(expression, paramName);
    
    if(null == param || null == param.getParameter() || !(param.getParameter() instanceof StreamExpressionValue)){
      if(null != defaultValue){
        return defaultValue;
      }
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single '%s' parameter of type integer but didn't find one",expression, paramName));
    }
    String nStr = ((StreamExpressionValue)param.getParameter()).getValue();
    try{
      return Integer.parseInt(nStr);
    }
    catch(NumberFormatException e){
      if(null != defaultValue){
        return defaultValue;
      }
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - %s '%s' is not a valid integer.",expression, paramName, nStr));
    }
  }

  public boolean getBooleanOperand(StreamExpression expression, String paramName, Boolean defaultValue) throws IOException{
    StreamExpressionNamedParameter param = getNamedOperand(expression, paramName);
    
    if(null == param || null == param.getParameter() || !(param.getParameter() instanceof StreamExpressionValue)){
      if(null != defaultValue){
        return defaultValue;
      }
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single '%s' parameter of type boolean but didn't find one",expression, paramName));
    }
    String nStr = ((StreamExpressionValue)param.getParameter()).getValue();
    return Boolean.parseBoolean(nStr);
  }

  
  public TupleStream constructStream(String expressionClause) throws IOException {
    return constructStream(StreamExpressionParser.parse(expressionClause));
  }
  public TupleStream constructStream(StreamExpression expression) throws IOException{
    String function = expression.getFunctionName();
    if(functionNames.containsKey(function)){
      Class<? extends Expressible> clazz = functionNames.get(function);
      if(Expressible.class.isAssignableFrom(clazz) && TupleStream.class.isAssignableFrom(clazz)){
        return (TupleStream)createInstance(functionNames.get(function), new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
      }
    }
    
    throw new IOException(String.format(Locale.ROOT,"Invalid stream expression %s - function '%s' is unknown (not mapped to a valid TupleStream)", expression, expression.getFunctionName()));
  }
  
  public Metric constructMetric(String expressionClause) throws IOException {
    return constructMetric(StreamExpressionParser.parse(expressionClause));
  }
  public Metric constructMetric(StreamExpression expression) throws IOException{
    String function = expression.getFunctionName();
    if(functionNames.containsKey(function)){
      Class<? extends Expressible> clazz = functionNames.get(function);
      if(Expressible.class.isAssignableFrom(clazz) && Metric.class.isAssignableFrom(clazz)){
        return (Metric)createInstance(functionNames.get(function), new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
      }
    }
    
    throw new IOException(String.format(Locale.ROOT,"Invalid metric expression %s - function '%s' is unknown (not mapped to a valid Metric)", expression, expression.getFunctionName()));
  }

  public StreamComparator constructComparator(String comparatorString, Class comparatorType) throws IOException {
    if(comparatorString.contains(",")){
      String[] parts = comparatorString.split(",");
      StreamComparator[] comps = new StreamComparator[parts.length];
      for(int idx = 0; idx < parts.length; ++idx){
        comps[idx] = constructComparator(parts[idx].trim(), comparatorType);
      }
      return new MultipleFieldComparator(comps);
    }
    else if(comparatorString.contains("=")){
      // expected format is "left=right order"
      String[] parts = comparatorString.split("[ =]");
      
      if(parts.length < 3){
        throw new IOException(String.format(Locale.ROOT,"Invalid comparator expression %s - expecting 'left=right order'",comparatorString));
      }
      
      String leftFieldName = null;
      String rightFieldName = null;
      String order = null;
      for(String part : parts){
        // skip empty
        if(null == part || 0 == part.trim().length()){ continue; }
        
        // assign each in order
        if(null == leftFieldName){ 
          leftFieldName = part.trim(); 
        }
        else if(null == rightFieldName){ 
          rightFieldName = part.trim(); 
        }
        else {
          order = part.trim();
          break; // we're done, stop looping
        }
      }
      
      if(null == leftFieldName || null == rightFieldName || null == order){
        throw new IOException(String.format(Locale.ROOT,"Invalid comparator expression %s - expecting 'left=right order'",comparatorString));
      }
      
      return (StreamComparator)createInstance(comparatorType, new Class[]{ String.class, String.class, ComparatorOrder.class }, new Object[]{ leftFieldName, rightFieldName, ComparatorOrder.fromString(order) });
    }
    else{
      // expected format is "field order"
      String[] parts = comparatorString.split(" ");
      if(2 != parts.length){
        throw new IOException(String.format(Locale.ROOT,"Invalid comparator expression %s - expecting 'field order'",comparatorString));
      }
      
      String fieldName = parts[0].trim();
      String order = parts[1].trim();
      
      return (StreamComparator)createInstance(comparatorType, new Class[]{ String.class, ComparatorOrder.class }, new Object[]{ fieldName, ComparatorOrder.fromString(order) });
    }
  }
    
  public StreamEqualitor constructEqualitor(String equalitorString, Class equalitorType) throws IOException {
    if(equalitorString.contains(",")){
      String[] parts = equalitorString.split(",");
      StreamEqualitor[] eqs = new StreamEqualitor[parts.length];
      for(int idx = 0; idx < parts.length; ++idx){
        eqs[idx] = constructEqualitor(parts[idx].trim(), equalitorType);
      }
      return new MultipleFieldEqualitor(eqs);
    }
    else{
      String leftFieldName;
      String rightFieldName;
      
      if(equalitorString.contains("=")){
        String[] parts = equalitorString.split("=");
        if(2 != parts.length){
          throw new IOException(String.format(Locale.ROOT,"Invalid equalitor expression %s - expecting fieldName=fieldName",equalitorString));
        }
        
        leftFieldName = parts[0].trim();
        rightFieldName = parts[1].trim();
      }
      else{
        leftFieldName = rightFieldName = equalitorString.trim();
      }
      
      return (StreamEqualitor)createInstance(equalitorType, new Class[]{ String.class, String.class }, new Object[]{ leftFieldName, rightFieldName });
    }
  }
  
  public Metric constructOperation(String expressionClause) throws IOException {
    return constructMetric(StreamExpressionParser.parse(expressionClause));
  }
  public StreamOperation constructOperation(StreamExpression expression) throws IOException{
    String function = expression.getFunctionName();
    if(functionNames.containsKey(function)){
      Class<? extends Expressible> clazz = functionNames.get(function);
      if(Expressible.class.isAssignableFrom(clazz) && StreamOperation.class.isAssignableFrom(clazz)){
        return (StreamOperation)createInstance(functionNames.get(function), new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
      }
    }
    
    throw new IOException(String.format(Locale.ROOT,"Invalid operation expression %s - function '%s' is unknown (not mapped to a valid StreamOperation)", expression, expression.getFunctionName()));
  }


  public <T> T createInstance(Class<T> clazz, Class<?>[] paramTypes, Object[] params) throws IOException{
    Constructor<T> ctor;
    try {
      ctor = clazz.getConstructor(paramTypes);
      return ctor.newInstance(params);
      
    } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      if(null != e.getMessage()){
        throw new IOException(String.format(Locale.ROOT,"Unable to construct instance of %s caused by %s", clazz.getName(), e.getMessage()),e);
      }
      else{
        throw new IOException(String.format(Locale.ROOT,"Unable to construct instance of %s", clazz.getName()),e);
      }
    }
  }
  
  public String getFunctionName(Class<? extends Expressible> clazz) throws IOException{
    for(Entry<String,Class<? extends Expressible>> entry : functionNames.entrySet()){
      if(entry.getValue() == clazz){
        return entry.getKey();
      }
    }
    
    throw new IOException(String.format(Locale.ROOT, "Unable to find function name for class '%s'", clazz.getName()));
  }
  
  public Object constructPrimitiveObject(String original){
    String lower = original.trim().toLowerCase(Locale.ROOT);
    
    if("null".equals(lower)){ return null; }
    if("true".equals(lower) || "false".equals(lower)){ return Boolean.parseBoolean(lower); }
    try{ return Long.valueOf(original); } catch(Exception ignored){};
    try{ if (original.matches(".{1,8}")){ return Float.valueOf(original); }} catch(Exception ignored){};
    try{ if (original.matches(".{1,17}")){ return Double.valueOf(original); }} catch(Exception ignored){};
    
    // is a string
    return original;
  }
}
