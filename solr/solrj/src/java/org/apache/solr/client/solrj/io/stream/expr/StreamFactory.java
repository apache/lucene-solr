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

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.Equalitor;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.TupleStream;

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

/**
 * Used to convert strings into stream expressions
 */
public class StreamFactory implements Serializable {
  
  private transient HashMap<String,String> collectionZkHosts;
  private transient HashMap<String,Class> streamFunctions;
  
  public StreamFactory(){
    collectionZkHosts = new HashMap<String,String>();
    streamFunctions = new HashMap<String,Class>();
  }
  
  public StreamFactory withCollectionZkHost(String collectionName, String zkHost){
    this.collectionZkHosts.put(collectionName, zkHost);
    return this;
  }
  public String getCollectionZkHost(String collectionName){
    if(this.collectionZkHosts.containsKey(collectionName)){
      return this.collectionZkHosts.get(collectionName);
    }
    return null;
  }
  
  public Map<String,Class> getStreamFunctions(){
    return streamFunctions;
  }
  public StreamFactory withStreamFunction(String streamFunction, Class clazz){
    this.streamFunctions.put(streamFunction, clazz);
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
    List<StreamExpressionNamedParameter> namedParameters = new ArrayList<StreamExpressionNamedParameter>();
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
    List<StreamExpression> namedParameters = new ArrayList<StreamExpression>();
    for(StreamExpressionParameter parameter : getOperandsOfType(expression, StreamExpression.class)){
      namedParameters.add((StreamExpression)parameter);
    }
    
    return namedParameters;
  }
  public List<StreamExpression> getExpressionOperands(StreamExpression expression, String functionName){
    List<StreamExpression> namedParameters = new ArrayList<StreamExpression>();
    for(StreamExpressionParameter parameter : getOperandsOfType(expression, StreamExpression.class)){
      StreamExpression expressionOperand = (StreamExpression)parameter;
      if(expressionOperand.getFunctionName().equals(functionName)){
        namedParameters.add(expressionOperand);
      }
    }
    
    return namedParameters;
  }
  public List<StreamExpressionParameter> getOperandsOfType(StreamExpression expression, Class ... clazzes){
    List<StreamExpressionParameter> parameters = new ArrayList<StreamExpressionParameter>();
    
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
    List<StreamExpression> matchingStreamExpressions = new ArrayList<StreamExpression>();
    List<StreamExpression> allStreamExpressions = getExpressionOperands(expression);
    
    parameterLoop:
    for(StreamExpression streamExpression : allStreamExpressions){
      if(streamFunctions.containsKey(streamExpression.getFunctionName())){
        for(Class clazz : clazzes){
          if(!clazz.isAssignableFrom(streamFunctions.get(streamExpression.getFunctionName()))){
            continue parameterLoop;
          }
        }
        
        matchingStreamExpressions.add(streamExpression);
      }
    }
    
    return matchingStreamExpressions;   
  }
  
  public TupleStream constructStream(String expressionClause) throws IOException {
    return constructStream(StreamExpressionParser.parse(expressionClause));
  }
  public TupleStream constructStream(StreamExpression expression) throws IOException{
    String function = expression.getFunctionName();
    if(streamFunctions.containsKey(function)){
      Class clazz = streamFunctions.get(function);
      if(Expressible.class.isAssignableFrom(clazz) && TupleStream.class.isAssignableFrom(clazz)){
        TupleStream stream = (TupleStream)createInstance(streamFunctions.get(function), new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
        return stream;
      }
    }
    
    throw new IOException(String.format(Locale.ROOT,"Invalid stream expression %s - function '%s' is unknown (not mapped to a valid TupleStream)", expression, expression.getFunctionName()));
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
    else{
      String[] parts = comparatorString.split(" ");
      if(2 != parts.length){
        throw new IOException(String.format(Locale.ROOT,"Invalid comparator expression %s - expecting fieldName and order",comparatorString));
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

  public <T> T createInstance(Class<T> clazz, Class<?>[] paramTypes, Object[] params) throws IOException{
    // This should use SolrResourceLoader - TODO
    // This is adding a restriction that the class has a public constructor - we may not want to do that
    Constructor<T> ctor;
    try {
      ctor = clazz.getConstructor(paramTypes);
      return ctor.newInstance(params);
      
    } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new IOException(String.format(Locale.ROOT,"Unable to construct instance of %s", clazz.getName()),e);
    }
  }
  
  public String getFunctionName(Class clazz) throws IOException{
    for(Entry<String,Class> entry : streamFunctions.entrySet()){
      if(entry.getValue() == clazz){
        return entry.getKey();
      }
    }
    
    throw new IOException(String.format(Locale.ROOT, "Unable to find function name for class '%s'", clazz.getName()));
  }
}
