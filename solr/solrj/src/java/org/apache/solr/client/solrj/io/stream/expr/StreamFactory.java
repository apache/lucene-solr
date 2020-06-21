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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;

/**
 * Used to convert strings into stream expressions
 */
public class StreamFactory implements Serializable {
  
  private transient HashMap<String, String> collectionZkHosts;
  private transient HashMap<String, Supplier<Class<? extends Expressible>>> functionNames;
  private transient String defaultZkHost;
  private transient String defaultCollection;
  private transient String defaultSort;
  
  public StreamFactory(){
    collectionZkHosts = new HashMap<>();
    functionNames = new HashMap<>();
  }

  public StreamFactory(HashMap<String, Supplier<Class<? extends Expressible>>> functionNames) {
    this.functionNames = functionNames;
    collectionZkHosts = new HashMap<>();
  }
  
  public StreamFactory withCollectionZkHost(String collectionName, String zkHost) {
    this.collectionZkHosts.put(collectionName, zkHost);
    this.defaultCollection = collectionName;
    return this;
  }

  public String getDefaultCollection() {
    return defaultCollection;
  }

  public StreamFactory withDefaultZkHost(String zkHost) {
    this.defaultZkHost = zkHost;
    return this;
  }

  public Object clone() {
    //Shallow copy
    StreamFactory clone = new StreamFactory(functionNames);
    return clone.withCollectionZkHost(defaultCollection, defaultZkHost).withDefaultSort(defaultSort);
  }

  public StreamFactory withDefaultSort(String sort) {
    this.defaultSort = sort;
    return this;
  }

  public String getDefaultSort() {
    return this.defaultSort;
  }

  public String getDefaultZkHost() {
    return this.defaultZkHost;
  }

  public String getCollectionZkHost(String collectionName) {
    if (this.collectionZkHosts.containsKey(collectionName)) {
      return this.collectionZkHosts.get(collectionName);
    }
    return null;
  }
  
  public Map<String, Supplier<Class<? extends Expressible>>> getFunctionNames() {
    return Collections.unmodifiableMap(functionNames);
  }

  public StreamFactory withFunctionName(String functionName, Class<? extends Expressible> clazz) {
    this.functionNames.put(functionName, () -> clazz);
    return this;
  }

   public StreamFactory withFunctionName(String functionName, Supplier< Class<? extends Expressible>> clazz) {
    this.functionNames.put(functionName, clazz);
    return this;
  }

  public StreamFactory withoutFunctionName(String functionName) {
    this.functionNames.remove(functionName);
    return this;
  }

  public StreamExpressionParameter getOperand(StreamExpression expression, int parameterIndex) {
    if (null == expression.getParameters() || parameterIndex >= expression.getParameters().size()) {
      return null;
    }
    return expression.getParameters().get(parameterIndex);
  }
  
  public List<String> getValueOperands(StreamExpression expression) {
    return getOperandsOfType(expression, StreamExpressionValue.class).stream().map(item -> ((StreamExpressionValue) item).getValue()).collect(Collectors.toList());
  }
  
  /** Given an expression, will return the value parameter at the given index, or null if doesn't exist */
  public String getValueOperand(StreamExpression expression, int parameterIndex) {
    StreamExpressionParameter parameter = getOperand(expression, parameterIndex);
    if (null != parameter) {
      if (parameter instanceof StreamExpressionValue) {
        return ((StreamExpressionValue)parameter).getValue();
      } else if (parameter instanceof StreamExpression) {
        return parameter.toString();
      }
    }
    return null;
  }
  
  public List<StreamExpressionNamedParameter> getNamedOperands(StreamExpression expression) {
    List<StreamExpressionNamedParameter> namedParameters = new ArrayList<>();
    for (StreamExpressionParameter parameter : getOperandsOfType(expression, StreamExpressionNamedParameter.class)) {
      namedParameters.add((StreamExpressionNamedParameter) parameter);
    }
    return namedParameters;
  }

  public StreamExpressionNamedParameter getNamedOperand(StreamExpression expression, String name) {
    List<StreamExpressionNamedParameter> namedParameters = getNamedOperands(expression);
    for (StreamExpressionNamedParameter param : namedParameters) {
      if (param.getName().equals(name)) {
        return param;
      }
    }
    return null;
  }
  
  public List<StreamExpression> getExpressionOperands(StreamExpression expression) {
    List<StreamExpression> namedParameters = new ArrayList<>();
    for (StreamExpressionParameter parameter : getOperandsOfType(expression, StreamExpression.class)) {
      namedParameters.add((StreamExpression) parameter);
    }
    return namedParameters;
  }

  public List<StreamExpression> getExpressionOperands(StreamExpression expression, String functionName) {
    List<StreamExpression> namedParameters = new ArrayList<>();
    for (StreamExpressionParameter parameter : getOperandsOfType(expression, StreamExpression.class)) {
      StreamExpression expressionOperand = (StreamExpression) parameter;
      if (expressionOperand.getFunctionName().equals(functionName)) {
        namedParameters.add(expressionOperand);
      }
    }
    return namedParameters;
  }

  @SuppressWarnings({"unchecked"})
  public List<StreamExpressionParameter> getOperandsOfType(StreamExpression expression,
                                                           @SuppressWarnings({"rawtypes"})Class ... clazzes) {
    List<StreamExpressionParameter> parameters = new ArrayList<>();
    
    parameterLoop:
     for (StreamExpressionParameter parameter : expression.getParameters()) {
      for (@SuppressWarnings({"rawtypes"})Class clazz : clazzes) {
        if (!clazz.isAssignableFrom(parameter.getClass())) {
          continue parameterLoop; // go to the next parameter since this parameter cannot be assigned to at least one of the classes
        }
      }
      parameters.add(parameter);
    }
    return parameters;
  }
  
  @SuppressWarnings({"unchecked"})
  public List<StreamExpression> getExpressionOperandsRepresentingTypes(StreamExpression expression,
                                                                       @SuppressWarnings({"rawtypes"})Class ... clazzes) {
    List<StreamExpression> matchingStreamExpressions = new ArrayList<>();
    List<StreamExpression> allStreamExpressions = getExpressionOperands(expression);
    
    parameterLoop:
    for (StreamExpression streamExpression : allStreamExpressions) {
      Supplier<Class<? extends Expressible>> classSupplier = functionNames.get(streamExpression.getFunctionName());
      if (classSupplier != null) {
        for (@SuppressWarnings({"rawtypes"})Class clazz : clazzes) {
          if (!clazz.isAssignableFrom(classSupplier.get())) {
            continue parameterLoop;
          }
        }
        matchingStreamExpressions.add(streamExpression);
      }
    }
    return matchingStreamExpressions;   
  }
  
  @SuppressWarnings({"unchecked"})
  public boolean doesRepresentTypes(StreamExpression expression, @SuppressWarnings({"rawtypes"})Class ... clazzes) {
    Supplier<Class<? extends Expressible>> classSupplier = functionNames.get(expression.getFunctionName());
    if (classSupplier != null) {
      for (@SuppressWarnings({"rawtypes"})Class clazz : clazzes) {
        if (!clazz.isAssignableFrom(classSupplier.get())) {
          return false;
        }
      }
      return true;
    }
    return false;    
  }
  
  public int getIntOperand(StreamExpression expression, String paramName, Integer defaultValue) throws IOException {
    StreamExpressionNamedParameter param = getNamedOperand(expression, paramName);

    if (null == param || null == param.getParameter() || !(param.getParameter() instanceof StreamExpressionValue)) {
      if (null != defaultValue) {
        return defaultValue;
      }
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single '%s' parameter of type integer but didn't find one",expression, paramName));
    }
    String nStr = ((StreamExpressionValue) param.getParameter()).getValue();
    try {
      return Integer.parseInt(nStr);
    } catch (NumberFormatException e) {
      if (null != defaultValue) {
        return defaultValue;
      }
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - %s '%s' is not a valid integer.", expression, paramName, nStr));
    }
  }

  public boolean getBooleanOperand(StreamExpression expression, String paramName, Boolean defaultValue) throws IOException {
    StreamExpressionNamedParameter param = getNamedOperand(expression, paramName);
    
    if (null == param || null == param.getParameter() || !(param.getParameter() instanceof StreamExpressionValue)) {
      if (null != defaultValue) {
        return defaultValue;
      }
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single '%s' parameter of type boolean but didn't find one", expression, paramName));
    }
    String nStr = ((StreamExpressionValue) param.getParameter()).getValue();
    return Boolean.parseBoolean(nStr);
  }

  public TupleStream constructStream(String expressionClause) throws IOException {
    return constructStream(StreamExpressionParser.parse(expressionClause));
  }
  @SuppressWarnings({"rawtypes"})
  public TupleStream constructStream(StreamExpression expression) throws IOException {
    String function = expression.getFunctionName();
    Supplier<Class<? extends Expressible>> classSupplier = functionNames.get(function);

    if (classSupplier != null) {
      Class<? extends Expressible> clazz =  classSupplier.get();
      if (Expressible.class.isAssignableFrom(clazz) && TupleStream.class.isAssignableFrom(clazz)) {
        return (TupleStream)createInstance(clazz, new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
      }
    }
    
    throw new IOException(String.format(Locale.ROOT, "Invalid stream expression %s - function '%s' is unknown (not mapped to a valid TupleStream)", expression, expression.getFunctionName()));
  }
  
  public Metric constructMetric(String expressionClause) throws IOException {
    return constructMetric(StreamExpressionParser.parse(expressionClause));
  }

  @SuppressWarnings({"rawtypes"})
  public Metric constructMetric(StreamExpression expression) throws IOException {
    String function = expression.getFunctionName();
    Supplier<Class<? extends Expressible>> classSupplier = functionNames.get(function);
    if (classSupplier != null) {
      Class<? extends Expressible> clazz = classSupplier.get();
      if (Expressible.class.isAssignableFrom(clazz) && Metric.class.isAssignableFrom(clazz)) {
        return (Metric)createInstance(clazz, new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
      }
    }
    
    throw new IOException(String.format(Locale.ROOT, "Invalid metric expression %s - function '%s' is unknown (not mapped to a valid Metric)", expression, expression.getFunctionName()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public StreamComparator constructComparator(String comparatorString, @SuppressWarnings({"rawtypes"})Class comparatorType) throws IOException {
    if (comparatorString.contains(",")) {
      String[] parts = comparatorString.split(",");
      StreamComparator[] comps = new StreamComparator[parts.length];
      for (int idx = 0; idx < parts.length; ++idx) {
        comps[idx] = constructComparator(parts[idx].trim(), comparatorType);
      }
      return new MultipleFieldComparator(comps);
    } else if (comparatorString.contains("=")) {
      // expected format is "left=right order"
      String[] parts = comparatorString.split("[ =]");
      
      if (parts.length < 3) {
        throw new IOException(String.format(Locale.ROOT, "Invalid comparator expression %s - expecting 'left=right order'",comparatorString));
      }
      
      String leftFieldName = null;
      String rightFieldName = null;
      String order = null;
      for (String part : parts) {
        // skip empty
        if (null == part || 0 == part.trim().length()) { continue; }
        
        // assign each in order
        if (null == leftFieldName) {
          leftFieldName = part.trim(); 
        } else if (null == rightFieldName) {
          rightFieldName = part.trim(); 
        } else {
          order = part.trim();
          break; // we're done, stop looping
        }
      }
      
      if (null == leftFieldName || null == rightFieldName || null == order) {
        throw new IOException(String.format(Locale.ROOT, "Invalid comparator expression %s - expecting 'left=right order'",comparatorString));
      }
      
      return (StreamComparator) createInstance(comparatorType, new Class[]{ String.class, String.class, ComparatorOrder.class }, new Object[]{ leftFieldName, rightFieldName, ComparatorOrder.fromString(order) });
    } else {
      // expected format is "field order"
      String[] parts = comparatorString.split(" ");
      if (2 != parts.length) {
        throw new IOException(String.format(Locale.ROOT, "Invalid comparator expression %s - expecting 'field order'",comparatorString));
      }
      
      String fieldName = parts[0].trim();
      String order = parts[1].trim();
      
      return (StreamComparator) createInstance(comparatorType, new Class[]{ String.class, ComparatorOrder.class }, new Object[]{ fieldName, ComparatorOrder.fromString(order) });
    }
  }
    
  @SuppressWarnings({"unchecked", "rawtypes"})
  public StreamEqualitor constructEqualitor(String equalitorString, Class equalitorType) throws IOException {
    if (equalitorString.contains(",")) {
      String[] parts = equalitorString.split(",");
      StreamEqualitor[] eqs = new StreamEqualitor[parts.length];
      for (int idx = 0; idx < parts.length; ++idx) {
        eqs[idx] = constructEqualitor(parts[idx].trim(), equalitorType);
      }
      return new MultipleFieldEqualitor(eqs);
    } else {
      String leftFieldName;
      String rightFieldName;
      
      if (equalitorString.contains("=")) {
        String[] parts = equalitorString.split("=");
        if (2 != parts.length) {
          throw new IOException(String.format(Locale.ROOT, "Invalid equalitor expression %s - expecting fieldName=fieldName",equalitorString));
        }
        
        leftFieldName = parts[0].trim();
        rightFieldName = parts[1].trim();
      } else {
        leftFieldName = rightFieldName = equalitorString.trim();
      }
      
      return (StreamEqualitor) createInstance(equalitorType, new Class[]{ String.class, String.class }, new Object[]{ leftFieldName, rightFieldName });
    }
  }
  
  public Metric constructOperation(String expressionClause) throws IOException {
    return constructMetric(StreamExpressionParser.parse(expressionClause));
  }

  @SuppressWarnings({"rawtypes"})
  public StreamOperation constructOperation(StreamExpression expression) throws IOException {
    String function = expression.getFunctionName();
    Supplier<Class<? extends Expressible>> classSupplier = functionNames.get(function);
    if (classSupplier != null) {
      Class<? extends Expressible> clazz = classSupplier.get();
      if (Expressible.class.isAssignableFrom(clazz) && StreamOperation.class.isAssignableFrom(clazz)) {
        return (StreamOperation) createInstance(clazz, new Class[]{StreamExpression.class, StreamFactory.class}, new Object[]{expression, this});
      }
    }

    throw new IOException(String.format(Locale.ROOT, "Invalid operation expression %s - function '%s' is unknown (not mapped to a valid StreamOperation)", expression, expression.getFunctionName()));
  }
  
  public org.apache.solr.client.solrj.io.eval.StreamEvaluator constructEvaluator(String expressionClause) throws IOException {
    return constructEvaluator(StreamExpressionParser.parse(expressionClause));
  }

  @SuppressWarnings({"rawtypes"})
  public org.apache.solr.client.solrj.io.eval.StreamEvaluator constructEvaluator(StreamExpression expression) throws IOException {
    String function = expression.getFunctionName();
    Supplier<Class<? extends Expressible>> classSupplier = functionNames.get(function);

    if (classSupplier != null) {
      Class<? extends Expressible> clazz = classSupplier.get();
      if (Expressible.class.isAssignableFrom(clazz) && StreamEvaluator.class.isAssignableFrom(clazz)) {
        return (org.apache.solr.client.solrj.io.eval.StreamEvaluator)createInstance(clazz, new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
      }
    }
    
    throw new IOException(String.format(Locale.ROOT, "Invalid evaluator expression %s - function '%s' is unknown (not mapped to a valid StreamEvaluator)", expression, expression.getFunctionName()));
  }

  public boolean isStream(StreamExpression expression) throws IOException {
    String function = expression.getFunctionName();
    Supplier<Class<? extends Expressible>> classSupplier = functionNames.get(function);
    if (classSupplier != null) {
      Class<? extends Expressible> clazz = classSupplier.get();
      if (Expressible.class.isAssignableFrom(clazz) && TupleStream.class.isAssignableFrom(clazz)) {
        return true;
      }
    }

    return false;
  }

  public boolean isEvaluator(StreamExpression expression) throws IOException {
    String function = expression.getFunctionName();
    Supplier<Class<? extends Expressible>> classSupplier = functionNames.get(function);
    if (classSupplier != null) {
      Class<? extends Expressible> clazz = classSupplier.get();
      if (Expressible.class.isAssignableFrom(clazz) && StreamEvaluator.class.isAssignableFrom(clazz)) {
        return true;
      }
    }

    return false;
  }

  public <T> T createInstance(Class<T> clazz, Class<?>[] paramTypes, Object[] params) throws IOException {
    Constructor<T> ctor;
    try {
      ctor = clazz.getConstructor(paramTypes);
      return ctor.newInstance(params);

    } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      if (null != e.getMessage()) {
        throw new IOException(String.format(Locale.ROOT, "Unable to construct instance of %s caused by %s", clazz.getName(), e.getMessage()), e);
      } else {
        throw new IOException(String.format(Locale.ROOT, "Unable to construct instance of %s", clazz.getName()), e);
      }
    }
  }

  public String getFunctionName(Class<? extends Expressible> clazz) throws IOException {
    for (Entry<String, Supplier<Class<? extends Expressible>>> entry : functionNames.entrySet()) {
      if (entry.getValue().get() == clazz) {
        return entry.getKey();
      }
    }


    throw new IOException(String.format(Locale.ROOT, "Unable to find function name for class '%s'", clazz.getName()));
  }

  public Object constructPrimitiveObject(String original) {
    String lower = original.trim().toLowerCase(Locale.ROOT);

    if ("null".equals(lower)) { return null; }
    if ("true".equals(lower) || "false".equals(lower)){ return Boolean.parseBoolean(lower); }
    try { return Long.valueOf(original); } catch(Exception ignored) { };
    try { return Double.valueOf(original); } catch(Exception ignored) { };

    // is a string
    return original;
  }
}
