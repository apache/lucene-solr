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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.EvaluatorException;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
* Selects fields from the incoming stream and applies optional field renaming.
* Does not reorder the outgoing stream.
* @since 6.0.0
**/


public class SelectStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private StreamContext streamContext;
  private Map<String,String> selectedFields;
  private Map<StreamEvaluator,String> selectedEvaluators;
  private List<StreamOperation> operations;

  public SelectStream(TupleStream stream, List<String> selectedFields) throws IOException {
    this.stream = stream;
    this.selectedFields = new HashMap<>();
    for(String selectedField : selectedFields){
      this.selectedFields.put(selectedField, selectedField);
    }
    operations = new ArrayList<>();
    selectedEvaluators = new LinkedHashMap();
  }
  
  public SelectStream(TupleStream stream, Map<String,String> selectedFields) throws IOException {
    this.stream = stream;
    this.selectedFields = selectedFields;
    operations = new ArrayList<>();
    selectedEvaluators = new LinkedHashMap();
  }
  
  public SelectStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    List<StreamExpressionParameter> selectAsFieldsExpressions = factory.getOperandsOfType(expression, StreamExpressionValue.class);
    List<StreamExpression> operationExpressions = factory.getExpressionOperandsRepresentingTypes(expression, StreamOperation.class);
    List<StreamExpression> evaluatorExpressions = factory.getExpressionOperandsRepresentingTypes(expression, StreamEvaluator.class);
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + selectAsFieldsExpressions.size() + operationExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single stream but found %d (must be TupleStream types)",expression, streamExpressions.size()));
    }

    if(0 == selectAsFieldsExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one select field but found %d",expression, streamExpressions.size()));
    }
    
    if(0 != evaluatorExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - evaluators must be given a name, like 'add(...) as result' but found %d evaluators without names",expression, evaluatorExpressions.size()));
    }

    stream = factory.constructStream(streamExpressions.get(0));
    
    selectedFields = new HashMap<String,String>();
    selectedEvaluators = new LinkedHashMap();
    for(StreamExpressionParameter parameter : selectAsFieldsExpressions){
      StreamExpressionValue selectField = (StreamExpressionValue)parameter;
      String value = selectField.getValue().trim();
      
      // remove possible wrapping quotes
      if(value.length() > 2 && value.startsWith("\"") && value.endsWith("\"")){
        value = value.substring(1, value.length() - 1);
      }
      if(value.toLowerCase(Locale.ROOT).contains(" as ")){
        String[] parts = value.split("(?i) as "); // ensure we are splitting in a case-insensitive way
        if(2 != parts.length){
          throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting select field of form 'fieldA' or 'fieldA as alias' but found %s",expression, value));
        }
        
        String asValue = parts[0].trim();
        String asName = parts[1].trim();
        
        boolean handled = false;
        if(asValue.contains("(")){
          // possible evaluator
          try{
            StreamExpression asValueExpression = StreamExpressionParser.parse(asValue);
            if(factory.doesRepresentTypes(asValueExpression, StreamEvaluator.class)){
              selectedEvaluators.put(factory.constructEvaluator(asValueExpression), asName);
              handled = true;
            }
          } catch(Throwable e) {
            Throwable t = e;
            while(true) {
              if(t instanceof EvaluatorException) {
                throw new IOException(t);
              }
              t = t.getCause();
              if(t == null) {
                break;
              }
            }
            // it was not handled, so treat as a non-evaluator
          }
        }
        
        if(!handled){        
          selectedFields.put(asValue, asName);
        }
      }
      else{
        selectedFields.put(value,value);
      }
    }
    
    operations = new ArrayList<>();
    for(StreamExpression expr : operationExpressions){
      operations.add(factory.constructOperation(expr));
    }
  }
    
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    if(includeStreams){
      // stream
      if(stream instanceof Expressible){
        expression.addParameter(((Expressible)stream).toExpression(factory));
      }
      else{
        throw new IOException("This SelectStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }
    
    // selected fields
    for(Map.Entry<String, String> selectField : selectedFields.entrySet()) {
      if(selectField.getKey().equals(selectField.getValue())){
        expression.addParameter(selectField.getKey());
      }
      else{
        expression.addParameter(String.format(Locale.ROOT, "%s as %s", selectField.getKey(), selectField.getValue()));
      }
    }
    
    // selected evaluators
    for(Map.Entry<StreamEvaluator, String> selectedEvaluator : selectedEvaluators.entrySet()) {
      expression.addParameter(String.format(Locale.ROOT, "%s as %s", selectedEvaluator.getKey().toExpression(factory), selectedEvaluator.getValue()));
    }
    
    for(StreamOperation operation : operations){
      expression.addParameter(operation.toExpression(factory));
    }
    
    return expression;   
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    Explanation explanation = new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[]{
        stream.toExplanation(factory)
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory, false).toString());   
    
    for(StreamEvaluator evaluator : selectedEvaluators.keySet()){
      explanation.addHelper(evaluator.toExplanation(factory));
    }
    
    for(StreamOperation operation : operations){
      explanation.addHelper(operation.toExplanation(factory));
    }
    
    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
    this.stream.setStreamContext(context);
    Set<StreamEvaluator> evaluators = selectedEvaluators.keySet();

    for(StreamEvaluator evaluator : evaluators) {
      evaluator.setStreamContext(context);
    }
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(stream);
    return l;
  }

  public void open() throws IOException {
    stream.open();
  }

  public void close() throws IOException {
    stream.close();
  }

  public Tuple read() throws IOException {
    Tuple original = stream.read();
    
    if(original.EOF){
      return original;
    }

    // create a copy with the limited set of fields
    Tuple workingToReturn = new Tuple(new HashMap<>());
    Tuple workingForEvaluators = new Tuple(new HashMap<>());

    //Clear the TupleContext before running the evaluators.
    //The TupleContext allows evaluators to cache values within the scope of a single tuple.
    //For example a LocalDateTime could be parsed by one evaluator and used by other evaluators within the scope of the tuple.
    //This avoids the need to create multiple LocalDateTime instances for the same tuple to satisfy a select expression.

    streamContext.getTupleContext().clear();

    for(Object fieldName : original.fields.keySet()){
      workingForEvaluators.put(fieldName, original.get(fieldName));
      if(selectedFields.containsKey(fieldName)){
        workingToReturn.put(selectedFields.get(fieldName), original.get(fieldName));
      }
    }
    
    // apply all operations
    for(StreamOperation operation : operations){
      operation.operate(workingToReturn);
      operation.operate(workingForEvaluators);
    }
    
    // Apply all evaluators
    for(Map.Entry<StreamEvaluator, String> selectedEvaluator : selectedEvaluators.entrySet()) {
      Object o = selectedEvaluator.getKey().evaluate(workingForEvaluators);
      if(o != null) {
        workingForEvaluators.put(selectedEvaluator.getValue(), o);
        workingToReturn.put(selectedEvaluator.getValue(), o);
      }
    }
    
    return workingToReturn;
  }
  
  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    // apply aliasing to comparator
    return stream.getStreamSort().copyAliased(selectedFields);
  }

  public int getCost() {
    return 0;
  }
}
