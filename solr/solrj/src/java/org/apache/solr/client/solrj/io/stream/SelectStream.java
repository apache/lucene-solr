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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
* Selects fields from the incoming stream and applies optional field renaming.
* Does not reorder the outgoing stream.
**/


public class SelectStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private Map<String,String> selectedFields;
  private List<StreamOperation> operations;

  public SelectStream(TupleStream stream, List<String> selectedFields) throws IOException {
    this.stream = stream;
    this.selectedFields = new HashMap<>();
    for(String selectedField : selectedFields){
      this.selectedFields.put(selectedField, selectedField);
    }
    operations = new ArrayList<>();
  }
  
  public SelectStream(TupleStream stream, Map<String,String> selectedFields) throws IOException {
    this.stream = stream;
    this.selectedFields = selectedFields;
    operations = new ArrayList<>();
  }
  
  public SelectStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    List<StreamExpressionParameter> selectFieldsExpressions = factory.getOperandsOfType(expression, StreamExpressionValue.class);
    List<StreamExpression> operationExpressions = factory.getExpressionOperandsRepresentingTypes(expression, StreamOperation.class);
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + selectFieldsExpressions.size() + operationExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single stream but found %d (must be TupleStream types)",expression, streamExpressions.size()));
    }

    if(0 == selectFieldsExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one select field but found %d",expression, streamExpressions.size()));
    }

    stream = factory.constructStream(streamExpressions.get(0));
    
    selectedFields = new HashMap<String,String>(selectFieldsExpressions.size());
    for(StreamExpressionParameter parameter : selectFieldsExpressions){
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
        selectedFields.put(parts[0].trim(), parts[1].trim());
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
  public StreamExpression toExpression(StreamFactory factory) throws IOException {    
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // stream
    if(stream instanceof Expressible){
      expression.addParameter(((Expressible)stream).toExpression(factory));
    }
    else{
      throw new IOException("This SelectStream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }
    
    // selects
    for(Map.Entry<String, String> selectField : selectedFields.entrySet()) {
      if(selectField.getKey().equals(selectField.getValue())){
        expression.addParameter(selectField.getKey());
      }
      else{
        expression.addParameter(String.format(Locale.ROOT, "%s as %s", selectField.getKey(), selectField.getValue()));
      }
    }
    
    for(StreamOperation operation : operations){
      expression.addParameter(operation.toExpression(factory));
    }
    
    return expression;   
  }

  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
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
    Tuple working = new Tuple(new HashMap<>());
    for(Object fieldName : original.fields.keySet()){
      if(selectedFields.containsKey(fieldName)){
        working.put(selectedFields.get(fieldName), original.get(fieldName));
      }
    }
    
    // apply all operations
    for(StreamOperation operation : operations){
      operation.operate(working);
    }
    
    return working;
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