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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.FieldValueEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * @since 6.6.0
 */
public class CartesianProductStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private List<NamedEvaluator> evaluators;
  private StreamComparator orderBy;
  
  // Used to contain the sorted queue of generated tuples
  private LinkedList<Tuple> generatedTuples;
  
  public CartesianProductStream(StreamExpression expression,StreamFactory factory) throws IOException {
    String functionName = factory.getFunctionName(getClass());
    
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    List<StreamExpressionParameter> evaluateAsExpressions = factory.getOperandsOfType(expression, StreamExpressionValue.class);
    StreamExpressionNamedParameter orderByExpression = factory.getNamedOperand(expression, "productSort");
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + evaluateAsExpressions.size() + (null == orderByExpression ? 0 : 1)){
      throw new IOException(String.format(Locale.ROOT,"Invalid %s expression %s - unknown operands found", functionName, expression));
    }
    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid %s expression %s - expecting single stream but found %d (must be TupleStream types)", functionName, expression, streamExpressions.size()));
    }

    stream = factory.constructStream(streamExpressions.get(0));
    orderBy = null == orderByExpression ? null : factory.constructComparator(((StreamExpressionValue)orderByExpression.getParameter()).getValue(), FieldComparator.class);
    
    evaluators = new ArrayList<>();
    for(StreamExpressionParameter evaluateAsExpression : evaluateAsExpressions){
      String fullString = ((StreamExpressionValue)evaluateAsExpression).getValue().trim();
      String originalFullString = fullString; // used for error messages
      
      // remove possible wrapping quotes
      if(fullString.length() > 2 && fullString.startsWith("\"") && fullString.endsWith("\"")){
        fullString = fullString.substring(1, fullString.length() - 1).trim();
      }
      
      String evaluatorPart = null;
      String asNamePart = null;
      
      if(fullString.toLowerCase(Locale.ROOT).contains(" as ")){
        String[] parts = fullString.split("(?i) as "); // ensure we are splitting in a case-insensitive way
        if(2 != parts.length){
          throw new IOException(String.format(Locale.ROOT,"Invalid %s expression %s - expecting evaluator of form 'fieldA' or 'fieldA as alias' but found %s", functionName, expression, originalFullString));
        }
        
        evaluatorPart = parts[0].trim();
        asNamePart = parts[1].trim();        
      }
      else{
        evaluatorPart = fullString;
        // no rename
      }
      
      boolean wasHandledAsEvaluatorFunction = false;
      StreamEvaluator evaluator = null;
      if(evaluatorPart.contains("(")){
        // is a possible evaluator
        try{
          StreamExpression asValueExpression = StreamExpressionParser.parse(evaluatorPart);
          if(factory.doesRepresentTypes(asValueExpression, StreamEvaluator.class)){
            evaluator = factory.constructEvaluator(asValueExpression);
            wasHandledAsEvaluatorFunction = true;
          }
        }
        catch(Throwable e){
          // it was not handled, so treat as a non-evaluator
        }
      }
      if(!wasHandledAsEvaluatorFunction){
        // treat as a straight field evaluator
        evaluator = new FieldValueEvaluator(evaluatorPart);
        if(null == asNamePart){
          asNamePart = evaluatorPart; // just use the field name
        }
      }

      if(null == evaluator || null == asNamePart){
        throw new IOException(String.format(Locale.ROOT,"Invalid %s expression %s - failed to parse evaluator '%s'", functionName, expression, originalFullString));
      }
      
      evaluators.add(new NamedEvaluator(asNamePart, evaluator));
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
      // we know stream is expressible
      expression.addParameter(((Expressible)stream).toExpression(factory));
    }
    else{
      expression.addParameter("<stream>");
    }
        
    // selected evaluators
    for(NamedEvaluator evaluator : evaluators) {
      expression.addParameter(String.format(Locale.ROOT, "%s as %s", evaluator.getEvaluator().toExpression(factory), evaluator.getName()));
    }

    if(orderBy != null) {
      expression.addParameter(new StreamExpressionNamedParameter("productSort", orderBy.toExpression(factory)));
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
    
    for(NamedEvaluator evaluator : evaluators){
      explanation.addHelper(evaluator.getEvaluator().toExplanation(factory));
    }

    if(orderBy != null) {
      explanation.addHelper(orderBy.toExplanation(factory));
    }
    
    return explanation;
  }

  public Tuple read() throws IOException {
    if(generatedTuples.isEmpty()){
      Tuple tuple = stream.read();
      
      if(tuple.EOF){
        return tuple;
      }
    
      // returns tuples in desired sorted order
      generatedTuples = generateTupleList(tuple);
    }
    
    return generatedTuples.pop();
  }
  
  @SuppressWarnings({"unchecked"})
  private LinkedList<Tuple> generateTupleList(Tuple original) throws IOException{
    Map<String, Object> evaluatedValues = new HashMap<>();
    
    for(NamedEvaluator evaluator : evaluators){
      evaluatedValues.put(evaluator.getName(), evaluator.getEvaluator().evaluate(original));
    }
    
    // use an array list internally because it has better sort performance
    // in Java 8. We do pay a conversion to a linked list but ..... oh well
    ArrayList<Tuple> generatedTupleList = new ArrayList<>();
    
    int[] workingIndexes = new int[evaluators.size()]; // java language spec ensures all values are 0
    do{
      Tuple generated = original.clone();
      for(int offset = 0; offset < workingIndexes.length; ++offset){
        String fieldName = evaluators.get(offset).getName();
        Object evaluatedValue = evaluatedValues.get(fieldName);
        if(evaluatedValue instanceof Collection){
          // because of the way a FieldEvaluator works we know that 
          // any collection is a list.
          generated.put(fieldName, ((List<Object>)evaluatedValue).get(workingIndexes[offset]));
        }
      }
      generatedTupleList.add(generated);
    }while(iterate(evaluators, workingIndexes, evaluatedValues));
    
    // order if we need to
    if(null != orderBy){
      generatedTupleList.sort(orderBy);
    }
    
    return new LinkedList<>(generatedTupleList);
  }
  
  private boolean iterate(List<NamedEvaluator> evaluators, int[] indexes, Map<String, Object> evaluatedValues){
    // this assumes evaluators and indexes are the same length, which is ok cause we created it so we know it is
    // go right to left and increment, returning true if we're not at the end
    for(int offset = indexes.length - 1; offset >= 0; --offset){
      Object evaluatedValue = evaluatedValues.get(evaluators.get(offset).getName());
      if(evaluatedValue instanceof Collection){
        int currentIndexValue = indexes[offset];
        if(currentIndexValue < ((Collection)evaluatedValue).size() - 1){
          indexes[offset] = currentIndexValue + 1;
          return true;
        }
        else if(0 != offset){
          indexes[offset] = 0;
          // move to the left
        }
      }
    }
    
    // no more
    return false;
  }
  
  /** Return the incoming sort + the sort applied to the generated tuples */
  public StreamComparator getStreamSort(){
    if(null != orderBy){
      return stream.getStreamSort().append(orderBy);
    }
    return stream.getStreamSort();
  }
  
  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
    for(NamedEvaluator evaluator : evaluators) {
      evaluator.getEvaluator().setStreamContext(context);
    }
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<>();
    l.add(stream);
    return l;
  }

  public void open() throws IOException {
    stream.open();
    generatedTuples = new LinkedList<>();
  }

  public void close() throws IOException {
    stream.close();
    generatedTuples.clear();
  }

  public int getCost() {
    return 0;
  }
  
  class NamedEvaluator{
    private String name;
    private StreamEvaluator evaluator;
    
    public NamedEvaluator(String name, StreamEvaluator evaluator){
      this.name = name;
      this.evaluator = evaluator;
    }
    
    public String getName(){
      return name;
    }
    
    public StreamEvaluator getEvaluator(){
      return evaluator;
    }
  }
}
