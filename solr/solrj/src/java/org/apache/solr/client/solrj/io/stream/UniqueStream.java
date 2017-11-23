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
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.ops.DistinctOperation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;


/**
 * The UniqueStream emits a unique stream of Tuples based on a Comparator.
 *
 * Note: The sort order of the underlying stream must match the Comparator.
 * @since 5.1.0
 **/

public class UniqueStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream originalStream;
  private StreamEqualitor originalEqualitor;
  
  private ReducerStream reducerStream;

  public UniqueStream(TupleStream stream, StreamEqualitor eq) throws IOException {
    init(stream,eq);
  }
  
  public UniqueStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter overExpression = factory.getNamedOperand(expression, "over");
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }
    
    if(null == overExpression || !(overExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'over' parameter listing fields to unique over but didn't find one",expression));
    }
    
    init(factory.constructStream(streamExpressions.get(0)), factory.constructEqualitor(((StreamExpressionValue)overExpression.getParameter()).getValue(), FieldEqualitor.class));
  }
  
  private void init(TupleStream stream, StreamEqualitor eq) throws IOException{
    this.originalStream = stream;
    this.originalEqualitor = eq;
    
    this.reducerStream = new ReducerStream(stream, eq, new DistinctOperation());

    if(!eq.isDerivedFrom(stream.getStreamSort())){
      throw new IOException("Invalid UniqueStream - substream comparator (sort) must be a superset of this stream's equalitor.");
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
      // streams
      if(originalStream instanceof Expressible){
        expression.addParameter(((Expressible)originalStream).toExpression(factory));
      }
      else{
        throw new IOException("This UniqueStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }
    
    // over
    if(originalEqualitor instanceof Expressible){
      expression.addParameter(new StreamExpressionNamedParameter("over",((Expressible)originalEqualitor).toExpression(factory)));
    }
    else{
      throw new IOException("This UniqueStream contains a non-expressible equalitor - it cannot be converted to an expression");
    }
    
    return expression;   
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[] {
          originalStream.toExplanation(factory)
          // we're not including that this is wrapped with a ReducerStream stream because that's just an implementation detail
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory, false).toString())
      .withHelper(originalEqualitor.toExplanation(factory));
  }
    
  public void setStreamContext(StreamContext context) {
    this.originalStream.setStreamContext(context);
    this.reducerStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(originalStream);
    return l;
  }

  public void open() throws IOException {
    reducerStream.open();
      // opens originalStream as well
  }

  public void close() throws IOException {
    reducerStream.close();
      // closes originalStream as well
  }

  public Tuple read() throws IOException {
    return reducerStream.read();
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return reducerStream.getStreamSort();
  }
  
  public int getCost() {
    return 0;
  }

}
