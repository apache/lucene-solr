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
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
* Merges two or more streams together ordering the Tuples based on a Comparator.
* All streams must be sorted by the fields being compared - this will be validated on construction.
* @since 5.1.0
**/
public class MergeStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private PushBackStream[] streams;
  private StreamComparator comp;

  public MergeStream(TupleStream streamA, TupleStream streamB, StreamComparator comp) throws IOException {
    init(comp, streamA, streamB);
  }
  
  public MergeStream(StreamComparator comp, TupleStream ... streams) throws IOException {
    init(comp, streams);
  }
  
  public MergeStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter onExpression = factory.getNamedOperand(expression, "on");
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(streamExpressions.size() < 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least two streams but found %d (must be PushBackStream types)",expression, streamExpressions.size()));
    }

    if(null == onExpression || !(onExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'on' parameter listing fields to merge on but didn't find one",expression));
    }
    
    TupleStream[] streams = new TupleStream[streamExpressions.size()];
    for(int idx = 0; idx < streamExpressions.size(); ++idx){
      streams[idx] = factory.constructStream(streamExpressions.get(idx));
    }
    
    init( factory.constructComparator(((StreamExpressionValue)onExpression.getParameter()).getValue(), FieldComparator.class),
          streams
        );
  }
  
  private void init(StreamComparator comp, TupleStream ... streams) throws IOException {
    
    // All streams must both be sorted so that comp can be derived from
    for(TupleStream stream : streams){
      if(!comp.isDerivedFrom(stream.getStreamSort())){
        throw new IOException("Invalid MergeStream - all substream comparators (sort) must be a superset of this stream's comparator.");
      }
    }
    
    // Convert to PushBack streams so we can push back tuples
    this.streams = new PushBackStream[streams.length];
    for(int idx = 0; idx < streams.length; ++idx){
      this.streams[idx] = new PushBackStream(streams[idx]);
    }
    this.comp = comp;
  }
  
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // streams
    for(PushBackStream stream : streams){
      if(includeStreams){
        expression.addParameter(stream.toExpression(factory));
      }
      else{
        expression.addParameter("<stream>");
      }
    }
    
    // on
    expression.addParameter(new StreamExpressionNamedParameter("on",comp.toExpression(factory)));
    
    return expression;   
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_DECORATOR);
    explanation.setExpression(toExpression(factory, false).toString());
    explanation.addHelper(comp.toExplanation(factory));
    
    for(PushBackStream stream : streams){
      explanation.addChild(stream.toExplanation(factory));
    }
    
    return explanation;    
  }

  public void setStreamContext(StreamContext context) {
    for(PushBackStream stream : streams){
      stream.setStreamContext(context);
    }
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    for(PushBackStream stream : streams){
      l.add(stream);
    }
    return l;
  }

  public void open() throws IOException {
    for(PushBackStream stream : streams){
      stream.open();
    }
  }

  public void close() throws IOException {
    for(PushBackStream stream : streams){
      stream.close();
    }
  }

  public Tuple read() throws IOException {
    
    // might be able to optimize this by sorting the streams based on the next to read tuple from each.
    // if we can ensure the sort of the streams and update it in less than linear time then there would
    // be some performance gain. But, assuming the # of streams is kinda small then this might not be
    // worth it
    
    Tuple minimum = null;
    PushBackStream minimumStream = null;
    for(PushBackStream stream : streams){
      Tuple current = stream.read();
      
      if(current.EOF){
        stream.pushBack(current);
        continue;
      }
      
      if(null == minimum){
        minimum = current;
        minimumStream = stream;
        continue;
      }
      
      if(comp.compare(current, minimum) < 0){
        // Push back on its stream
        minimumStream.pushBack(minimum);
        
        minimum = current;
        minimumStream = stream;
        continue;
      }
      else{
        stream.pushBack(current);
      }
    }
    
    // If all EOF then min will be null, else min is the current minimum
    if(null == minimum){
      // return EOF, doesn't matter which cause we're done
      return streams[0].read();
    }
    
    return minimum;
    
//    Tuple a = streamA.read();
//    Tuple b = streamB.read();
//
//    if(a.EOF && b.EOF) {
//      return a;
//    }
//
//    if(a.EOF) {
//      streamA.pushBack(a);
//      return b;
//    }
//
//    if(b.EOF) {
//      streamB.pushBack(b);
//      return a;
//    }
//
//    int c = comp.compare(a,b);
//
//    if(c < 0) {
//      streamB.pushBack(b);
//      return a;
//    } else {
//      streamA.pushBack(a);
//      return b;
//    }
  }
  
  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return comp;
  }


  public int getCost() {
    return 0;
  }
}
