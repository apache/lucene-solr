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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;

public class RecordCountStream extends TupleStream implements Expressible, Serializable {

  private TupleStream stream;
  private int count;

  public RecordCountStream(TupleStream stream) {
    this.stream = stream;
  }
  
  public RecordCountStream(StreamExpression expression, StreamFactory factory) throws IOException{
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
        
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }
    
    stream = factory.constructStream(streamExpressions.get(0));
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
        throw new IOException("This RecordCountStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }
    
    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[]{
        stream.toExplanation(factory)
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory, false).toString())
      ;    
  }

  public void close() throws IOException {
    this.stream.close();
  }

  public void open() throws IOException {
    this.stream.open();
  }

  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList<>();
    l.add(stream);
    return l;
  }

  public void setStreamContext(StreamContext streamContext) {
    stream.setStreamContext(streamContext);
  }

  public Tuple read() throws IOException {
    Tuple t = stream.read();
    if(t.EOF) {
      t.put("count", count);
      return t;
    } else {
      ++count;
      return t;
    }
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }
}