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
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
* Unions streamA with streamB ordering the Tuples based on a Comparator.
* Both streams must be sorted by the fields being compared.
**/


public class MergeStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private PushBackStream streamA;
  private PushBackStream streamB;
  private StreamComparator comp;

  public MergeStream(TupleStream streamA, TupleStream streamB, StreamComparator comp) throws IOException {
    init(streamA, streamB, comp);
  }
  
  public MergeStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter onExpression = factory.getNamedOperand(expression, "on");
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(2 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two streams but found %d (must be PushBackStream types)",expression, streamExpressions.size()));
    }

    if(null == onExpression || !(onExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'on' parameter listing fields to merge on but didn't find one",expression));
    }
    
    init( factory.constructStream(streamExpressions.get(0)),
          factory.constructStream(streamExpressions.get(1)),
          factory.constructComparator(((StreamExpressionValue)onExpression.getParameter()).getValue(), FieldComparator.class)
        );
  }
  
  private void init(TupleStream streamA, TupleStream streamB, StreamComparator comp) throws IOException {
    this.streamA = new PushBackStream(streamA);
    this.streamB = new PushBackStream(streamB);
    this.comp = comp;

    // streamA and streamB must both be sorted so that comp can be derived from
    if(!comp.isDerivedFrom(streamA.getStreamSort()) || !comp.isDerivedFrom(streamB.getStreamSort())){
      throw new IOException("Invalid MergeStream - both substream comparators (sort) must be a superset of this stream's comparator.");
    }
  }
  
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {    
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // streams
    expression.addParameter(streamA.toExpression(factory));
    expression.addParameter(streamB.toExpression(factory));
    
    // on
    expression.addParameter(new StreamExpressionNamedParameter("on",comp.toExpression(factory)));
    
    return expression;   
  }

  public void setStreamContext(StreamContext context) {
    this.streamA.setStreamContext(context);
    this.streamB.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(streamA);
    l.add(streamB);
    return l;
  }

  public void open() throws IOException {
    streamA.open();
    streamB.open();
  }

  public void close() throws IOException {
    streamA.close();
    streamB.close();
  }

  public Tuple read() throws IOException {
    Tuple a = streamA.read();
    Tuple b = streamB.read();

    if(a.EOF && b.EOF) {
      return a;
    }

    if(a.EOF) {
      streamA.pushBack(a);
      return b;
    }

    if(b.EOF) {
      streamB.pushBack(b);
      return a;
    }

    int c = comp.compare(a,b);

    if(c < 0) {
      streamB.pushBack(b);
      return a;
    } else {
      streamA.pushBack(a);
      return b;
    }
  }
  
  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return comp;
  }


  public int getCost() {
    return 0;
  }
}