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
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.ops.DistinctOperation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
* Emits tuples from streamA which do not exist in streamB. Resulting tuples are ordered
* the same as they were in streamA. Both streams must be sorted by the fields being compared.
**/

public class ComplementStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private PushBackStream streamA;
  private PushBackStream streamB;
  private TupleStream originalStreamB;
  private StreamEqualitor eq;

  public ComplementStream(TupleStream streamA, TupleStream streamB, StreamEqualitor eq) throws IOException {
    init(streamA, streamB, eq);
  }
  
  public ComplementStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter onExpression = factory.getNamedOperand(expression, "on");
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(2 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two streams but found %d (must be TupleStream types)",expression, streamExpressions.size()));
    }

    if(null == onExpression || !(onExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'on' parameter listing fields to merge on but didn't find one",expression));
    }
    
    init( factory.constructStream(streamExpressions.get(0)),
          factory.constructStream(streamExpressions.get(1)),
          factory.constructEqualitor(((StreamExpressionValue)onExpression.getParameter()).getValue(), FieldEqualitor.class)
        );
  }
  
  private void init(TupleStream streamA, TupleStream streamB, StreamEqualitor eq) throws IOException {
    this.streamA = new PushBackStream(streamA);
    this.streamB = new PushBackStream(new UniqueStream(streamB, eq));
    this.originalStreamB = streamB; // hold onto this for toExpression
    this.eq = eq;

    // streamA and streamB must both be sorted so that comp can be derived from
    if(!eq.isDerivedFrom(streamA.getStreamSort()) || !eq.isDerivedFrom(streamB.getStreamSort())){
      throw new IOException("Invalid ComplementStream - both substream comparators (sort) must be a superset of this stream's equalitor.");
    }
  }
  
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {    
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // streams
    if(streamA instanceof Expressible){
      expression.addParameter(((Expressible)streamA).toExpression(factory));
    }
    else{
      throw new IOException("This IntersectionStream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }
    
    if(originalStreamB instanceof Expressible){
      expression.addParameter(((Expressible)originalStreamB).toExpression(factory));
    }
    else{
      throw new IOException("This IntersectStream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }
    
    // on
    expression.addParameter(new StreamExpressionNamedParameter("on",eq.toExpression(factory)));
    
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
    
    while(true){
      Tuple a = streamA.read();
      Tuple b = streamB.read();
      
      // if a is EOF then we're done
      if(a.EOF){ return a; }
      
      // if b is EOF then return a
      if(b.EOF){
        streamB.pushBack(b);
        return a; 
      }
      
      // if a != b && a < b then we know there is no b which a might equal so return a
      if(!eq.test(a, b) && streamA.getStreamSort().compare(a, b) < 0){
        streamB.pushBack(b);
        return a;
      }
      
      // if a == b then ignore a cause it exists in b
      // else we know that b < a so we can ignore b
      if(eq.test(a, b)){
        streamB.pushBack(b);
      }
      else{
        streamA.pushBack(a);
      }
    }
  }
  
  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return streamA.getStreamSort();
  }


  public int getCost() {
    return 0;
  }
}
