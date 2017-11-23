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
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/** 
 * Defines a JoinStream which can hold N streams, all joined with the same equalitor 
 * @since 6.0.0
 */
public abstract class JoinStream extends TupleStream implements Expressible {
  
  private static final long serialVersionUID = 1;
  private List<PushBackStream> streams;
  protected StreamEqualitor eq;
  
  public JoinStream(StreamEqualitor eq, TupleStream first, TupleStream second, TupleStream... others) {
    this.streams = new ArrayList<PushBackStream>();
    
    this.eq = eq;
    
    this.streams.add(new PushBackStream(first));
    this.streams.add(new PushBackStream(second));
    
    for (TupleStream other : others) {
      this.streams.add(new PushBackStream(other));
    }
  }
  
  protected abstract void validateTupleOrder() throws IOException;
  
  public JoinStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression,
        Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter onExpression = factory.getNamedOperand(expression, "on");
    
    // validate expression contains only what we want.
    if (expression.getParameters().size() != streamExpressions.size() + 1) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));
    }
    
    if (streamExpressions.size() < 2) {
      throw new IOException(String.format(Locale.ROOT,
          "Invalid expression %s - expecting at least two streams but found %d (must be PushBackStream types)",
          expression, streamExpressions.size()));
    }
    
    this.streams = new ArrayList<PushBackStream>();
    for (StreamExpression streamExpression : streamExpressions) {
      this.streams.add(new PushBackStream(factory.constructStream(streamExpression)));
    }
    
    if (null == onExpression || !(onExpression.getParameter() instanceof StreamExpressionValue)) {
      throw new IOException(String.format(Locale.ROOT,
          "Invalid expression %s - expecting single 'on' parameter listing fields to join on but didn't find one",
          expression));
    }
    
    this.eq = factory.constructEqualitor(((StreamExpressionValue) onExpression.getParameter()).getValue(),
        FieldEqualitor.class);
  }
  
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // streams
    for (PushBackStream stream : streams) {
      if(includeStreams){
        expression.addParameter(stream.toExpression(factory));
      }
      else{
        expression.addParameter("<stream>");
      }
    }
    
    // on
    if (eq instanceof Expressible) {
      expression.addParameter(new StreamExpressionNamedParameter("on", ((Expressible) eq).toExpression(factory)));
    } else {
      throw new IOException(
          "This JoinStream contains a non-expressible equalitor - it cannot be converted to an expression");
    }
    
    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_DECORATOR);
    explanation.setExpression(toExpression(factory, false).toString());
    explanation.addHelper(eq.toExplanation(factory));
    
    for(TupleStream stream : streams){
      explanation.addChild(stream.toExplanation(factory));
    }
    
    return explanation;    
  }
  
  public void setStreamContext(StreamContext context) {
    for (PushBackStream stream : streams) {
      stream.setStreamContext(context);
    }
  }
  
  public void open() throws IOException {
    for (PushBackStream stream : streams) {
      stream.open();
    }
  }
  
  public void close() throws IOException {
    for (PushBackStream stream : streams) {
      stream.close();
    }
  }
  
  public List<TupleStream> children() {
    List<TupleStream> list = new ArrayList<TupleStream>();
    for (TupleStream stream : streams) {
      list.add(stream);
    }
    return list;
  }
  
  public PushBackStream getStream(int idx) {
    if (streams.size() > idx) {
      return streams.get(idx);
    }
    
    throw new IllegalArgumentException(String.format(Locale.ROOT,"Stream idx=%d doesn't exist. Number of streams is %d", idx,
        streams.size()));
  }
  
  protected boolean isValidTupleOrder() {
    // Validate that the equalitor is derivable from the comparator in each stream. If it is, then we know all stream
    // comparators are
    // derivable with each other stream
    for (TupleStream stream : streams) {
      if (!eq.isDerivedFrom(stream.getStreamSort())) {
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * Given the stream, start from beginning and load group with all tuples that are equal to the first in stream
   * (including the first one in the stream). All matched tuples are removed from the stream. Result is at least one
   * tuple will be read from the stream and 0 or more tuples will exist in the group. If the first tuple is EOF then the
   * group will have 0 items. Else it will have at least one item. The first group member is returned.
   * 
   * @param group
   *          - should be empty
   */
  protected Tuple loadEqualTupleGroup(PushBackStream stream, LinkedList<Tuple> group, StreamComparator groupComparator)
      throws IOException {
    // Find next set of same tuples from the stream
    Tuple firstMember = stream.read();
    
    if (!firstMember.EOF) {
      // first in group, implicitly a member
      group.add(firstMember);
      
      BREAKPOINT: while (true) {
        Tuple nMember = stream.read();
        if (!nMember.EOF && 0 == groupComparator.compare(firstMember, nMember)) {
          // they are in same group
          group.add(nMember);
        } else {
          stream.pushBack(nMember);
          break BREAKPOINT;
        }
      }
    }
    
    return firstMember;
  }
  
  public int getCost() {
    return 0;
  }
  
}
