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
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;


/**
*  Iterates over a TupleStream and Ranks the topN tuples based on a Comparator.
**/

public class RankStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream tupleStream;
  private Comparator<Tuple> comp;
  private int size;
  private transient PriorityQueue<Tuple> top;
  private transient boolean finished = false;
  private transient LinkedList<Tuple> topList;

  public RankStream(TupleStream tupleStream, int size, Comparator<Tuple> comp) {
    init(tupleStream,size,comp);
  }
  
  public RankStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter nParam = factory.getNamedOperand(expression, "n");
    StreamExpressionNamedParameter sortExpression = factory.getNamedOperand(expression, "sort");
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(null == nParam || null == nParam.getParameter() || !(nParam.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single 'n' parameter of type positive integer but didn't find one",expression));
    }
    String nStr = ((StreamExpressionValue)nParam.getParameter()).getValue();
    int nInt = 0;
    try{
      nInt = Integer.parseInt(nStr);
      if(nInt <= 0){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - topN '%s' must be greater than 0.",expression, nStr));
      }
    }
    catch(NumberFormatException e){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - topN '%s' is not a valid integer.",expression, nStr));
    }    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }    
    if(null == sortExpression || !(sortExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'over' parameter listing fields to unique over but didn't find one",expression));
    }
    
    TupleStream stream = factory.constructStream(streamExpressions.get(0));
    Comparator<Tuple> comp = factory.constructComparator(((StreamExpressionValue)sortExpression.getParameter()).getValue(), StreamComparator.class);
    
    init(stream,nInt,comp);    
  }
  
  private void init(TupleStream tupleStream, int size, Comparator<Tuple> comp){
    this.tupleStream = tupleStream;
    this.comp = comp;
    this.size = size;
  }
  
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {    
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // n
    expression.addParameter(new StreamExpressionNamedParameter("n", Integer.toString(size)));
    
    // stream
    if(tupleStream instanceof Expressible){
      expression.addParameter(((Expressible)tupleStream).toExpression(factory));
    }
    else{
      throw new IOException("This RankStream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }
        
    // sort
    if(comp instanceof Expressible){
      expression.addParameter(new StreamExpressionNamedParameter("sort",((Expressible)comp).toExpression(factory)));
    }
    else{
      throw new IOException("This RankStream contains a non-expressible comparator - it cannot be converted to an expression");
    }
    
    return expression;   
  }
  
  public void setStreamContext(StreamContext context) {
    this.tupleStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {
    this.top = new PriorityQueue(size, new ReverseComp(comp));
    this.topList = new LinkedList();
    tupleStream.open();
  }

  public void close() throws IOException {
    tupleStream.close();
  }
  
  public Comparator<Tuple> getComparator(){
    return this.comp;
  }

  public Tuple read() throws IOException {
    if(!finished) {
      while(true) {
        Tuple tuple = tupleStream.read();
        if(tuple.EOF) {
          finished = true;
          int s = top.size();
          for(int i=0; i<s; i++) {
            Tuple t = top.poll();
            topList.addFirst(t);
          }
          topList.addLast(tuple);
          break;
        } else {
          Tuple peek = top.peek();
          if(top.size() >= size) {
            if(comp.compare(tuple, peek) < 0) {
              top.poll();
              top.add(tuple);
            }
          } else {
            top.add(tuple);
          }
        }
      }
    }

    return topList.pollFirst();
  }

  public int getCost() {
    return 0;
  }

  class ReverseComp implements Comparator<Tuple>, Serializable {

    private Comparator<Tuple> comp;

    public ReverseComp(Comparator<Tuple> comp) {
      this.comp = comp;
    }

    public int compare(Tuple t1, Tuple t2) {
      return comp.compare(t1, t2)*(-1);
    }
  }
}