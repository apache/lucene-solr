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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.ExpressibleComparator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;


/**
 * The UniqueStream emits a unique stream of Tuples based on a Comparator.
 *
 * Note: The sort order of the underlying stream must match the Comparator.
 **/

public class UniqueStream extends TupleStream implements ExpressibleStream {

  private static final long serialVersionUID = 1;

  private TupleStream tupleStream;
  private Comparator<Tuple> comp;
  private transient Tuple currentTuple;

  public UniqueStream(TupleStream tupleStream, Comparator<Tuple> comp) {
    this.tupleStream = tupleStream;
    this.comp = comp;
  }
  
  public UniqueStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, ExpressibleStream.class, TupleStream.class);
    StreamExpressionNamedParameter overExpression = factory.getNamedOperand(expression, "over");
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }
    this.tupleStream = factory.constructStream(streamExpressions.get(0));
    
    if(null == overExpression || !(overExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'over' parameter listing fields to unique over but didn't find one",expression));
    }
    
    // Uniqueness is always done over equality, so always use an EqualTo comparator
    this.comp = factory.constructComparator(((StreamExpressionValue)overExpression.getParameter()).getValue(), FieldComparator.class);
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {    
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // streams
    if(tupleStream instanceof ExpressibleStream){
      expression.addParameter(((ExpressibleStream)tupleStream).toExpression(factory));
    }
    else{
      throw new IOException("This UniqueStream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }
    
    // over
    if(comp instanceof ExpressibleComparator){
      expression.addParameter(new StreamExpressionNamedParameter("over",((ExpressibleComparator)comp).toExpression(factory)));
    }
    else{
      throw new IOException("This UniqueStream contains a non-expressible comparator - it cannot be converted to an expression");
    }
    
    return expression;   
  }
  
  public void setComp(Comparator<Tuple> comp) {
    this.comp = comp;
  }
  
  public void setStreamContext(StreamContext context) {
    this.tupleStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {
    tupleStream.open();
  }

  public void close() throws IOException {
    tupleStream.close();
  }

  public Tuple read() throws IOException {
    Tuple tuple = tupleStream.read();
    if(tuple.EOF) {
      return tuple;
    }

    if(currentTuple == null) {
      currentTuple = tuple;
      return tuple;
    } else {
      while(true) {
        int i = comp.compare(currentTuple, tuple);
        if(i == 0) {
          //We have duplicate tuple so read the next tuple from the stream.
          tuple = tupleStream.read();
          if(tuple.EOF) {
            return tuple;
          }
        } else {
          //We have a non duplicate
          this.currentTuple = tuple;
          return tuple;
        }
      }
    }
  }

  public int getCost() {
    return 0;
  }

}