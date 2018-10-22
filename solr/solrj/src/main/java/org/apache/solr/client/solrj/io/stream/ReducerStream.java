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
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.ops.ReduceOperation;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 *  Iterates over a TupleStream and buffers Tuples that are equal based on a comparator.
 *  This allows tuples to be grouped by common field(s).
 *
 *  The read() method emits one tuple per group. The fields of the emitted Tuple reflect the first tuple
 *  encountered in the group.
 *
 *  Use the Tuple.getMaps() method to return all the Tuples in the group. This method returns
 *  a list of maps (including the group head), which hold the data for each Tuple in the group.
 *
 *  Note: The ReducerStream requires that it's underlying stream be sorted and partitioned by the same
 *  fields as it's comparator.
 *
 * @since 5.1.0
 **/

public class ReducerStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private PushBackStream stream;
  private StreamEqualitor eq;
  private ReduceOperation op;
  private boolean needsReduce;

  private transient Tuple currentGroupHead;
  
  public ReducerStream(TupleStream stream, StreamEqualitor eq, ReduceOperation op) throws IOException {
    init(stream, eq, op);
  }

  public ReducerStream(TupleStream stream, StreamComparator comp, ReduceOperation op) throws IOException {
    init(stream, convertToEqualitor(comp), op);
  }
  
  private StreamEqualitor convertToEqualitor(StreamComparator comp){
    if(comp instanceof MultipleFieldComparator){
      MultipleFieldComparator mComp = (MultipleFieldComparator)comp;
      StreamEqualitor[] eqs = new StreamEqualitor[mComp.getComps().length];
      for(int idx = 0; idx < mComp.getComps().length; ++idx){
        eqs[idx] = convertToEqualitor(mComp.getComps()[idx]);
      }
      return new MultipleFieldEqualitor(eqs);
    }
    else{
      FieldComparator fComp = (FieldComparator)comp;
      return new FieldEqualitor(fComp.getLeftFieldName(), fComp.getRightFieldName());
    }
  }

  public ReducerStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter byExpression = factory.getNamedOperand(expression, "by");
    List<StreamExpression> operationExpressions = factory.getExpressionOperandsRepresentingTypes(expression, ReduceOperation.class);

    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }    
    if(null == byExpression || !(byExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'by' parameter listing fields to group by but didn't find one",expression));
    }

    ReduceOperation reduceOperation = null;
    if(operationExpressions != null && operationExpressions.size() == 1) {
      StreamExpression ex = operationExpressions.get(0);
      StreamOperation operation = factory.constructOperation(ex);
      if(operation instanceof ReduceOperation) {
        reduceOperation = (ReduceOperation) operation;
      } else {
        throw new IOException("The ReducerStream requires a ReduceOperation. A StreamOperation was provided.");
      }
    } else {
      throw new IOException("The ReducerStream requires a ReduceOperation.");
    }

    init(factory.constructStream(streamExpressions.get(0)),
         factory.constructEqualitor(((StreamExpressionValue) byExpression.getParameter()).getValue(), FieldEqualitor.class),
         reduceOperation);
  }
  
  private void init(TupleStream stream, StreamEqualitor eq, ReduceOperation op) throws IOException{
    this.stream = new PushBackStream(stream);
    this.eq = eq;
    this.op = op;
    
    if(!eq.isDerivedFrom(stream.getStreamSort())){
      throw new IOException("Invalid ReducerStream - substream comparator (sort) must be a superset of this stream's comparator.");
    }
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // stream
    if(includeStreams){
      expression.addParameter(stream.toExpression(factory));
    }
    else{
      expression.addParameter("<stream>");
    }
    
    // over
    if(eq instanceof Expressible){
      expression.addParameter(new StreamExpressionNamedParameter("by",((Expressible)eq).toExpression(factory)));
    }
    else{
      throw new IOException("This ReducerStream contains a non-expressible comparator - it cannot be converted to an expression");
    }

    if(op instanceof Expressible) {
      expression.addParameter(op.toExpression(factory));
    } else {
      throw new IOException("This ReducerStream contains a non-expressible operation - it cannot be converted to an expression");
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
      .withHelpers(new Explanation[]{
          eq.toExplanation(factory),
          op.toExplanation(factory)
      });
  }
  
  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
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

    while(true) {
      Tuple t = stream.read();

      if(t.EOF) {
       if(needsReduce) {
         stream.pushBack(t);
         needsReduce = false;
         return op.reduce();
       } else {
         return t;
       }
      }

      if(currentGroupHead == null) {
        currentGroupHead = t;
        op.operate(t);
        needsReduce = true;
      } else {
        if(eq.test(currentGroupHead, t)) {
          op.operate(t);
          needsReduce = true;
        } else {
          stream.pushBack(t);
          currentGroupHead = null;
          needsReduce = false;
          return op.reduce();
        }
      }
    }
  }
  
  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return stream.getStreamSort();
  }

  public int getCost() {
    return 0;
  }
}
