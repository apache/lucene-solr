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
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * @since 6.6.0
 */
public class CellStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private TupleStream stream;
  private String name;
  private Tuple tuple;
  private Tuple EOFTuple;

  public CellStream(String name, TupleStream stream) throws IOException {
    init(name, stream);
  }

  public CellStream(StreamExpression expression, StreamFactory factory) throws IOException {
    String name = factory.getValueOperand(expression, 0);
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    if(streamExpressions.size() != 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting 1 stream but found %d",expression, streamExpressions.size()));
    }

    TupleStream tupleStream = factory.constructStream(streamExpressions.get(0));
    init(name, tupleStream);
  }

  public String getName() {
    return this.name;
  }

  private void init(String name, TupleStream tupleStream) {
    this.name = name;
    this.stream = tupleStream;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(name);
    if(includeStreams) {
      expression.addParameter(((Expressible)stream).toExpression(factory));
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
    explanation.addChild(stream.toExplanation(factory));

    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(stream);

    return l;
  }

  public Tuple read() throws IOException {
    if(tuple.EOF) {
      return tuple;
    } else {
      Tuple t = tuple;
      tuple = EOFTuple;
      return t;
    }
  }

  public void close() throws IOException {
  }

  public void open() throws IOException {
    try {
      stream.open();
      List<Tuple> list = new ArrayList<>();
      while(true) {
        Tuple tuple = stream.read();
        if(tuple.EOF) {
          EOFTuple = tuple;
          break;
        } else {
          list.add(tuple);
        }
      }

      tuple = new Tuple();
      tuple.put(name, list);
    } finally {
      stream.close();
    }
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  public int getCost() {
    return 0;
  }


}
