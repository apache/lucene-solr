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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
public class GetStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private StreamContext streamContext;
  private String name;
  private Iterator<Tuple> tupleIterator;

  public GetStream(String name) throws IOException {
    init(name);
  }

  public GetStream(StreamExpression expression, StreamFactory factory) throws IOException {
    String name = factory.getValueOperand(expression, 0);
    init(name);
  }

  private void init(String name) {
    this.name = name;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(name);
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    explanation.setExpression(toExpression(factory, false).toString());
    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<>();
    return l;
  }

  public Tuple read() throws IOException {
    if (tupleIterator.hasNext()) {
      Tuple t = tupleIterator.next();
      return t.clone();
    } else {
      return Tuple.EOF();
    }
  }

  public void close() throws IOException {
  }

  @SuppressWarnings({"unchecked"})
  public void open() throws IOException {
    Map<String, Object> lets = streamContext.getLets();
    Object o = lets.get(name);
    @SuppressWarnings({"rawtypes"})
    List l = null;
    if(o instanceof List) {
      l = (List)o;
      tupleIterator = l.iterator();
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
