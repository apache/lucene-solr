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
public class EvalStream extends TupleStream implements Expressible {

  private TupleStream stream;
  private TupleStream evalStream;

  private StreamFactory streamFactory;
  private StreamContext streamContext;

  public EvalStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    TupleStream stream = factory.constructStream(streamExpressions.get(0));
    init(stream, factory);
  }

  private void init(TupleStream tupleStream, StreamFactory factory) throws IOException{
    this.stream = tupleStream;
    this.streamFactory = factory;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {

    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // stream
    if(includeStreams) {
      if (stream instanceof Expressible) {
        expression.addParameter(((Expressible) stream).toExpression(factory));
      } else {
        throw new IOException("The EvalStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
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
        .withExpression(toExpression(factory, false).toString());
  }

  public void setStreamContext(StreamContext streamContext) {
    this.streamContext = streamContext;
    this.stream.setStreamContext(streamContext);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<>();
    l.add(stream);
    return l;
  }

  public void open() throws IOException {
    try {
      stream.open();
      Tuple tuple = stream.read();
      String expr = tuple.getString("expr_s");

      if(expr == null) {
        throw new IOException("expr_s cannot be empty for the EvalStream");
      }

      evalStream = streamFactory.constructStream(expr);
      evalStream.setStreamContext(streamContext);
      evalStream.open();
    } finally {
      stream.close();
    }
  }

  public void close() throws IOException {
    evalStream.close();
  }

  public Tuple read() throws IOException {
    return evalStream.read();
  }

  public StreamComparator getStreamSort(){
    return stream.getStreamSort();
  }

  public int getCost() {
    return 0;
  }
}
