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
public class ListStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private TupleStream[] streams;
  private TupleStream currentStream;
  private int streamIndex;

  public ListStream(TupleStream... streams) throws IOException {
    init(streams);
  }

  public ListStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    TupleStream[] streams = new TupleStream[streamExpressions.size()];
    for(int idx = 0; idx < streamExpressions.size(); ++idx){
      streams[idx] = factory.constructStream(streamExpressions.get(idx));
    }

    init(streams);
  }

  private void init(TupleStream ... tupleStreams) {
    this.streams = tupleStreams;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    if(includeStreams) {
      for(TupleStream stream : streams) {
        expression.addParameter(((Expressible)stream).toExpression(factory));
      }
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
    for(TupleStream stream : streams) {
      explanation.addChild(stream.toExplanation(factory));
    }

    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    for(TupleStream stream : streams) {
      stream.setStreamContext(context);
    }
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    for(TupleStream stream : streams) {
      l.add(stream);
    }
    return l;
  }

  public Tuple read() throws IOException {
    while(true) {
      if (currentStream == null) {
        if (streamIndex < streams.length) {
          currentStream = streams[streamIndex];
          // Set the stream to null in the array of streams once its been set to the current stream.
          // This will remove the reference to the stream
          // and should allow it to be garbage collected once it's no longer the current stream.
          streams[streamIndex] = null;
          currentStream.open();
        } else {
          return Tuple.EOF();
        }
      }

      Tuple tuple = currentStream.read();
      if (tuple.EOF) {
        currentStream.close();
        currentStream = null;
        ++streamIndex;
      } else {
        return tuple;
      }
    }
  }

  public void close() throws IOException {
  }

  public void open() throws IOException {


  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  public int getCost() {
    return 0;
  }


}
