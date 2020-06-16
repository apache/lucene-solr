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
import java.util.Date;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;


/**
 *  The NullStream Iterates over a TupleStream and eats the tuples. It returns the tuple count in the EOF tuple.
 *  Because the NullStreaam eats all the Tuples it see's it can be used as a simple tool for performance analysis of
 *  underlying streams.
 * @since 6.4.0
 **/

public class NullStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private long count;
  private long start;
  private Tuple eof;

  public NullStream(TupleStream tupleStream) throws IOException {
    init(tupleStream);
  }

  public NullStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    TupleStream stream = factory.constructStream(streamExpressions.get(0));

    init(stream);
  }

  private void init(TupleStream tupleStream) throws IOException{
    this.stream = tupleStream;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    if(includeStreams){
      // stream
      if(stream instanceof Expressible){
        expression.addParameter(((Expressible)stream).toExpression(factory));
      }
      else{
        throw new IOException("This RankStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
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

  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(stream);
    return l;
  }

  public void open() throws IOException {
    start = new Date().getTime();
    count = 0;
    stream.open();
  }

  public void close() throws IOException {
    stream.close();
  }

  public Tuple read() throws IOException {

    if(eof != null) {
      return eof;
    }

    while(true) {
      Tuple tuple  = stream.read();
      if(tuple.EOF) {
        eof = tuple;
        long end = new Date().getTime();
        Tuple t = new Tuple();
        t.put("nullCount", count);
        t.put("timer", end-start);
        return t;
      } else {
        ++count;
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
