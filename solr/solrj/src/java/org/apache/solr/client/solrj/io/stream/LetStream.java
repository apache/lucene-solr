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
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class LetStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private TupleStream stream;
  private List<CellStream> cellStreams;
  private StreamContext streamContext;

  public LetStream(TupleStream stream, List<CellStream> cellStreams) throws IOException {
    init(stream, cellStreams);
  }

  public LetStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);

    if(streamExpressions.size() < 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting atleast 2 streams but found %d",expression, streamExpressions.size()));
    }

    TupleStream stream = null;
    List<CellStream> cellStreams = new ArrayList();

    for(StreamExpression streamExpression : streamExpressions) {
      TupleStream s = factory.constructStream(streamExpression);
      if(s instanceof CellStream) {
        cellStreams.add((CellStream)s);
      } else {
        if(stream == null) {
          stream = s;
        } else {
          throw new IOException("Found more then one stream that was not a CellStream");
        }
      }
    }

    init(stream, cellStreams);
  }

  private void init(TupleStream _stream, List<CellStream> _cellStreams) {
    this.stream = _stream;
    this.cellStreams = _cellStreams;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(((Expressible) stream).toExpression(factory));
    for(CellStream cellStream : cellStreams) {
      expression.addParameter(((Expressible)cellStream).toExpression(factory));
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
    this.streamContext = context;
    this.stream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(stream);

    return l;
  }

  public Tuple read() throws IOException {
    return stream.read();
  }

  public void close() throws IOException {
    stream.close();
  }

  public void open() throws IOException {
    Map<String, List<Tuple>> lets = streamContext.getLets();
    for(CellStream cellStream : cellStreams) {
      try {
        cellStream.setStreamContext(streamContext);
        cellStream.open();
        Tuple tup = cellStream.read();
        String name = cellStream.getName();
        List<Tuple> tuples = (List<Tuple>)tup.get(name);
        lets.put(name, tuples);
      } finally {
        cellStream.close();
      }
    }
    stream.open();
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  public int getCost() {
    return 0;
  }


}