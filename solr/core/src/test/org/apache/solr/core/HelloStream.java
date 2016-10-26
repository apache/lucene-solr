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

package org.apache.solr.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class HelloStream extends TupleStream implements Expressible{

  boolean isSentHelloWorld = false;

  public HelloStream() {
  }

  public HelloStream(StreamExpression expression, StreamFactory factory) throws IOException{

  }

  @Override
  public void setStreamContext(StreamContext context) {

  }

  @Override
  public List<TupleStream> children() {
    return null;
  }

  @Override
  public void open() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public Tuple read() throws IOException {
    if (isSentHelloWorld) {
      Map m = new HashMap();
      m.put("EOF", true);
      return new Tuple(m);
    } else {
      isSentHelloWorld = true;
      Map m = new HashMap<>();
      m.put("msg", "Hello World!");
      return new Tuple(m);
    }
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName("hello")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_SOURCE)
        .withExpression("--non-expressible--");
  }
}