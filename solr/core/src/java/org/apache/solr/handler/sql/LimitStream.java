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
package org.apache.solr.handler.sql;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class LimitStream extends TupleStream {

  private final TupleStream stream;
  private final int limit;
  private int count;

  LimitStream(TupleStream stream, int limit) {
    this.stream = stream;
    this.limit = limit;
  }

  public void open() throws IOException {
    this.stream.open();
  }

  public void close() throws IOException {
    this.stream.close();
  }

  public List<TupleStream> children() {
    List<TupleStream> children = new ArrayList<>();
    children.add(stream);
    return children;
  }

  public StreamComparator getStreamSort(){
    return stream.getStreamSort();
  }

  public void setStreamContext(StreamContext context) {
    stream.setStreamContext(context);
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
        .withChildren(new Explanation[]{
            stream.toExplanation(factory)
        })
        .withFunctionName("SQL LIMIT")
        .withExpression("--non-expressible--")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_DECORATOR);
  }

  public Tuple read() throws IOException {
    ++count;
    if(count > limit) {
      Map<String, String> fields = new HashMap<>();
      fields.put("EOF", "true");
      return new Tuple(fields);
    }

    return stream.read();
  }
}