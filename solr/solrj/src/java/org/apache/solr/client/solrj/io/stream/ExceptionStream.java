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
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 6.0.0
 */
public class ExceptionStream extends TupleStream {

  private TupleStream stream;
  private Exception openException;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ExceptionStream(TupleStream stream) {
    this.stream = stream;
  }

  public List<TupleStream> children() {
    return null;
  }

  public void open() {
    try {
      stream.open();
    } catch (Exception e) {
      this.openException = e;
    }
  }

  public Tuple read() {
    if(openException != null) {
      //There was an exception during the open.
      SolrException.log(log, openException);
      return Tuple.EXCEPTION(openException.getMessage(), true);
    }

    try {
      return stream.read();
    } catch (Exception e) {
      SolrException.log(log, e);
      return Tuple.EXCEPTION(e.getMessage(), true);
    }
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
      .withFunctionName("non-expressible")
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_SOURCE)
      .withExpression("non-expressible");
  }

  public StreamComparator getStreamSort() {
    return this.stream.getStreamSort();
  }

  public void close() throws IOException {
    stream.close();
  }

  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
  }
}
