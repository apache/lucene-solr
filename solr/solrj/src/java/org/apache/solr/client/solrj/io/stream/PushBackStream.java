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
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * A TupleStream that allows a single Tuple to be pushed back onto the stream after it's been read.
 * This is a useful class when building streams that maintain the order of Tuples between multiple
 * substreams.
 * @since 5.1.0
 **/

public class PushBackStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private Tuple tuple;

  public PushBackStream(TupleStream stream) {
    this.stream = stream;
  }
  
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException{
    if(stream instanceof Expressible){
      return ((Expressible)stream).toExpression(factory);
    }
    
    throw new IOException("This PushBackStream contains a non-expressible TupleStream - it cannot be converted to an expression");
  }

  public Explanation toExplanation(StreamFactory factory) throws IOException{
    return stream.toExplanation(factory);
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

  public void pushBack(Tuple tuple) {
    this.tuple = tuple;
  }

  public Tuple read() throws IOException {
    if(tuple != null) {
      Tuple t = tuple;
      tuple = null;
      return t;
    } else {
      return stream.read();
    }
  }
  
  /** Return the stream sort - ie, the order in which records are returned
   *  This returns the streamSort of the substream */
  public StreamComparator getStreamSort(){
    return stream.getStreamSort();
  }


  public int getCost() {
    return 0;
  }
}
