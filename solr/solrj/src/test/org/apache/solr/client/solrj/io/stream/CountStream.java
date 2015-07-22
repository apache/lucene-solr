package  org.apache.solr.client.solrj.io.stream;
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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class CountStream extends TupleStream implements ExpressibleStream, Serializable {

  private TupleStream stream;
  private int count;

  public CountStream(TupleStream stream) {
    this.stream = stream;
  }
  
  public CountStream(StreamExpression expression, StreamFactory factory) throws IOException{
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, ExpressibleStream.class, TupleStream.class);
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
        
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }
    
    stream = factory.constructStream(streamExpressions.get(0));
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // stream
    if(stream instanceof ExpressibleStream){
      expression.addParameter(((ExpressibleStream)stream).toExpression(factory));
    }
    else{
      throw new IOException("This CountStream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }
    
    return expression;
  }

  public void close() throws IOException {
    this.stream.close();
  }

  public void open() throws IOException {
    this.stream.open();
  }

  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList();
    l.add(stream);
    return l;
  }

  public void setStreamContext(StreamContext streamContext) {
    stream.setStreamContext(streamContext);
  }

  public Tuple read() throws IOException {
    Tuple t = stream.read();
    if(t.EOF) {
      t.put("count", count);
      return t;
    } else {
      ++count;
      return t;
    }
  }
}