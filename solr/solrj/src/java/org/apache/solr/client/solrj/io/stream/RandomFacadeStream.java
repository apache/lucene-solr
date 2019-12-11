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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import static org.apache.solr.common.params.CommonParams.ROWS;

public class RandomFacadeStream extends TupleStream implements Expressible  {

  private TupleStream innerStream;

  public RandomFacadeStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");


    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // pull out known named params
    Map<String,String> params = new HashMap<String,String>();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("buckets") && !namedParam.getName().equals("bucketSorts") && !namedParam.getName().equals("limit")){
        params.put(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    //Add sensible defaults

    if(!params.containsKey("q")) {
      params.put("q", "*:*");
    }

    if(!params.containsKey("fl")) {
      params.put("fl", "*");
    }

    if(!params.containsKey("rows")) {
      params.put("rows", "500");
    }

    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    }
    else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }

    if(params.get(ROWS) != null) {
      int rows = Integer.parseInt(params.get(ROWS));
      if(rows >= 5000) {
        DeepRandomStream deepRandomStream = new DeepRandomStream();
        deepRandomStream.init(collectionName, zkHost, toSolrParams(params));
        this.innerStream = deepRandomStream;
      } else {
        RandomStream randomStream = new RandomStream();
        randomStream.init(zkHost, collectionName, params);
        this.innerStream = randomStream;
      }
    } else {
      RandomStream randomStream = new RandomStream();
      randomStream.init(zkHost, collectionName, params);
      this.innerStream = randomStream;
    }
  }

  private SolrParams toSolrParams(Map<String, String> props) {
    ModifiableSolrParams sp = new ModifiableSolrParams();
    for(Map.Entry<String, String> entry : props.entrySet()) {
      sp.add(entry.getKey(), entry.getValue());
    }
    return sp;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return ((Expressible)innerStream).toExpression(factory);
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return innerStream.toExplanation(factory);
  }

  public void setStreamContext(StreamContext context) {
    this.innerStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    return innerStream.children();
  }

  public void open() throws IOException {
    innerStream.open();
  }

  public void close() throws IOException {
    innerStream.close();
  }

  public Tuple read() throws IOException {
    return innerStream.read();
  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }
}
