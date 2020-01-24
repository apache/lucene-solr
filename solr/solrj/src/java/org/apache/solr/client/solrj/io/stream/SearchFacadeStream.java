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
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Connects to Zookeeper to pick replicas from a specific collection to send the query to.
 * Under the covers the SolrStream instances send the query to the replicas.
 * SolrStreams are opened using a thread pool, but a single thread is used
 * to iterate and merge Tuples from each SolrStream.
 * @since 5.1.0
 **/

public class SearchFacadeStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private TupleStream innerStream;

  public SearchFacadeStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);

    //Handle comma delimited list of collections.
    if(collectionName.indexOf('"') > -1) {
      collectionName = collectionName.replaceAll("\"", "").replaceAll(" ", "");
    }

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }


    ModifiableSolrParams mParams = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("aliases")){
        mParams.add(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
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
    /*
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }
    */

    if(mParams.get(CommonParams.QT) != null && mParams.get(CommonParams.QT).equals("/export")) {
      CloudSolrStream cloudSolrStream = new CloudSolrStream();
      cloudSolrStream.init(collectionName, zkHost, mParams);
      this.innerStream = cloudSolrStream;
    } else {

      if(mParams.get("partitionKeys") != null) {
        throw new IOException("partitionKeys can only be used in the search function when the /export handler is specified");
      }

      SearchStream searchStream = new SearchStream();
      searchStream.init(zkHost, collectionName, mParams);
      this.innerStream = searchStream;
    }
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

  /**
   * Opens the CloudSolrStream
   *
   ***/
  public void open() throws IOException {
    innerStream.open();
  }

  public List<TupleStream> children() {
    return innerStream.children();
  }

  /**
   *  Closes the CloudSolrStream
   **/
  public void close() throws IOException {
    innerStream.close();
  }


  public Tuple read() throws IOException {
    return innerStream.read();
  }

  public StreamComparator getStreamSort(){
    return innerStream.getStreamSort();
  }
}
