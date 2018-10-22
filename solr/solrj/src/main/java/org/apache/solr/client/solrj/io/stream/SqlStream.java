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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

/**
 * @since 7.0.0
 */
public class SqlStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected SolrParams params;
  protected transient CloudSolrClient cloudSolrClient;
  protected transient TupleStream tupleStream;
  protected transient StreamContext streamContext;

  /**
   * @param zkHost         Zookeeper ensemble connection string
   * @param collectionName Name of the collection to operate on
   * @param params         Map&lt;String, String&gt; of parameter/value pairs
   * @throws IOException Something went wrong
   *                     <p>
   *                     This form does not allow specifying multiple clauses, say "fq" clauses, use the form that
   *                     takes a SolrParams. Transition code can call the preferred method that takes SolrParams
   *                     by calling CloudSolrStream(zkHost, collectionName,
   *                     new ModifiableSolrParams(SolrParams.toMultiMap(new NamedList(Map&lt;String, String&gt;)));
   */
  public SqlStream(String zkHost, String collectionName, SolrParams params) throws IOException {
    init(collectionName, zkHost, params);
  }

  public SqlStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    // Collection Name
    if(null == collectionName){
      collectionName = factory.getDefaultCollection();
    }

    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
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

    // We've got all the required items
    init(collectionName, zkHost, mParams);
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    // functionName(collectionName, param1, param2, ..., paramN, sort="comp", [aliases="field=alias,..."])

    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));

    // collection
    expression.addParameter(collection);

    // parameters

    for (Entry<String, String[]> param : params) {
      String value = String.join(",", param.getValue());

      // SOLR-8409: This is a special case where the params contain a " character
      // Do note that in any other BASE streams with parameters where a " might come into play
      // that this same replacement needs to take place.
      value = value.replace("\"", "\\\"");

      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), value));
    }

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());

    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    explanation.setExpression(toExpression(factory).toString());

    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);

    if(null != params){
      ModifiableSolrParams mParams = new ModifiableSolrParams(params);
      child.setExpression(mParams.getMap().entrySet().stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(",")));
    }
    explanation.addChild(child);

    return explanation;
  }

  protected void init(String collectionName, String zkHost, SolrParams params) throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.params = new ModifiableSolrParams(params);

    if (params.get("stmt") == null) {
      throw new IOException("stmt param expected for sql function");
    }
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  public void open() throws IOException {
    constructStream();
    tupleStream.open();
  }

  public List<TupleStream> children() {
    return null;
  }

  protected void constructStream() throws IOException {
    try {

      List<String> shardUrls = getShards(this.zkHost, this.collection, this.streamContext);
      Collections.shuffle(shardUrls, new Random());
      String url  = shardUrls.get(0);
      ModifiableSolrParams mParams = new ModifiableSolrParams(params);
      mParams.add(CommonParams.QT, "/sql");
      this.tupleStream = new SolrStream(url, mParams);
      if(streamContext != null) {
        tupleStream.setStreamContext(streamContext);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void close() throws IOException {
    tupleStream.close();
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return null;
  }

  public Tuple read() throws IOException {
    return tupleStream.read();
  }
}
