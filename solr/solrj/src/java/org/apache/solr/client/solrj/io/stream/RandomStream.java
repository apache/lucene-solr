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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;

import static org.apache.solr.common.params.CommonParams.SORT;

/**
 *  The RandomStream emits a stream of psuedo random Tuples that match the query parameters. Sample expression syntax:
 *  random(collection, q="Hello word", rows="50", fl="title, body")
 * @since 6.1.0
 **/

public class RandomStream extends TupleStream implements Expressible  {

  private String zkHost;
  private Map<String, String> props;
  private String collection;
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;
  private Iterator<SolrDocument> documentIterator;

  public RandomStream() {
    // Used by the RandomFacade
  }

  public RandomStream(String zkHost,
                      String collection,
                     Map<String, String> props) throws IOException {
    init(zkHost, collection, props);
  }

  public RandomStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");


    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }

    // pull out known named params
    Map<String,String> params = new HashMap<String,String>();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("buckets") && !namedParam.getName().equals("bucketSorts") && !namedParam.getName().equals("limit")){
        params.put(namedParam.getName(), namedParam.getParameter().toString().trim());
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
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }

    // We've got all the required items
    init(zkHost, collectionName, params);
  }

  void init(String zkHost, String collection, Map<String, String> props) throws IOException {
    this.zkHost  = zkHost;
    this.props   = props;
    this.collection = collection;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // collection
    expression.addParameter(collection);

    // parameters
    for(Entry<String,String> param : props.entrySet()){
      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), param.getValue()));
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
    if(null != props){
      child.setExpression(props.entrySet().stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(",")));
    }
    explanation.addChild(child);
    
    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    cache = context.getSolrClientCache();
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    return l;
  }

  public void open() throws IOException {
    if(cache != null) {
      cloudSolrClient = cache.getCloudSolrClient(zkHost);
    } else {
      final List<String> hosts = new ArrayList<>();
      hosts.add(zkHost);
      cloudSolrClient = new CloudSolrClient.Builder(hosts, Optional.empty()).withSocketTimeout(30000).withConnectionTimeout(15000).build();
    }

    ModifiableSolrParams params = getParams(this.props);

    params.remove(SORT); //Override any sort.

    Random rand = new Random();
    int seed = rand.nextInt();

    String sortField = "random_"+seed;
    params.add(SORT, sortField+" asc");

    QueryRequest request = new QueryRequest(params, SolrRequest.METHOD.POST);
    try {
      QueryResponse response = request.process(cloudSolrClient, collection);
      SolrDocumentList docs = response.getResults();
      documentIterator = docs.iterator();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void close() throws IOException {
    if(cache == null) {
      cloudSolrClient.close();
    }
  }

  public Tuple read() throws IOException {
    if(documentIterator.hasNext()) {
      Map map = new HashMap();
      SolrDocument doc = documentIterator.next();
      for(Entry<String, Object> entry : doc.entrySet()) {
        map.put(entry.getKey(), entry.getValue());
      }
      return new Tuple(map);
    } else {
      Map fields = new HashMap();
      fields.put("EOF", true);
      Tuple tuple = new Tuple(fields);
      return tuple;
    }
  }

  private ModifiableSolrParams getParams(Map<String, String> props) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    for(Entry<String, String> entry : props.entrySet()) {
      String value = entry.getValue();
      params.add(entry.getKey(), value);
    }
    return params;
  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }
}
