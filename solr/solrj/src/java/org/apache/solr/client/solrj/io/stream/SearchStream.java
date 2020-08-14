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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
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
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

public class SearchStream extends TupleStream implements Expressible  {

  private String zkHost;
  private ModifiableSolrParams params;
  private String collection;
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;
  private Iterator<SolrDocument> documentIterator;
  protected StreamComparator comp;

  public SearchStream() {}


  public SearchStream(StreamExpression expression, StreamFactory factory) throws IOException{
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
    ModifiableSolrParams params = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("buckets") && !namedParam.getName().equals("bucketSorts") && !namedParam.getName().equals("limit")){
        params.add(namedParam.getName(), namedParam.getParameter().toString().trim());
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

  void init(String zkHost, String collection, ModifiableSolrParams params) throws IOException {
    this.zkHost  = zkHost;
    this.params   = params;

    if(this.params.get(CommonParams.Q) == null) {
      this.params.add(CommonParams.Q, "*:*");
    }
    this.collection = collection;
    if(params.get(CommonParams.SORT) != null) {
      this.comp = parseComp(params.get(CommonParams.SORT), params.get(CommonParams.FL));
    }
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression("search");

    // collection
    if(collection.indexOf(',') > -1) {
      expression.addParameter("\""+collection+"\"");
    } else {
      expression.addParameter(collection);
    }

    for (Entry<String, String[]> param : params.getMap().entrySet()) {
      for (String val : param.getValue()) {
        // SOLR-8409: Escaping the " is a special case.
        // Do note that in any other BASE streams with parameters where a " might come into play
        // that this same replacement needs to take place.
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(),
            val.replace("\"", "\\\"")));
      }
    }

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());

    explanation.setFunctionName("search");
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    explanation.setExpression(toExpression(factory).toString());

    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);

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
      cloudSolrClient = new CloudSolrClient.Builder(hosts, Optional.empty()).build();
    }


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


  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return comp;
  }

  private StreamComparator parseComp(String sort, String fl) throws IOException {

    HashSet fieldSet = null;

    if(fl != null) {
      fieldSet = new HashSet();
      String[] fls = fl.split(",");
      for (String f : fls) {
        fieldSet.add(f.trim()); //Handle spaces in the field list.
      }
    }

    String[] sorts = sort.split(",");
    StreamComparator[] comps = new StreamComparator[sorts.length];
    for(int i=0; i<sorts.length; i++) {
      String s = sorts[i];

      String[] spec = s.trim().split("\\s+"); //This should take into account spaces in the sort spec.

      if (spec.length != 2) {
        throw new IOException("Invalid sort spec:" + s);
      }

      String fieldName = spec[0].trim();
      String order = spec[1].trim();

      if(fieldSet != null && !fieldSet.contains(spec[0])) {
        throw new IOException("Fields in the sort spec must be included in the field list:"+spec[0]);
      }

      comps[i] = new FieldComparator(fieldName, order.equalsIgnoreCase("asc") ? ComparatorOrder.ASCENDING : ComparatorOrder.DESCENDING);
    }

    if(comps.length > 1) {
      return new MultipleFieldComparator(comps);
    } else {
      return comps[0];
    }
  }
}
