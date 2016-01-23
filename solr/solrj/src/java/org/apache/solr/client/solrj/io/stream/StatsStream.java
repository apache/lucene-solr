package org.apache.solr.client.solrj.io.stream;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

public class StatsStream extends TupleStream implements Expressible  {

  private static final long serialVersionUID = 1;

  private Metric[] metrics;
  private String zkHost;
  private Tuple tuple;
  private Map<String, String> props;
  private String collection;
  private boolean done;
  private long count;
  private boolean doCount;
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;

  public StatsStream(String zkHost,
                     String collection,
                     Map<String, String> props,
                     Metric[] metrics) {
    init(zkHost, collection, props, metrics);
  }
  
  private void init(String zkHost, String collection, Map<String, String> props, Metric[] metrics) {
    this.zkHost  = zkHost;
    this.props   = props;
    this.metrics = metrics;
    this.collection = collection;
  }
  
  public StatsStream(StreamExpression expression, StreamFactory factory) throws IOException{   
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    List<StreamExpression> metricExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Metric.class);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    
    // Validate there are no unknown parameters - zkHost is namedParameter so we don't need to count it twice
    if(expression.getParameters().size() != 1 + namedParams.size() + metricExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - unknown operands found",expression));
    }
    
    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }
        
    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }
    
    Map<String,String> params = new HashMap<String,String>();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost")){
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
    
    // metrics, optional - if not provided then why are you using this?
    Metric[] metrics = new Metric[metricExpressions.size()];
    for(int idx = 0; idx < metricExpressions.size(); ++idx){
      metrics[idx] = factory.constructMetric(metricExpressions.get(idx));
    }
    
    // We've got all the required items
    init(zkHost, collectionName, params, metrics);
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // functionName(collectionName, param1, param2, ..., paramN, sort="comp", sum(fieldA), avg(fieldB))
    
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
    
    // metrics
    for(Metric metric : metrics){
      expression.addParameter(metric.toExpression(factory));
    }
    
    return expression;   
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
      cloudSolrClient = new CloudSolrClient(zkHost);
    }

    ModifiableSolrParams params = getParams(this.props);
    addStats(params, metrics);
    params.add("stats", "true");
    params.add("rows", "0");

    QueryRequest request = new QueryRequest(params);
    try {
      NamedList response = cloudSolrClient.request(request, collection);
      this.tuple = getTuple(response);
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
    if(!done) {
      done = true;
      return tuple;
    } else {
      Map fields = new HashMap();
      fields.put("EOF", true);
      Tuple tuple = new Tuple(fields);
      return tuple;
    }
  }

  public StreamComparator getStreamSort() {
    return null;
  }

  private void addStats(ModifiableSolrParams params, Metric[] _metrics) {
    Map<String, List<String>> m = new HashMap();
    for(Metric metric : _metrics) {
      String metricId = metric.getIdentifier();
      if(metricId.contains("(")) {
        metricId = metricId.substring(0, metricId.length()-1);
        String[] parts = metricId.split("\\(");
        String function = parts[0];
        String column = parts[1];
        List<String> stats = m.get(column);

        if(stats == null && !column.equals("*")) {
          stats = new ArrayList();
          m.put(column, stats);
        }

        if(function.equals("min")) {
          stats.add("min");
        } else if(function.equals("max")) {
          stats.add("max");
        } else if(function.equals("sum")) {
          stats.add("sum");
        } else if(function.equals("avg")) {
          stats.add("mean");
        } else if(function.equals("count")) {
          this.doCount = true;
        }
      }
    }

    for(String field : m.keySet()) {
      StringBuilder buf = new StringBuilder();
      List<String> stats = m.get(field);
      buf.append("{!");

      for(String stat : stats) {
        buf.append(stat).append("=").append("true ");
      }

      buf.append("}").append(field);
      params.add("stats.field", buf.toString());
    }
  }

  private ModifiableSolrParams getParams(Map<String, String> props) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    for(String key : props.keySet()) {
      String value = props.get(key);
      params.add(key, value);
    }
    return params;
  }

  private Tuple getTuple(NamedList response) {

    Map map = new HashMap();

    if(doCount) {
      SolrDocumentList solrDocumentList = (SolrDocumentList) response.get("response");
      this.count = solrDocumentList.getNumFound();
      map.put("count(*)", this.count);
    }

    NamedList stats = (NamedList)response.get("stats");
    NamedList statsFields = (NamedList)stats.get("stats_fields");

    for(int i=0; i<statsFields.size(); i++) {
      String field = statsFields.getName(i);
      NamedList theStats = (NamedList)statsFields.getVal(i);
      for(int s=0; s<theStats.size(); s++) {
        addStat(map, field, theStats.getName(s), theStats.getVal(s));
      }
    }

    Tuple tuple = new Tuple(map);
    return tuple;
  }

  public int getCost() {
    return 0;
  }

  private void addStat(Map map, String field, String stat, Object val) {
    if(stat.equals("mean")) {
      map.put("avg("+field+")", val);
    } else {
      map.put(stat+"("+field+")", val);
    }
  }
}