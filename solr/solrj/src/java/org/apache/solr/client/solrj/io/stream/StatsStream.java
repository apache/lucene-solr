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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
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
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
* @since 6.0.0
*/
public class StatsStream extends TupleStream implements Expressible  {

  private static final long serialVersionUID = 1;

  private Metric[] metrics;
  private String zkHost;
  private Tuple tuple;
  private SolrParams params;
  private String collection;
  private boolean done;
  private boolean doCount;
  private Map<String, Metric> metricMap;
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;
  protected StreamContext streamContext;

  public StatsStream(String zkHost,
                     String collection,
                     SolrParams params,
                     Metric[] metrics) {
    init(zkHost, collection, params, metrics);
  }

  private void init(String zkHost, String collection, SolrParams params, Metric[] metrics) {
    this.zkHost  = zkHost;
    this.params = params;
    this.metrics = metrics;
    this.collection = collection;
    metricMap = new HashMap();
    for(Metric metric : metrics) {
      metricMap.put(metric.getIdentifier(), metric);
    }
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

    ModifiableSolrParams params = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost")){
        params.set(namedParam.getName(), namedParam.getParameter().toString().trim());
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
    ModifiableSolrParams mParams = new ModifiableSolrParams(params);
    for (Entry<String, String[]> param : mParams.getMap().entrySet()) {
      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), String.join(",", param.getValue())));
    }

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    // metrics
    for(Metric metric : metrics){
      expression.addParameter(metric.toExpression(factory));
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());

    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    explanation.setExpression(toExpression(factory).toString());

    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName(String.format(Locale.ROOT, "solr (worker ? of ?)"));
      // TODO: fix this so we know the # of workers - check with Joel about a Stat's ability to be in a
      // parallel stream.

    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);
    ModifiableSolrParams mParams = new ModifiableSolrParams(params);
    child.setExpression(mParams.getMap().entrySet().stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(",")));
    explanation.addChild(child);

    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    streamContext = context;
    cache = context.getSolrClientCache();
  }

  public List<TupleStream> children() {
    return new ArrayList<>();
  }

  public void open() throws IOException {
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams(this.params);
    addStats(paramsLoc, metrics);
    paramsLoc.set("stats", "true");
    paramsLoc.set("rows", "0");
    if (streamContext.isLocal()) {
      paramsLoc.set("distrib", "false");
    }

    Map<String, List<String>> shardsMap = (Map<String, List<String>>)streamContext.get("shards");
    if(shardsMap == null) {
      QueryRequest request = new QueryRequest(paramsLoc, SolrRequest.METHOD.POST);
      CloudSolrClient cloudSolrClient = cache.getCloudSolrClient(zkHost);
      try {
        NamedList response = cloudSolrClient.request(request, collection);
        this.tuple = getTuple(response);
      } catch (Exception e) {
        throw new IOException(e);
      }
    } else {
      List<String> shards = shardsMap.get(collection);
      HttpSolrClient client = cache.getHttpSolrClient(shards.get(0));

      if(shards.size() > 1) {
        String shardsParam = getShardString(shards);
        paramsLoc.add("shards", shardsParam);
        paramsLoc.add("distrib", "true");
      }

      QueryRequest request = new QueryRequest(paramsLoc, SolrRequest.METHOD.POST);
      try {
        NamedList response = client.request(request);
        this.tuple = getTuple(response);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  private String getShardString(List<String> shards) {
    StringBuilder builder = new StringBuilder();
    for(String shard : shards) {
      if(builder.length() > 0) {
        builder.append(",");
      }
      builder.append(shard);
    }
    return builder.toString();
  }



  public void close() throws IOException {

  }

  public Tuple read() throws IOException {
    if(!done) {
      done = true;
      return tuple;
    } else {
      Map<String, Object> fields = new HashMap<>();
      fields.put("EOF", true);
      return new Tuple(fields);
    }
  }

  public StreamComparator getStreamSort() {
    return null;
  }

  private void addStats(ModifiableSolrParams params, Metric[] _metrics) {
    Map<String, List<String>> m = new HashMap<>();
    for(Metric metric : _metrics) {
      String metricId = metric.getIdentifier();
      if(metricId.contains("(")) {
        metricId = metricId.substring(0, metricId.length()-1);
        String[] parts = metricId.split("\\(");
        String function = parts[0];
        String column = parts[1];
        List<String> stats = m.get(column);

        if(stats == null) {
          stats = new ArrayList<>();
        }

        if(!column.equals("*")) {
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

    for(Entry<String, List<String>> entry : m.entrySet()) {
      StringBuilder buf = new StringBuilder();
      List<String> stats = entry.getValue();
      buf.append("{!");

      for(String stat : stats) {
        buf.append(stat).append("=").append("true ");
      }

      buf.append("}").append(entry.getKey());
      params.add("stats.field", buf.toString());
    }
  }

  private Tuple getTuple(NamedList response) {

    Map<String, Object> map = new HashMap<>();
    SolrDocumentList solrDocumentList = (SolrDocumentList) response.get("response");

    long count = solrDocumentList.getNumFound();

    if(doCount) {
      map.put("count(*)", count);
    }

    if(count != 0) {
      NamedList stats = (NamedList)response.get("stats");
      NamedList statsFields = (NamedList)stats.get("stats_fields");

      for(int i=0; i<statsFields.size(); i++) {
        String field = statsFields.getName(i);
        NamedList theStats = (NamedList)statsFields.getVal(i);
        for(int s=0; s<theStats.size(); s++) {
          addStat(map, field, theStats.getName(s), theStats.getVal(s));
        }
      }
    }

    return new Tuple(map);
  }

  public int getCost() {
    return 0;
  }

  private void addStat(Map<String, Object> map, String field, String stat, Object val) {
    if(stat.equals("mean")) {
      String name = "avg("+field+")";
      Metric m = metricMap.get(name);
      if(m.outputLong) {
        Number num = (Number) val;
        map.put(name, Math.round(num.doubleValue()));
      } else {
        map.put(name, val);
      }
    } else {
      map.put(stat+"("+field+")", val);
    }
  }
}
