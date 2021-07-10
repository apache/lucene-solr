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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
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
import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import static org.apache.solr.client.solrj.io.stream.FacetStream.TIERED_PARAM;
import static org.apache.solr.client.solrj.io.stream.FacetStream.defaultTieredEnabled;

/**
 * @since 6.6.0
 */
public class StatsStream extends TupleStream implements Expressible, ParallelMetricsRollup  {

  private static final long serialVersionUID = 1;

  // use a single "*" rollup bucket
  private static final Bucket[] STATS_BUCKET = new Bucket[]{new Bucket("*")};

  private Metric[] metrics;
  private Tuple tuple;
  private int index;
  private String zkHost;
  private SolrParams params;
  private String collection;
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;
  private StreamContext context;
  protected transient TupleStream parallelizedStream;

  public StatsStream(String zkHost,
                     String collection,
                     SolrParams params,
                     Metric[] metrics
  ) throws IOException {
    init(collection, params, metrics, zkHost);
  }

  public StatsStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);

    if(collectionName.indexOf('"') > -1) {
      collectionName = collectionName.replaceAll("\"", "").replaceAll(" ", "");
    }

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    List<StreamExpression> metricExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, Metric.class);

    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // Construct the metrics
    Metric[] metrics = null;
    if(metricExpressions.size() > 0) {
      metrics = new Metric[metricExpressions.size()];
      for(int idx = 0; idx < metricExpressions.size(); ++idx){
        metrics[idx] = factory.constructMetric(metricExpressions.get(idx));
      }
    } else {
      metrics = new Metric[1];
      metrics[0] = new CountMetric();
    }

    // pull out known named params
    ModifiableSolrParams params = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost")){
        params.add(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    if(params.get("q") == null) {
      params.set("q", "*:*");
    }

    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    } else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }

    // We've got all the required items
    init(collectionName, params, metrics, zkHost);
  }

  public String getCollection() {
    return this.collection;
  }

  private void init(String collection,
                    SolrParams params,
                    Metric[] metrics,
                    String zkHost) throws IOException {
    this.zkHost  = zkHost;
    this.collection = collection;
    this.metrics = metrics;
    this.params = params;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    // collection
    if(collection.indexOf(',') > -1) {
      expression.addParameter("\""+collection+"\"");
    } else {
      expression.addParameter(collection);
    }

    // parameters
    ModifiableSolrParams tmpParams = new ModifiableSolrParams(params);

    for (Entry<String, String[]> param : tmpParams.getMap().entrySet()) {
      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(),
          String.join(",", param.getValue())));
    }

    // metrics
    for(Metric metric : metrics){
      expression.addParameter(metric.toExpression(factory));
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
    // TODO: fix this so we know the # of workers - check with Joel about a Topic's ability to be in a
    // parallel stream.

    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);

    child.setExpression(params.stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), Arrays.toString(e.getValue()))).collect(Collectors.joining(",")));

    explanation.addChild(child);

    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.context = context;
    cache = context.getSolrClientCache();
  }

  public List<TupleStream> children() {
    return new ArrayList<>();
  }

  public void open() throws IOException {

    @SuppressWarnings({"unchecked"})
    Map<String, List<String>> shardsMap = (Map<String, List<String>>)context.get("shards");

    // Parallelize the stats stream across multiple collections for an alias using plist if possible
    if (shardsMap == null && params.getBool(TIERED_PARAM, defaultTieredEnabled)) {
      ClusterStateProvider clusterStateProvider = cache.getCloudSolrClient(zkHost).getClusterStateProvider();
      final List<String> resolved = clusterStateProvider != null ? clusterStateProvider.resolveAlias(collection) : null;
      if (resolved != null && resolved.size() > 1) {
        Optional<TupleStream> maybeParallelize = openParallelStream(context, resolved, metrics);
        if (maybeParallelize.isPresent()) {
          this.parallelizedStream = maybeParallelize.get();
          return; // we're using a plist to parallelize the facet operation
        } // else, there's a metric that we can't rollup over the plist results safely ... no plist for you!
      }
    }

    String json = getJsonFacetString(metrics);

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams(params);
    paramsLoc.set("json.facet", json);
    paramsLoc.set("rows", "0");

    if(shardsMap == null) {
      QueryRequest request = new QueryRequest(paramsLoc, SolrRequest.METHOD.POST);
      cloudSolrClient = cache.getCloudSolrClient(zkHost);
      try {
        NamedList<?> response = cloudSolrClient.request(request, collection);
        getTuples(response, metrics);
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
        NamedList<?> response = client.request(request);
        getTuples(response, metrics);
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
    if (parallelizedStream != null) {
      return parallelizedStream.read();
    }

    if(index == 0) {
      ++index;
      return tuple;
    } else {
      return Tuple.EOF();
    }
  }

  private String getJsonFacetString(Metric[] _metrics) {
    StringBuilder buf = new StringBuilder();
    appendJson(buf, _metrics);
    return "{"+buf.toString()+"}";
  }

  private void appendJson(StringBuilder buf,
                          Metric[] _metrics) {

    int metricCount = 0;
    for(Metric metric : _metrics) {
      String identifier = metric.getIdentifier();
      if(!identifier.startsWith("count(")) {
        if(metricCount>0) {
          buf.append(",");
        }
        if(identifier.startsWith("per(")) {
          buf.append("\"facet_").append(metricCount).append("\":\"").append(identifier.replaceFirst("per", "percentile")).append('"');
        } else if(identifier.startsWith("std(")) {
          buf.append("\"facet_").append(metricCount).append("\":\"").append(identifier.replaceFirst("std", "stddev")).append('"');
        } else if (identifier.startsWith("countDist(")) {
          buf.append("\"facet_").append(metricCount).append("\":\"").append(identifier.replaceFirst("countDist", "unique")).append('"');
        } else {
          buf.append("\"facet_").append(metricCount).append("\":\"").append(identifier).append('"');
        }
        ++metricCount;
      }
    }
  }

  private void getTuples(NamedList<?> response,
                         Metric[] metrics) {

    this.tuple = new Tuple();
    NamedList<?> facets = (NamedList<?>)response.get("facets");
    fillTuple(tuple, facets, metrics);
  }

  private void fillTuple(Tuple t,
                         NamedList<?> nl,
                         Metric[] _metrics) {

    if(nl == null) {
      return;
    }

    int m = 0;
    for(Metric metric : _metrics) {
      String identifier = metric.getIdentifier();
      if(!identifier.startsWith("count(")) {
        if(nl.get("facet_"+m) != null) {
          Object d = nl.get("facet_" + m);
          if(d instanceof Number) {
            if (metric.outputLong) {
              t.put(identifier, Math.round(((Number)d).doubleValue()));
            } else {
              t.put(identifier, ((Number)d).doubleValue());
            }
          } else {
            t.put(identifier, d);
          }
        }
        ++m;
      } else {
        long l = ((Number)nl.get("count")).longValue();
        t.put("count(*)", l);
      }
    }
  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public TupleStream[] parallelize(List<String> partitions) throws IOException {
    final ModifiableSolrParams withoutTieredParam = new ModifiableSolrParams(params);
    withoutTieredParam.remove(TIERED_PARAM); // each individual request is not tiered

    TupleStream[] streams = new TupleStream[partitions.size()];
    for (int p = 0; p < streams.length; p++) {
      streams[p] = new StatsStream(zkHost, partitions.get(p), withoutTieredParam, metrics);
    }
    return streams;
  }

  @Override
  public TupleStream getSortedRollupStream(ParallelListStream plist, Metric[] rollupMetrics) throws IOException {
    return new SelectStream(new HashRollupStream(plist, STATS_BUCKET, rollupMetrics), getRollupSelectFields(rollupMetrics));
  }

  // Map the rollup metric to the original metric name so that we can project out the correct field names in the tuple
  protected Map<String, String> getRollupSelectFields(Metric[] rollupMetrics) {
    Map<String, String> map = new HashMap<>(rollupMetrics.length * 2);
    for (Metric m : rollupMetrics) {
      String[] cols = m.getColumns();
      map.put(m.getIdentifier(), cols != null && cols.length > 0 ? cols[0] : "*");
    }
    return map;
  }
}