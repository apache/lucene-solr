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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * @since 6.6.0
 */
public class TimeSeriesStream extends TupleStream implements Expressible  {

  private static final long serialVersionUID = 1;

  private String start;
  private String end;
  private String gap;
  private String field;
  private String format;
  private DateTimeFormatter formatter;

  private Metric[] metrics;
  private List<Tuple> tuples = new ArrayList();
  private int index;
  private String zkHost;
  private SolrParams params;
  private String collection;
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;

  public TimeSeriesStream(String zkHost,
                          String collection,
                          SolrParams params,
                          Metric[] metrics,
                          String field,
                          String start,
                          String end,
                          String gap,
                          String format) throws IOException {
    init(collection, params, field, metrics, start, end, gap, format, zkHost);
  }

  public TimeSeriesStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);

    if(collectionName.indexOf('"') > -1) {
      collectionName = collectionName.replaceAll("\"", "").replaceAll(" ", "");
    }

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter startExpression = factory.getNamedOperand(expression, "start");
    StreamExpressionNamedParameter endExpression = factory.getNamedOperand(expression, "end");
    StreamExpressionNamedParameter fieldExpression = factory.getNamedOperand(expression, "field");
    StreamExpressionNamedParameter gapExpression = factory.getNamedOperand(expression, "gap");
    StreamExpressionNamedParameter formatExpression = factory.getNamedOperand(expression, "format");
    StreamExpressionNamedParameter qExpression = factory.getNamedOperand(expression, "q");

    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    List<StreamExpression> metricExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, Metric.class);


    if(qExpression == null) {
      throw new IOException("The timeseries expression requires the q parameter");
    }

    String start = null;
    if(startExpression != null) {
      start = ((StreamExpressionValue)startExpression.getParameter()).getValue();
    }

    String end = null;
    if(endExpression != null) {
      end = ((StreamExpressionValue)endExpression.getParameter()).getValue();
    }

    String gap = null;
    if(gapExpression != null) {
      gap = ((StreamExpressionValue)gapExpression.getParameter()).getValue();
    }

    String field = null;
    if(fieldExpression != null) {
      field = ((StreamExpressionValue)fieldExpression.getParameter()).getValue();
    }

    String format = null;
    if(formatExpression != null) {
      format = ((StreamExpressionValue)formatExpression.getParameter()).getValue();
    }

    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }

    // Construct the metrics
    Metric[] metrics = new Metric[metricExpressions.size()];
    for(int idx = 0; idx < metricExpressions.size(); ++idx){
      metrics[idx] = factory.constructMetric(metricExpressions.get(idx));
    }

    if(0 == metrics.length){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one metric expected.",expression,collectionName));
    }

    // pull out known named params
    ModifiableSolrParams params = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("start") && !namedParam.getName().equals("end") && !namedParam.getName().equals("gap")){
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
    init(collectionName, params, field, metrics, start, end, gap, format, zkHost);
  }

  public String getCollection() {
    return this.collection;
  }

  private void init(String collection,
                    SolrParams params,
                    String field,
                    Metric[] metrics,
                    String start,
                    String end,
                    String gap,
                    String format,
                    String zkHost) throws IOException {
    this.zkHost  = zkHost;
    this.collection = collection;
    this.start = start;
    this.gap = gap;
    if(!gap.startsWith("+")) {
      this.gap = "+"+gap;
    }
    this.metrics = metrics;
    this.field = field;
    this.params = params;
    this.end = end;
    if(format != null) {
      this.format = format;
      formatter = DateTimeFormatter.ofPattern(format, Locale.ROOT);
    }
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

    expression.addParameter(new StreamExpressionNamedParameter("start", start));
    expression.addParameter(new StreamExpressionNamedParameter("end", end));
    expression.addParameter(new StreamExpressionNamedParameter("gap", gap));
    expression.addParameter(new StreamExpressionNamedParameter("field", gap));
    expression.addParameter(new StreamExpressionNamedParameter("format", format));


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
    cache = context.getSolrClientCache();
  }

  public List<TupleStream> children() {
    return new ArrayList();
  }

  public void open() throws IOException {
    if (cache != null) {
      cloudSolrClient = cache.getCloudSolrClient(zkHost);
    } else {
      final List<String> hosts = new ArrayList<>();
      hosts.add(zkHost);
      cloudSolrClient = new Builder(hosts, Optional.empty()).build();
    }

    String json = getJsonFacetString(field, metrics, start, end, gap);

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams(params);
    paramsLoc.set("json.facet", json);
    paramsLoc.set("rows", "0");

    QueryRequest request = new QueryRequest(paramsLoc, SolrRequest.METHOD.POST);
    try {
      NamedList response = cloudSolrClient.request(request, collection);
      getTuples(response, field, metrics);
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
    if(index < tuples.size()) {
      Tuple tuple = tuples.get(index);
      ++index;
      return tuple;
    } else {
      Map fields = new HashMap();
      fields.put("EOF", true);
      Tuple tuple = new Tuple(fields);
      return tuple;
    }
  }

  private String getJsonFacetString(String field, Metric[] _metrics, String start, String end, String gap) {
    StringBuilder buf = new StringBuilder();
    appendJson(buf, _metrics, field, start, end, gap);
    return "{"+buf.toString()+"}";
  }


  private void appendJson(StringBuilder buf,
                          Metric[] _metrics,
                          String field,
                          String start,
                          String end,
                          String gap) {
    buf.append('"');
    buf.append("timeseries");
    buf.append('"');
    buf.append(":{");
    buf.append("\"type\":\"range\"");
    buf.append(",\"field\":\"").append(field).append('"');
    buf.append(",\"start\":\"").append(start).append('"');
    buf.append(",\"end\":\"").append(end).append('"');
    buf.append(",\"gap\":\"").append(gap).append('"');

    buf.append(",\"facet\":{");
    int metricCount = 0;
    for(Metric metric : _metrics) {
      String identifier = metric.getIdentifier();
      if(!identifier.startsWith("count(")) {
        if(metricCount>0) {
          buf.append(",");
        }
        buf.append("\"facet_").append(metricCount).append("\":\"").append(identifier).append('"');
        ++metricCount;
      }
    }
    buf.append("}}");
  }

  private void getTuples(NamedList response,
                         String field,
                         Metric[] metrics) {

    Tuple tuple = new Tuple(new HashMap());
    NamedList facets = (NamedList)response.get("facets");
    fillTuples(tuples, tuple, facets, field, metrics);
  }

  private void fillTuples(List<Tuple> tuples,
                          Tuple currentTuple,
                          NamedList facets,
                          String field,
                          Metric[] _metrics) {

    NamedList nl = (NamedList)facets.get("timeseries");
    if(nl == null) {
      return;
    }

    List allBuckets = (List)nl.get("buckets");
    for(int b=0; b<allBuckets.size(); b++) {
      NamedList bucket = (NamedList)allBuckets.get(b);
      Object val = bucket.get("val");

      if(formatter != null) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(((java.util.Date) val).toInstant(), ZoneOffset.UTC);
        val = localDateTime.format(formatter);
      }

      Tuple t = currentTuple.clone();
      t.put(field, val);
      int m = 0;
      for(Metric metric : _metrics) {
        String identifier = metric.getIdentifier();
        if(!identifier.startsWith("count(")) {
          if(bucket.get("facet_"+m) != null) {
            Number d = (Number) bucket.get("facet_" + m);
            if (metric.outputLong) {
              t.put(identifier, Math.round(d.doubleValue()));
            } else {
              t.put(identifier, d.doubleValue());
            }
          } else {
            t.put(identifier, 0);
          }
          ++m;
        } else {
          long l = ((Number)bucket.get("count")).longValue();
          t.put("count(*)", l);
        }
      }
      tuples.add(t);
    }
  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }
}
