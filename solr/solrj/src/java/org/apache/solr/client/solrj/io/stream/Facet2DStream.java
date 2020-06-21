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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
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

public class Facet2DStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private String collection;
  private ModifiableSolrParams params;
  private Bucket x;
  private Bucket y;
  private Metric metric;
  private String zkHost;
  private Iterator<Tuple> out;
  private List<Tuple> tuples = new ArrayList<Tuple>();
  private int dimensionX;
  private int dimensionY;
  private FieldComparator bucketSort;

  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;

  public Facet2DStream(String zkHost, String collection, ModifiableSolrParams params, Bucket x, Bucket y, String dimensions, Metric metric) throws IOException {
    if (dimensions != null) {
      String[] strDimensions = dimensions.split(",");
      if (strDimensions.length != 2) {
        throw new IOException(String.format(Locale.ROOT, "invalid expression %s - two dimension values expected"));
      }
      this.dimensionX = Integer.parseInt(strDimensions[0]);
      this.dimensionY = Integer.parseInt(strDimensions[1]);

    }
    String bucketSortString = metric.getIdentifier() + " desc";
    this.bucketSort = parseBucketSort(bucketSortString, x, y);

    init(collection, params, x, y, bucketSort, dimensionX, dimensionY, metric, zkHost);
  }

  public Facet2DStream(StreamExpression expression, StreamFactory factory) throws IOException {

    String collectionName = factory.getValueOperand(expression, 0);

    if (collectionName.indexOf('"') > -1) {
      collectionName = collectionName.replaceAll("\"", "").replaceAll(" ", "");
    }

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter bucketXExpression = factory.getNamedOperand(expression, "x");
    StreamExpressionNamedParameter bucketYExpression = factory.getNamedOperand(expression, "y");
    StreamExpressionNamedParameter dimensionsExpression = factory.getNamedOperand(expression, "dimensions");
    List <StreamExpression> metricExpression = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, Metric.class);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    if (collectionName == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - collectionName expected as first operand", expression));
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    for (StreamExpressionNamedParameter namedParam : namedParams) {
      if (!namedParam.getName().equals("x") && !namedParam.getName().equals("y") &&
          !namedParam.getName().equals("dimensions") && !namedParam.getName().equals("zkHost")){
        params.add(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    if(params.get("q") == null) {
      params.set("q", "*:*");
    }

    Bucket x = null;
    if (bucketXExpression != null) {
      if (bucketXExpression.getParameter() instanceof StreamExpressionValue) {
        String keyX = ((StreamExpressionValue) bucketXExpression.getParameter()).getValue();
        if(keyX != null && !keyX.equals("")){
          x = new Bucket(keyX.trim());
        }
      }
    }
    Bucket y = null;
    if (bucketYExpression != null) {
      if (bucketYExpression.getParameter() instanceof StreamExpressionValue) {
        String keyY = ((StreamExpressionValue) bucketYExpression.getParameter()).getValue();
        if(keyY != null && !keyY.equals("")){
          y = new Bucket(keyY.trim());
        }
      }
    }

    if (x == null || y == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - x and y buckets expected. eg. 'x=\"name\"'", expression, collectionName));
    }

    Metric metric = null;
    if(metricExpression == null || metricExpression.size() == 0){
      metric = new CountMetric();
    } else {
      metric = factory.constructMetric(metricExpression.get(0));
    }

    String bucketSortString = metric.getIdentifier() + " desc";
    FieldComparator bucketSort = parseBucketSort(bucketSortString, x, y);

    int dimensionX = 10;
    int dimensionY = 10;
    if (dimensionsExpression != null) {
      if (dimensionsExpression.getParameter() instanceof StreamExpressionValue) {
        String[] strDimensions = ((StreamExpressionValue) dimensionsExpression.getParameter()).getValue().split(",");
        if (strDimensions.length != 2) {
          throw new IOException(String.format(Locale.ROOT, "invalid expression %s - two dimension values expected"));
        }
        dimensionX = Integer.parseInt(strDimensions[0].trim());
        dimensionY = Integer.parseInt(strDimensions[1].trim());
      }
    }

    String zkHost = null;
    if (zkHostExpression == null) {
      zkHost = factory.getCollectionZkHost(collectionName);
      if (zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    } else if (zkHostExpression.getParameter() instanceof StreamExpressionValue) {
      zkHost = ((StreamExpressionValue) zkHostExpression.getParameter()).getValue();
    }

    if (zkHost == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - zkHost not found for collection '%s'", expression, collectionName));
    }

    init(collectionName, params, x, y, bucketSort, dimensionX, dimensionY, metric, zkHost);
  }

  private FieldComparator parseBucketSort(String bucketSortString, Bucket x, Bucket y) throws IOException {
    String[] spec = bucketSortString.trim().split("\\s+");

    String fieldName = spec[0].trim();
    return new FieldComparator(fieldName, ComparatorOrder.DESCENDING);

  }

  private void init(String collection, SolrParams params, Bucket x, Bucket y, FieldComparator bucketSort, int dimensionX, int dimensionY, Metric metric, String zkHost) {
    this.collection = collection;
    this.params = new ModifiableSolrParams(params);
    this.x = x;
    this.y = y;
    this.dimensionX = dimensionX;
    this.dimensionY = dimensionY;
    this.metric = metric;
    this.bucketSort = bucketSort;
    this.zkHost = zkHost;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    explanation.setExpression(toExpression(factory).toString());

    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);

    child.setExpression(params.stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), Arrays.toString(e.getValue()))).collect(Collectors.joining(",")));
    explanation.addChild(child);

    return explanation;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    //collection
    if (collection.indexOf(',') > -1) {
      expression.addParameter("\"" + collection + "\"");
    } else {
      expression.addParameter(collection);
    }

    //parameters for q,fl etc
    for (Entry<String, String[]> param : params.getMap().entrySet()) {
      for (String val : param.getValue()) {
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), val));
      }
    }

    //bucket x
    {
      StringBuilder builder = new StringBuilder();

      builder.append(x.toString());
      expression.addParameter(new StreamExpressionNamedParameter("x", builder.toString()));
    }

    //bucket y
    {
      StringBuilder builder = new StringBuilder();

      builder.append(y.toString());
      expression.addParameter(new StreamExpressionNamedParameter("y", builder.toString()));
    }

    //dimensions
    expression.addParameter(new StreamExpressionNamedParameter("dimensions", Integer.toString(dimensionX) + "," + Integer.toString(dimensionY)));

    //metric
    expression.addParameter(metric.toExpression(factory));

    //zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    return expression;
  }

  public void setStreamContext(StreamContext context) {
    cache = context.getSolrClientCache();
  }

  public List<TupleStream> children() {
    return new ArrayList<>();
  }

  public void open() throws IOException {
    if (cache != null) {
      cloudSolrClient = cache.getCloudSolrClient(zkHost);
    } else {
      final List<String> hosts = new ArrayList<>();
      hosts.add(zkHost);
      cloudSolrClient = new Builder(hosts, Optional.empty()).withSocketTimeout(30000).withConnectionTimeout(15000).build();
    }
    FieldComparator[] adjustedSorts = adjustSorts(x, y, bucketSort);

    String json = getJsonFacetString(x, y, metric, adjustedSorts, dimensionX, dimensionY);
    //assert expectedJson(json);

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams(params);
    paramsLoc.set("json.facet", json);
    paramsLoc.set("rows", "0");

    QueryRequest request = new QueryRequest(paramsLoc, SolrRequest.METHOD.POST);
    try {
      @SuppressWarnings({"rawtypes"})
      NamedList response = cloudSolrClient.request(request, collection);
      getTuples(response, x, y, metric);
      this.out = tuples.iterator();

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public Tuple read() throws IOException {
    if (out.hasNext()) {
      return out.next();
    } else {
      Tuple tuple = Tuple.EOF();
      tuple.put("rows", tuples.size());
      return tuple;
    }

  }

  public void close() throws IOException {
    if (cache == null) {
      if (cloudSolrClient != null) {
        cloudSolrClient.close();
      }
    }
  }

  private String getJsonFacetString(Bucket x, Bucket y, Metric metric, FieldComparator[] adjustedSorts, int dimensionX, int dimensionY) {
    StringBuilder buf = new StringBuilder();
    appendJson(buf, x, y, metric, adjustedSorts, dimensionX, dimensionY);

    return "{" + buf.toString() + "}";

  }

  private FieldComparator[] adjustSorts(Bucket x, Bucket y, FieldComparator bucketSort) throws IOException {
    FieldComparator[] adjustSorts = new FieldComparator[2];
    if (bucketSort.getLeftFieldName().contains("(")) {
      for (int i = 0; i < adjustSorts.length; i++) {
        adjustSorts[i] = bucketSort;
      }
    } else {
      adjustSorts[0] = new FieldComparator(x.toString(), bucketSort.getOrder());
      adjustSorts[1] = new FieldComparator(y.toString(), bucketSort.getOrder());

    }
    return adjustSorts;
  }

  private void appendJson(StringBuilder buf, Bucket x, Bucket y, Metric metric, FieldComparator[] adjustedSorts, int dimensionX, int dimensionY) {

    buf.append('"');
    buf.append("x");
    buf.append('"');
    buf.append(":{");
    buf.append("\"type\":\"terms\"");
    buf.append(",\"field\":\"").append(x.toString()).append('"');
    buf.append(",\"limit\":").append(dimensionX);
    buf.append(",\"overrequest\":1000");
    String fsort = getFacetSort(adjustedSorts[0].getLeftFieldName(), metric);
    buf.append(",\"sort\":\"").append(fsort).append(" desc\"");
    buf.append(",\"facet\":{");

    String identifier = metric.getIdentifier();
    if (!identifier.startsWith("count(")) {
      buf.append("\"agg\":\"").append(identifier).append('"');
      buf.append(",");
    }
    buf.append('"');
    buf.append("y");
    buf.append('"');
    buf.append(":{");
    buf.append("\"type\":\"terms\"");
    buf.append(",\"field\":\"").append(y.toString()).append('"');
    buf.append(",\"limit\":").append(dimensionY);
    buf.append(",\"overrequest\":1000");
    String fsortY = getFacetSort(adjustedSorts[1].getLeftFieldName(), metric);
    buf.append(",\"sort\":\"").append(fsortY).append(" desc\"");
    buf.append(",\"facet\":{");
    if (!identifier.startsWith("count(")) {
      buf.append("\"agg\":\"").append(identifier).append('"');
    }
    buf.append("}}}}");
  }

  private String getFacetSort(String id, Metric metric) {
    if (id.startsWith("count(")) {
        return "count";
    } else if (id.equals(metric.getIdentifier())) {
        return "agg";
    }

    return null;
  }

  private void getTuples(@SuppressWarnings({"rawtypes"})NamedList response, Bucket x, Bucket y, Metric metric) {
    Tuple tuple = new Tuple();
    @SuppressWarnings({"rawtypes"})
    NamedList facets = (NamedList) response.get("facets");
    fillTuples(0, tuples, tuple, facets, x, y, metric);
  }

  private void fillTuples(int level, List<Tuple> tuples, Tuple currentTuple,
                          @SuppressWarnings({"rawtypes"})NamedList facets, Bucket x, Bucket y, Metric metric) {
    String bucketXName = x.toString();
    String bucketYName = y.toString();

    @SuppressWarnings({"rawtypes"})
    NamedList allXBuckets = (NamedList) facets.get("x");
    for (int b = 0; b < allXBuckets.size(); b++) {
      @SuppressWarnings({"rawtypes"})
      List buckets = (List) allXBuckets.get("buckets");
      for(int s=0; s<buckets.size(); s++) {

        @SuppressWarnings({"rawtypes"})
        NamedList bucket = (NamedList)buckets.get(s);
        Object val = bucket.get("val");
        if (val instanceof Integer) {
          val = ((Integer) val).longValue();
        }
        Tuple tx = currentTuple.clone();
        tx.put(bucketXName, val);

        @SuppressWarnings({"rawtypes"})
        NamedList allYBuckets = (NamedList) bucket.get("y");
        @SuppressWarnings({"rawtypes"})
        List ybuckets = (List)allYBuckets.get("buckets");

        for (int d = 0; d < ybuckets.size(); d++) {
          @SuppressWarnings({"rawtypes"})
          NamedList bucketY = (NamedList) ybuckets.get(d);
          Object valY = bucketY.get("val");
          if (valY instanceof Integer) {
            valY = ((Integer) valY).longValue();
          }
          Tuple yt = tx.clone();
          yt.put(bucketYName, valY);

          int m = 0;
          String identifier = metric.getIdentifier();
          if (!identifier.startsWith("count(")) {
            Number d1 = (Number) bucketY.get("agg");
            if (metric.outputLong) {
              if (d1 instanceof Long || d1 instanceof Integer) {
                yt.put(identifier, d1.longValue());
              } else {
                yt.put(identifier, Math.round(d1.doubleValue()));
              }
            } else {
              yt.put(identifier, d1.doubleValue());
            }
            ++m;
          } else {
            long l = ((Number)bucketY.get("count")).longValue();
            yt.put("count(*)", l);
          }
          tuples.add(yt);
        }
      }
    }
  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return bucketSort;
  }

  public String getCollection() {
    return collection;
  }


  public Bucket getX() {
    return x;
  }

  public Bucket getY() {
    return y;
  }
}