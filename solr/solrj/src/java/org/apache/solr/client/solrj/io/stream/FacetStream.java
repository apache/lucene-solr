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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
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
import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.ShardParams.SHARDS_PREFERENCE;
import static org.apache.solr.common.params.ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION;

/**
 * The FacetStream abstracts the output from the JSON facet API as a Stream of Tuples. This provides an alternative to the
 * RollupStream which uses Map/Reduce to perform aggregations.
 *
 * @since 6.0.0
 **/

public class FacetStream extends TupleStream implements Expressible, ParallelMetricsRollup {

  private static final long serialVersionUID = 1;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;
  protected transient TupleStream parallelizedStream;
  protected transient StreamContext context;
  private Bucket[] buckets;
  private Metric[] metrics;
  private int rows;
  private int offset;
  private int overfetch;
  private int bucketSizeLimit;
  private boolean refine;
  private String method;
  private FieldComparator[] bucketSorts;
  private List<Tuple> tuples = new ArrayList<Tuple>();
  private int index;
  private String zkHost;
  private ModifiableSolrParams params;
  private String collection;
  private boolean resortNeeded;
  private boolean serializeBucketSizeLimit;

  public FacetStream(String zkHost,
                     String collection,
                     SolrParams params,
                     Bucket[] buckets,
                     Metric[] metrics,
                     FieldComparator[] bucketSorts,
                     int bucketSizeLimit) throws IOException {

    if (bucketSizeLimit == -1) {
      bucketSizeLimit = Integer.MAX_VALUE;
    }
    init(collection, params, buckets, bucketSorts, metrics, bucketSizeLimit, 0, bucketSizeLimit, false, null, true, 0, zkHost);
  }

  public FacetStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);

    if (collectionName.indexOf('"') > -1) {
      collectionName = collectionName.replaceAll("\"", "").replaceAll(" ", "");
    }

    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter bucketExpression = factory.getNamedOperand(expression, "buckets");
    StreamExpressionNamedParameter bucketSortExpression = factory.getNamedOperand(expression, "bucketSorts");
    List<StreamExpression> metricExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, Metric.class);
    StreamExpressionNamedParameter bucketLimitExpression = factory.getNamedOperand(expression, "bucketSizeLimit");
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    StreamExpressionNamedParameter rowsExpression = factory.getNamedOperand(expression, "rows");
    StreamExpressionNamedParameter offsetExpression = factory.getNamedOperand(expression, "offset");
    StreamExpressionNamedParameter overfetchExpression = factory.getNamedOperand(expression, "overfetch");
    StreamExpressionNamedParameter refineExpression = factory.getNamedOperand(expression, "refine");
    StreamExpressionNamedParameter methodExpression = factory.getNamedOperand(expression, "method");

    // Validate there are no unknown parameters
    if (expression.getParameters().size() != 1 + namedParams.size() + metricExpressions.size()) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - unknown operands found", expression));
    }

    // Collection Name
    if (null == collectionName) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - collectionName expected as first operand", expression));
    }

    // Named parameters - passed directly to solr as solrparams
    if (0 == namedParams.size()) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - at least one named parameter expected. eg. 'q=*:*'", expression));
    }

    // pull out known named params
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (StreamExpressionNamedParameter namedParam : namedParams) {
      if (!namedParam.getName().equals("zkHost") &&
          !namedParam.getName().equals("buckets") &&
          !namedParam.getName().equals("bucketSorts") &&
          !namedParam.getName().equals("bucketSizeLimit") &&
          !namedParam.getName().equals("method") &&
          !namedParam.getName().equals("offset") &&
          !namedParam.getName().equals("rows") &&
          !namedParam.getName().equals("refine") &&
          !namedParam.getName().equals("overfetch")) {
        params.add(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    if (params.get("q") == null) {
      params.set("q", "*:*");
    }

    // buckets, required - comma separated
    Bucket[] buckets = null;
    if (null != bucketExpression) {
      if (bucketExpression.getParameter() instanceof StreamExpressionValue) {
        String[] keys = ((StreamExpressionValue) bucketExpression.getParameter()).getValue().split(",");
        if (0 != keys.length) {
          buckets = new Bucket[keys.length];
          for (int idx = 0; idx < keys.length; ++idx) {
            buckets[idx] = new Bucket(keys[idx].trim());
          }
        }
      }
    }

    if (null == buckets) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - at least one bucket expected. eg. 'buckets=\"name\"'", expression, collectionName));
    }


    // Construct the metrics
    Metric[] metrics = new Metric[metricExpressions.size()];
    for (int idx = 0; idx < metricExpressions.size(); ++idx) {
      metrics[idx] = factory.constructMetric(metricExpressions.get(idx));
    }

    if (metrics.length == 0) {
      metrics = new Metric[1];
      metrics[0] = new CountMetric();
    }

    String bucketSortString = null;

    if (bucketSortExpression == null) {
      bucketSortString = metrics[0].getIdentifier() + " desc";
    } else {
      bucketSortString = ((StreamExpressionValue) bucketSortExpression.getParameter()).getValue();
      if (bucketSortString.contains("(") &&
          metricExpressions.size() == 0 &&
          (!bucketSortExpression.equals("count(*) desc") &&
              !bucketSortExpression.equals("count(*) asc"))) {
        //Attempting bucket sort on a metric that is not going to be calculated.
        throw new IOException(String.format(Locale.ROOT, "invalid expression %s - the bucketSort is being performed on a metric that is not being calculated.", expression, collectionName));
      }
    }

    FieldComparator[] bucketSorts = parseBucketSorts(bucketSortString, buckets);

    if (null == bucketSorts || 0 == bucketSorts.length) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - at least one bucket sort expected. eg. 'bucketSorts=\"name asc\"'", expression, collectionName));
    }


    boolean refine = false;

    if (refineExpression != null) {
      String refineStr = ((StreamExpressionValue) refineExpression.getParameter()).getValue();
      if (refineStr != null) {
        refine = Boolean.parseBoolean(refineStr);
      }
    }

    if (bucketLimitExpression != null && (rowsExpression != null ||
        offsetExpression != null ||
        overfetchExpression != null)) {
      throw new IOException("bucketSizeLimit is incompatible with rows, offset and overfetch.");
    }

    String methodStr = null;
    if (methodExpression != null) {
      methodStr = ((StreamExpressionValue) methodExpression.getParameter()).getValue();
    }

    int overfetchInt = 250;
    if (overfetchExpression != null) {
      String overfetchStr = ((StreamExpressionValue) overfetchExpression.getParameter()).getValue();
      overfetchInt = Integer.parseInt(overfetchStr);
    }

    int offsetInt = 0;
    if (offsetExpression != null) {
      String offsetStr = ((StreamExpressionValue) offsetExpression.getParameter()).getValue();
      offsetInt = Integer.parseInt(offsetStr);
    }

    int rowsInt = Integer.MIN_VALUE;
    int bucketLimit = Integer.MIN_VALUE;
    boolean bucketLimitSet = false;

    if (null != rowsExpression) {
      String rowsStr = ((StreamExpressionValue) rowsExpression.getParameter()).getValue();
      try {
        rowsInt = Integer.parseInt(rowsStr);
        if (rowsInt <= 0 && rowsInt != -1) {
          throw new IOException(String.format(Locale.ROOT, "invalid expression %s - limit '%s' must be greater than 0 or -1.", expression, rowsStr));
        }
        //Rows is set so configure the bucketLimitSize
        if (rowsInt == -1) {
          bucketLimit = rowsInt = Integer.MAX_VALUE;
        } else if (overfetchInt == -1) {
          bucketLimit = Integer.MAX_VALUE;
        } else {
          bucketLimit = offsetInt + overfetchInt + rowsInt;
        }
      } catch (NumberFormatException e) {
        throw new IOException(String.format(Locale.ROOT, "invalid expression %s - limit '%s' is not a valid integer.", expression, rowsStr));
      }
    }

    if (bucketLimitExpression != null) {
      String bucketLimitStr = ((StreamExpressionValue) bucketLimitExpression.getParameter()).getValue();
      try {
        bucketLimit = Integer.parseInt(bucketLimitStr);
        bucketLimitSet = true;

        if (bucketLimit <= 0 && bucketLimit != -1) {
          throw new IOException(String.format(Locale.ROOT, "invalid expression %s - bucketSizeLimit '%s' must be greater than 0 or -1.", expression, bucketLimitStr));
        }

        // Bucket limit is set. So set rows.
        if (bucketLimit == -1) {
          rowsInt = bucketLimit = Integer.MAX_VALUE;
        } else {
          rowsInt = bucketLimit;
        }
      } catch (NumberFormatException e) {
        throw new IOException(String.format(Locale.ROOT, "invalid expression %s - bucketSizeLimit '%s' is not a valid integer.", expression, bucketLimitStr));
      }
    }

    if (rowsExpression == null && bucketLimitExpression == null) {
      rowsInt = 10;
      if (overfetchInt == -1) {
        bucketLimit = Integer.MAX_VALUE;
      } else {
        bucketLimit = offsetInt + overfetchInt + rowsInt;
      }
    }

    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if (null == zkHostExpression) {
      zkHost = factory.getCollectionZkHost(collectionName);
      if (zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    } else if (zkHostExpression.getParameter() instanceof StreamExpressionValue) {
      zkHost = ((StreamExpressionValue) zkHostExpression.getParameter()).getValue();
    }

    if (null == zkHost) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - zkHost not found for collection '%s'", expression, collectionName));
    }

    // We've got all the required items
    init(collectionName,
        params,
        buckets,
        bucketSorts,
        metrics,
        rowsInt,
        offsetInt,
        bucketLimit,
        refine,
        methodStr,
        bucketLimitSet,
        overfetchInt,
        zkHost);
  }

  private FacetStream() {
  }

  public int getBucketSizeLimit() {
    return this.bucketSizeLimit;
  }

  public int getRows() {
    return this.rows;
  }

  public int getOffset() {
    return this.offset;
  }

  public int getOverfetch() {
    return this.overfetch;
  }

  public Bucket[] getBuckets() {
    return this.buckets;
  }

  public String getCollection() {
    return this.collection;
  }

  private FieldComparator[] parseBucketSorts(String bucketSortString, Bucket[] buckets) throws IOException {

    String[] sorts = parseSorts(bucketSortString);

    FieldComparator[] comps = new FieldComparator[sorts.length];
    for (int i = 0; i < sorts.length; i++) {
      comps[i] = parseSortClause(sorts[i]);
    }

    return comps;
  }

  private FieldComparator parseSortClause(final String s) throws IOException {
    String fieldName = null;
    String order = null;
    if (s.endsWith("asc") || s.endsWith("ASC")) {
      order = "asc";
      fieldName = s.substring(0, s.length() - 3).trim().replace(" ", "");
    } else if (s.endsWith("desc") || s.endsWith("DESC")) {
      order = "desc";
      fieldName = s.substring(0, s.length() - 4).trim().replace(" ", "");
    } else {
      throw new IOException(String.format(Locale.ROOT, "invalid expression - bad sort caluse '%s'.", s));
    }

    return new FieldComparator(fieldName, order.equalsIgnoreCase("asc") ? ComparatorOrder.ASCENDING : ComparatorOrder.DESCENDING);
  }

  private String[] parseSorts(String sortString) {
    List<String> sorts = new ArrayList<>();
    boolean inParam = false;
    StringBuilder buff = new StringBuilder();
    for (int i = 0; i < sortString.length(); i++) {
      char c = sortString.charAt(i);
      if (c == '(') {
        inParam = true;
        buff.append(c);
      } else if (c == ')') {
        inParam = false;
        buff.append(c);
      } else if (c == ',' && !inParam) {
        sorts.add(buff.toString().trim());
        buff = new StringBuilder();
      } else {
        buff.append(c);
      }
    }

    if (buff.length() > 0) {
      sorts.add(buff.toString());
    }

    return sorts.toArray(new String[sorts.size()]);
  }

  private void init(String collection, SolrParams params, Bucket[] buckets, FieldComparator[] bucketSorts, Metric[] metrics, int rows, int offset, int bucketSizeLimit, boolean refine, String method, boolean serializeBucketSizeLimit, int overfetch, String zkHost) throws IOException {
    this.zkHost = zkHost;
    this.params = new ModifiableSolrParams(params);
    this.buckets = buckets;
    this.metrics = metrics;
    this.rows = rows;
    this.offset = offset;
    this.refine = refine;
    this.bucketSizeLimit = bucketSizeLimit;
    this.collection = collection;
    this.bucketSorts = bucketSorts;
    this.method = method;
    this.serializeBucketSizeLimit = serializeBucketSizeLimit;
    this.overfetch = overfetch;

    // In a facet world it only makes sense to have the same field name in all of the sorters
    // Because FieldComparator allows for left and right field names we will need to validate
    // that they are the same
    for (FieldComparator sort : bucketSorts) {
      if (sort.hasDifferentFieldNames()) {
        throw new IOException("Invalid FacetStream - all sorts must be constructed with a single field name.");
      }
    }
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    // collection
    if (collection.indexOf(',') > -1) {
      expression.addParameter("\"" + collection + "\"");
    } else {
      expression.addParameter(collection);
    }

    // parameters

    for (Entry<String, String[]> param : params.getMap().entrySet()) {
      for (String val : param.getValue()) {
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), val));
      }
    }

    // buckets
    {
      StringBuilder builder = new StringBuilder();
      for (Bucket bucket : buckets) {
        if (0 != builder.length()) {
          builder.append(",");
        }
        builder.append(bucket.toString());
      }
      expression.addParameter(new StreamExpressionNamedParameter("buckets", builder.toString()));
    }

    // bucketSorts
    {
      StringBuilder builder = new StringBuilder();
      for (FieldComparator sort : bucketSorts) {
        if (0 != builder.length()) {
          builder.append(",");
        }
        builder.append(sort.toExpression(factory));
      }
      expression.addParameter(new StreamExpressionNamedParameter("bucketSorts", builder.toString()));
    }

    // metrics
    for (Metric metric : metrics) {
      expression.addParameter(metric.toExpression(factory));
    }

    if (serializeBucketSizeLimit) {
      if (bucketSizeLimit == Integer.MAX_VALUE) {
        expression.addParameter(new StreamExpressionNamedParameter("bucketSizeLimit", Integer.toString(-1)));
      } else {
        expression.addParameter(new StreamExpressionNamedParameter("bucketSizeLimit", Integer.toString(bucketSizeLimit)));
      }
    } else {
      if (rows == Integer.MAX_VALUE) {
        expression.addParameter(new StreamExpressionNamedParameter("rows", Integer.toString(-1)));
      } else {
        expression.addParameter(new StreamExpressionNamedParameter("rows", Integer.toString(rows)));
      }

      expression.addParameter(new StreamExpressionNamedParameter("offset", Integer.toString(offset)));

      if (overfetch == Integer.MAX_VALUE) {
        expression.addParameter(new StreamExpressionNamedParameter("overfetch", Integer.toString(-1)));
      } else {
        expression.addParameter(new StreamExpressionNamedParameter("overfetch", Integer.toString(overfetch)));
      }
    }

    if (method != null) {
      expression.addParameter(new StreamExpressionNamedParameter("method", this.method));
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
    if (cache != null) {
      cloudSolrClient = cache.getCloudSolrClient(zkHost);
    } else {
      final List<String> hosts = new ArrayList<>();
      hosts.add(zkHost);
      cloudSolrClient = new Builder(hosts, Optional.empty()).withSocketTimeout(30000).withConnectionTimeout(15000).build();
    }

    if (params.getBool("plist", true)) {
      params.remove("plist");
      final List<String> resolved = cloudSolrClient.getClusterStateProvider().resolveAlias(collection);
      if (resolved.size() > 1) {
        Optional<TupleStream> maybeParallelize = openParallelStream(context, resolved, metrics);
        if (maybeParallelize.isPresent()) {
          this.parallelizedStream = maybeParallelize.get();
          return; // we're using a plist to parallelize the facet operation
        } // else, there's a metric that we can't rollup over the plist results safely ... no plist for you!
      }
    }

    FieldComparator[] adjustedSorts = adjustSorts(buckets, bucketSorts);
    this.resortNeeded = resortNeeded(adjustedSorts);

    String json = getJsonFacetString(buckets, metrics, adjustedSorts, method, refine, bucketSizeLimit);
    assert expectedJson(json);
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams(params);
    paramsLoc.set("json.facet", json);
    paramsLoc.set("rows", "0");

    // TJP: TODO ~ try not to send the request to the same node if doing a plist

    QueryRequest request = new QueryRequest(paramsLoc, SolrRequest.METHOD.POST);
    try {
      log.info("Sending JSON facet request to collection {} with params {}", collection, paramsLoc);
      @SuppressWarnings({"rawtypes"})
      NamedList response = cloudSolrClient.request(request, collection);
      getTuples(response, buckets, metrics);

      if (resortNeeded) {
        Collections.sort(tuples, getStreamSort());
      }

      index = this.offset;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private boolean expectedJson(String json) {
    if (this.method != null) {
      if (!json.contains("\"method\":\"" + this.method + "\"")) {
        return false;
      }
    }

    if (this.refine) {
      if (!json.contains("\"refine\":true")) {
        return false;
      }
    }

    if (serializeBucketSizeLimit) {
      if (!json.contains("\"limit\":" + bucketSizeLimit)) {
        return false;
      }
    } else {
      if (!json.contains("\"limit\":" + (this.rows + this.offset + this.overfetch))) {
        return false;
      }
    }

    for (Bucket bucket : buckets) {
      if (!json.contains("\"" + bucket.toString() + "\":")) {
        return false;
      }
    }

    for (Metric metric : metrics) {
      String func = metric.getFunctionName();
      if (!func.equals("count") && !func.equals("per") && !func.equals("std")) {
        if (!json.contains(metric.getIdentifier())) {
          return false;
        }
      }
    }

    return true;
  }

  public void close() throws IOException {
    if (cache == null) {
      if (cloudSolrClient != null) {
        cloudSolrClient.close();
      }
    }
  }

  public Tuple read() throws IOException {
    // if we're parallelizing the facet expression over multiple collections with plist,
    // then delegate the read operation to that stream instead
    if (parallelizedStream != null) {
      return parallelizedStream.read();
    }

    if (index < tuples.size() && index < (offset + rows)) {
      Tuple tuple = tuples.get(index);
      ++index;
      return tuple;
    } else {
      Tuple tuple = Tuple.EOF();

      if (bucketSizeLimit == Integer.MAX_VALUE) {
        tuple.put("totalRows", tuples.size());
      }
      return tuple;
    }
  }

  private String getJsonFacetString(Bucket[] _buckets,
                                    Metric[] _metrics,
                                    FieldComparator[] _sorts,
                                    String _method,
                                    boolean _refine,
                                    int _limit) {
    StringBuilder buf = new StringBuilder();
    appendJson(buf, _buckets, _metrics, _sorts, _limit, _method, _refine, 0);
    return "{" + buf.toString() + "}";
  }

  private FieldComparator[] adjustSorts(Bucket[] _buckets, FieldComparator[] _sorts) throws IOException {
    if (_buckets.length == _sorts.length) {
      return _sorts;
    } else if (_sorts.length == 1) {
      FieldComparator[] adjustedSorts = new FieldComparator[_buckets.length];
      if (_sorts[0].getLeftFieldName().contains("(")) {
        //Its a metric sort so apply the same sort criteria at each level.
        for (int i = 0; i < adjustedSorts.length; i++) {
          adjustedSorts[i] = _sorts[0];
        }
      } else {
        //Its an index sort so apply an index sort at each level.
        for (int i = 0; i < adjustedSorts.length; i++) {
          adjustedSorts[i] = new FieldComparator(_buckets[i].toString(), _sorts[0].getOrder());
        }
      }
      return adjustedSorts;
    } else {
      throw new IOException("If multiple sorts are specified there must be a sort for each bucket.");
    }
  }

  private boolean resortNeeded(FieldComparator[] fieldComparators) {
    for (FieldComparator fieldComparator : fieldComparators) {
      if (fieldComparator.getLeftFieldName().contains("(")) {
        return true;
      }
    }
    return false;
  }

  private void appendJson(StringBuilder buf,
                          Bucket[] _buckets,
                          Metric[] _metrics,
                          FieldComparator[] _sorts,
                          int _limit,
                          String method,
                          boolean refine,
                          int level) {
    buf.append('"');
    buf.append(_buckets[level].toString());
    buf.append('"');
    buf.append(":{");
    buf.append("\"type\":\"terms\"");
    buf.append(",\"field\":\"").append(_buckets[level].toString()).append('"');
    buf.append(",\"limit\":").append(_limit);

    if (refine) {
      buf.append(",\"refine\":true");
    }

    if (method != null) {
      buf.append(",\"method\":\"").append(method).append('"');
    }

    String fsort = getFacetSort(_sorts[level].getLeftFieldName(), _metrics);

    buf.append(",\"sort\":{\"").append(fsort).append("\":\"").append(_sorts[level].getOrder()).append("\"}");

    buf.append(",\"facet\":{");
    int metricCount = 0;


    ++level;
    boolean comma = false;
    for (Metric metric : _metrics) {
      //Only compute the metric if it's a leaf node or if the branch level sort equals is the metric
      String facetKey = "facet_" + metricCount;
      String identifier = metric.getIdentifier();
      if (!identifier.startsWith("count(")) {
        if (comma) {
          buf.append(",");
        }

        if (level == _buckets.length || fsort.equals(facetKey)) {
          comma = true;
          if (identifier.startsWith("per(")) {
            buf.append("\"facet_").append(metricCount).append("\":\"").append(identifier.replaceFirst("per", "percentile")).append('"');
          } else if (identifier.startsWith("std(")) {
            buf.append("\"facet_").append(metricCount).append("\":\"").append(identifier.replaceFirst("std", "stddev")).append('"');
          } else {
            buf.append('"').append(facetKey).append("\":\"").append(identifier).append('"');
          }
        }
        ++metricCount;
      }
    }

    if (level < _buckets.length) {
      if (metricCount > 0) {
        buf.append(",");
      }
      appendJson(buf, _buckets, _metrics, _sorts, _limit, method, refine, level);
    }
    buf.append("}}");
  }

  private String getFacetSort(String id, Metric[] _metrics) {
    int index = 0;
    int metricCount = 0;
    for (Metric metric : _metrics) {
      if (metric.getIdentifier().startsWith("count(")) {
        if (id.startsWith("count(")) {
          return "count";
        }
        ++index;
      } else {
        if (id.equals(_metrics[index].getIdentifier())) {
          return "facet_" + metricCount;
        }
        ++index;
        ++metricCount;
      }
    }
    return "index";
  }

  private void getTuples(@SuppressWarnings({"rawtypes"}) NamedList response,
                         Bucket[] buckets,
                         Metric[] metrics) {

    Tuple tuple = new Tuple();
    @SuppressWarnings({"rawtypes"})
    NamedList facets = (NamedList) response.get("facets");
    fillTuples(0,
        tuples,
        tuple,
        facets,
        buckets,
        metrics);

  }

  private void fillTuples(int level,
                          List<Tuple> tuples,
                          Tuple currentTuple,
                          @SuppressWarnings({"rawtypes"}) NamedList facets,
                          Bucket[] _buckets,
                          Metric[] _metrics) {

    String bucketName = _buckets[level].toString();
    @SuppressWarnings({"rawtypes"})
    NamedList nl = (NamedList) facets.get(bucketName);
    if (nl == null) {
      return;
    }
    @SuppressWarnings({"rawtypes"})
    List allBuckets = (List) nl.get("buckets");
    for (int b = 0; b < allBuckets.size(); b++) {
      @SuppressWarnings({"rawtypes"})
      NamedList bucket = (NamedList) allBuckets.get(b);
      Object val = bucket.get("val");
      if (val instanceof Integer) {
        val = ((Integer) val).longValue();  // calcite currently expects Long values here
      }
      Tuple t = currentTuple.clone();
      t.put(bucketName, val);
      int nextLevel = level + 1;
      if (nextLevel < _buckets.length) {
        fillTuples(nextLevel,
            tuples,
            t.clone(),
            bucket,
            _buckets,
            _metrics);
      } else {
        int m = 0;
        for (Metric metric : _metrics) {
          String identifier = metric.getIdentifier();
          if (!identifier.startsWith("count(")) {
            Number d = ((Number) bucket.get("facet_" + m));
            if (metric.outputLong) {
              if (d instanceof Long || d instanceof Integer) {
                t.put(identifier, d.longValue());
              } else {
                t.put(identifier, Math.round(d.doubleValue()));
              }
            } else {
              t.put(identifier, d.doubleValue());
            }
            ++m;
          } else {
            long l = ((Number) bucket.get("count")).longValue();
            t.put("count(*)", l);
          }
        }
        tuples.add(t);
      }
    }
  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return (bucketSorts.length > 1) ? new MultipleFieldComparator(bucketSorts) : bucketSorts[0];
  }

  @Override
  public TupleStream[] parallelize(List<String> partitions) throws IOException {
    final int numPartitions = partitions.size();

    // prefer a different node for each collection if possible as we don't want the same remote node
    // being the coordinator if possible, otherwise, our plist isn't distributing the load as well
    final Set<String> preferredNodes = new HashSet<>(Math.max((int) (numPartitions / .75f) + 1, 16));

    // collect the replicas for each partition in parallel as looking up cluster state for each partition can be slow
    // if state is not cached ...
    final ExecutorService runInParallel =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("FacetStream"));

    List<Future<FacetStream>> facetStreamFutures = partitions.stream().sorted().map(collectionName -> runInParallel.submit(() -> {
      SolrParams params = this.params;

      // if there's a preference already set, we don't look for a preferred server
      if (params.get(SHARDS_PREFERENCE) == null) {
        // careful, getReplicas is slow if not cached ...
        for (Replica r : getReplicas(this.zkHost, collectionName, this.context, this.params)) {
          if (!preferredNodes.contains(r.node)) {
            // we haven't preferred this node yet, so choose it for this collection,
            // other collections in the list will have to prefer another
            preferredNodes.add(r.node);
            // need to make a copy of the params so that we can vary the preferred server per collection
            String preferredServerUrl = ZkCoreNodeProps.getCoreUrl(r.getBaseUrl(), collectionName);
            params = new ModifiableSolrParams(params)
                .set(SHARDS_PREFERENCE, SHARDS_PREFERENCE_REPLICA_LOCATION + ":" + preferredServerUrl);
            break;
          }
        }
      }

      // need to call init with all of this' initialized fields, just changing the collectionName
      FacetStream cloned = new FacetStream();
      cloned.init(collectionName,
          params,
          this.buckets,
          this.bucketSorts,
          this.metrics,
          this.rows,
          this.offset,
          this.bucketSizeLimit,
          this.refine,
          this.method,
          this.serializeBucketSizeLimit,
          this.overfetch,
          zkHost);
      return cloned;
    })).collect(Collectors.toList());

    // collect all the results from the parallel operation
    final List<TupleStream> parallelStreams = new ArrayList<>(numPartitions);
    try {
      for (Future<FacetStream> f : facetStreamFutures) {
        FacetStream fs = f.get();
        if (fs != null) {
          parallelStreams.add(fs);
        }
      }
    } catch (Exception e) {
      Throwable rootCause = ExceptionUtils.getRootCause(e);
      if (rootCause instanceof IOException) {
        throw (IOException)rootCause;
      } else {
        throw new IOException(e);
      }
    } finally {
      runInParallel.shutdown();
    }

    return parallelStreams.toArray(new TupleStream[0]);
  }

  @Override
  public StreamComparator getParallelListSortOrder() throws IOException {
    String sortParam = params.get("sort");
    FieldComparator[] comps;
    if (sortParam != null) {
      comps = parseBucketSorts(sortParam, this.buckets);
    } else {
      comps = new FieldComparator[buckets.length];
      for (int c = 0; c < comps.length; c++) {
        comps[c] = new FieldComparator(buckets[c].toString(), ComparatorOrder.ASCENDING);
      }
    }
    return (comps.length > 1) ? new MultipleFieldComparator(comps) : comps[0];
  }

  @Override
  public RollupStream getRollupStream(SortStream sortStream, Metric[] rollupMetrics) {
    return new RollupStream(sortStream, this.buckets, rollupMetrics);
  }

  @Override
  public Map<String, String> getRollupSelectFields(Metric[] rollupMetrics) {
    Map<String, String> map = new HashMap<>();
    for (Bucket b : buckets) {
      String key = b.toString();
      map.put(key, key);
    }
    for (Metric m : rollupMetrics) {
      String[] cols = m.getColumns();
      String col = cols != null && cols.length > 0 ? cols[0] : "*";
      map.put(m.getIdentifier(), col);
    }
    return map;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("facet(");
    sb.append(collection);
    sb.append(", ").append(paramsAsArgs(Collections.emptyList()));
    sb.append(", buckets=\"").append(Arrays.stream(buckets).map(Bucket::toString).collect(Collectors.joining(", "))).append("\"");
    sb.append(", bucketSorts=\"");
    sb.append(Arrays.stream(bucketSorts).map(fc -> fc.getLeftFieldName() + " " + fc.getOrder()).collect(Collectors.joining(", "))).append("\"");
    sb.append(", bucketSizeLimit=").append(bucketSizeLimit);
    sb.append(", ").append(Arrays.stream(metrics).map(Metric::getIdentifier).collect(Collectors.joining(", ")));
    sb.append(")");
    return sb.toString();
  }

  private String paramsAsArgs(List<String> exclude) {
    StringBuilder sb = new StringBuilder();
    Set<String> paramNames = params.getParameterNames().stream().filter(p -> !exclude.contains(p)).collect(Collectors.toSet());
    for (String name : paramNames) {
      String paramValue = params.get(name);
      if (paramValue != null) {
        if (sb.length() > 0) sb.append(", ");
        sb.append(name).append("=\"").append(paramValue).append("\"");
      }
    }
    return sb.toString();
  }
}
