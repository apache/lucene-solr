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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 *  The FacetStream abstracts the output from the JSON facet API as a Stream of Tuples. This provides an alternative to the
 *  RollupStream which uses Map/Reduce to perform aggregations.
 * @since 6.0.0
 **/

public class FacetStream extends TupleStream implements Expressible  {

  private static final long serialVersionUID = 1;

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

  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;

  public FacetStream(String zkHost,
                     String collection,
                     SolrParams params,
                     Bucket[] buckets,
                     Metric[] metrics,
                     FieldComparator[] bucketSorts,
                     int bucketSizeLimit) throws IOException {

    if(bucketSizeLimit == -1) {
      bucketSizeLimit = Integer.MAX_VALUE;
    }
    init(collection, params, buckets, bucketSorts, metrics, bucketSizeLimit,0, bucketSizeLimit, false, null, true, 0, zkHost);
  }
  
  public FacetStream(StreamExpression expression, StreamFactory factory) throws IOException{   
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);

    if(collectionName.indexOf('"') > -1) {
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
    
    // pull out known named params
    ModifiableSolrParams params = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") &&
          !namedParam.getName().equals("buckets") &&
          !namedParam.getName().equals("bucketSorts") &&
          !namedParam.getName().equals("bucketSizeLimit") &&
          !namedParam.getName().equals("method") &&
          !namedParam.getName().equals("offset") &&
          !namedParam.getName().equals("rows") &&
          !namedParam.getName().equals("refine") &&
          !namedParam.getName().equals("overfetch")){
        params.add(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    // buckets, required - comma separated
    Bucket[] buckets = null;
    if(null != bucketExpression){
      if(bucketExpression.getParameter() instanceof StreamExpressionValue){
        String[] keys = ((StreamExpressionValue)bucketExpression.getParameter()).getValue().split(",");
        if(0 != keys.length){
          buckets = new Bucket[keys.length];
          for(int idx = 0; idx < keys.length; ++idx){
            buckets[idx] = new Bucket(keys[idx].trim());
          }
        }
      }
    }

    if(null == buckets){      
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one bucket expected. eg. 'buckets=\"name\"'",expression,collectionName));
    }

    String bucketSortString = null;

    if(bucketSortExpression == null) {
      throw new IOException("The bucketSorts parameter is required for the facet function.");
    } else {
      bucketSortString = ((StreamExpressionValue)bucketSortExpression.getParameter()).getValue();
    }

    FieldComparator[] bucketSorts = parseBucketSorts(bucketSortString, buckets);

    if(null == bucketSorts || 0 == bucketSorts.length) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one bucket sort expected. eg. 'bucketSorts=\"name asc\"'",expression,collectionName));
    }

    // Construct the metrics
    Metric[] metrics = new Metric[metricExpressions.size()];
    for(int idx = 0; idx < metricExpressions.size(); ++idx) {
      metrics[idx] = factory.constructMetric(metricExpressions.get(idx));
    }

    if(0 == metrics.length) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one metric expected.",expression,collectionName));
    }

    boolean refine = false;

    if(refineExpression != null) {
      String refineStr = ((StreamExpressionValue) refineExpression.getParameter()).getValue();
      if (refineStr != null) {
        refine = Boolean.parseBoolean(refineStr);
      }
    }

    if(bucketLimitExpression != null && (rowsExpression != null ||
                                         offsetExpression != null ||
                                         overfetchExpression != null)) {
      throw new IOException("bucketSizeLimit is incompatible with rows, offset and overfetch.");
    }

    String methodStr = null;
    if(methodExpression != null) {
      methodStr = ((StreamExpressionValue) methodExpression.getParameter()).getValue();
    }

    int overfetchInt = 150;
    if(overfetchExpression != null) {
      String overfetchStr = ((StreamExpressionValue) overfetchExpression.getParameter()).getValue();
      overfetchInt = Integer.parseInt(overfetchStr);
    }

    int offsetInt = 0;
    if(offsetExpression != null) {
      String offsetStr = ((StreamExpressionValue) offsetExpression.getParameter()).getValue();
      offsetInt = Integer.parseInt(offsetStr);
    }

    int rowsInt = Integer.MIN_VALUE;
    int bucketLimit = Integer.MIN_VALUE;
    boolean bucketLimitSet = false;

    if(null != rowsExpression) {
      String rowsStr = ((StreamExpressionValue)rowsExpression.getParameter()).getValue();
      try {
        rowsInt = Integer.parseInt(rowsStr);
        if (rowsInt <= 0 && rowsInt != -1) {
          throw new IOException(String.format(Locale.ROOT, "invalid expression %s - limit '%s' must be greater than 0 or -1.", expression, rowsStr));
        }
        //Rows is set so configure the bucketLimitSize
        if(rowsInt == -1) {
          bucketLimit = rowsInt = Integer.MAX_VALUE;
        } else if(overfetchInt == -1) {
          bucketLimit = Integer.MAX_VALUE;
        }else{
          bucketLimit = offsetInt+overfetchInt+rowsInt;
        }
      } catch (NumberFormatException e) {
        throw new IOException(String.format(Locale.ROOT, "invalid expression %s - limit '%s' is not a valid integer.", expression, rowsStr));
      }
    }

    if(bucketLimitExpression != null) {
      String bucketLimitStr = ((StreamExpressionValue) bucketLimitExpression.getParameter()).getValue();
      try {
        bucketLimit = Integer.parseInt(bucketLimitStr);
        bucketLimitSet = true;

        if (bucketLimit <= 0 && bucketLimit != -1) {
          throw new IOException(String.format(Locale.ROOT, "invalid expression %s - bucketSizeLimit '%s' must be greater than 0 or -1.", expression, bucketLimitStr));
        }

        // Bucket limit is set. So set rows.
        if(bucketLimit == -1) {
         rowsInt = bucketLimit = Integer.MAX_VALUE;
        } else {
          rowsInt = bucketLimit;
        }
      }  catch (NumberFormatException e) {
        throw new IOException(String.format(Locale.ROOT, "invalid expression %s - bucketSizeLimit '%s' is not a valid integer.", expression, bucketLimitStr));
      }
    }

    if(rowsExpression == null && bucketLimitExpression == null) {
      rowsInt = 10;
      if(overfetchInt == -1) {
        bucketLimit = Integer.MAX_VALUE;
      }else{
        bucketLimit = offsetInt+overfetchInt+rowsInt;
      }
    }

    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    } else if(zkHostExpression.getParameter() instanceof StreamExpressionValue) {
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }

    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
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

    String[] sorts = bucketSortString.split(",");
    FieldComparator[] comps = new FieldComparator[sorts.length];
    for(int i=0; i<sorts.length; i++) {
      String s = sorts[i];

      String[] spec = s.trim().split("\\s+"); //This should take into account spaces in the sort spec.
      
      if(2 != spec.length){
        throw new IOException(String.format(Locale.ROOT,"invalid expression - bad bucketSort '%s'. Expected form 'field order'",bucketSortString));
      }
      String fieldName = spec[0].trim();
      String order = spec[1].trim();
            
      comps[i] = new FieldComparator(fieldName, order.equalsIgnoreCase("asc") ? ComparatorOrder.ASCENDING : ComparatorOrder.DESCENDING);
    }

    return comps;
  }

  private void init(String collection, SolrParams params, Bucket[] buckets, FieldComparator[] bucketSorts, Metric[] metrics, int rows, int offset, int bucketSizeLimit, boolean refine, String method, boolean serializeBucketSizeLimit, int overfetch, String zkHost) throws IOException {
    this.zkHost  = zkHost;
    this.params = new ModifiableSolrParams(params);
    this.buckets = buckets;
    this.metrics = metrics;
    this.rows = rows;
    this.offset = offset;
    this.refine = refine;
    this.bucketSizeLimit   = bucketSizeLimit;
    this.collection = collection;
    this.bucketSorts = bucketSorts;
    this.method = method;
    this.serializeBucketSizeLimit = serializeBucketSizeLimit;
    this.overfetch = overfetch;
    
    // In a facet world it only makes sense to have the same field name in all of the sorters
    // Because FieldComparator allows for left and right field names we will need to validate
    // that they are the same
    for(FieldComparator sort : bucketSorts){
      if(sort.hasDifferentFieldNames()){
        throw new IOException("Invalid FacetStream - all sorts must be constructed with a single field name.");
      }
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

    for (Entry<String, String[]> param : params.getMap().entrySet()) {
      for (String val : param.getValue()) {
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), val));
      }
    }
    
    // buckets
    {
      StringBuilder builder = new StringBuilder();
      for(Bucket bucket : buckets){
        if(0 != builder.length()){ builder.append(","); }
        builder.append(bucket.toString());
      }
      expression.addParameter(new StreamExpressionNamedParameter("buckets", builder.toString()));
    }
    
    // bucketSorts
    {
      StringBuilder builder = new StringBuilder();
      for(FieldComparator sort : bucketSorts){
        if(0 != builder.length()){ builder.append(","); }
        builder.append(sort.toExpression(factory));
      }
      expression.addParameter(new StreamExpressionNamedParameter("bucketSorts", builder.toString()));
    }
    
    // metrics
    for(Metric metric : metrics){
      expression.addParameter(metric.toExpression(factory));
    }
    
    if(serializeBucketSizeLimit) {
      if(bucketSizeLimit == Integer.MAX_VALUE) {
        expression.addParameter(new StreamExpressionNamedParameter("bucketSizeLimit", Integer.toString(-1)));
      } else {
        expression.addParameter(new StreamExpressionNamedParameter("bucketSizeLimit", Integer.toString(bucketSizeLimit)));
      }
    } else {
      if (rows == Integer.MAX_VALUE) {
        expression.addParameter(new StreamExpressionNamedParameter("rows", Integer.toString(-1)));
      } else{
        expression.addParameter(new StreamExpressionNamedParameter("rows", Integer.toString(rows)));
      }

      expression.addParameter(new StreamExpressionNamedParameter("offset", Integer.toString(offset)));

      if(overfetch == Integer.MAX_VALUE) {
        expression.addParameter(new StreamExpressionNamedParameter("overfetch", Integer.toString(-1)));
      } else {
        expression.addParameter(new StreamExpressionNamedParameter("overfetch", Integer.toString(overfetch)));
      }
    }

    if(method != null) {
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
    cache = context.getSolrClientCache();
  }

  public List<TupleStream> children() {
    return new ArrayList();
  }

  public void open() throws IOException {
    if(cache != null) {
      cloudSolrClient = cache.getCloudSolrClient(zkHost);
    } else {
      final List<String> hosts = new ArrayList<>();
      hosts.add(zkHost);
      cloudSolrClient = new Builder(hosts, Optional.empty()).withSocketTimeout(30000).withConnectionTimeout(15000).build();
    }

    FieldComparator[] adjustedSorts = adjustSorts(buckets, bucketSorts);
    this.resortNeeded = resortNeeded(adjustedSorts);

    String json = getJsonFacetString(buckets, metrics, adjustedSorts, method, refine, bucketSizeLimit);
    assert expectedJson(json);
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams(params);
    paramsLoc.set("json.facet", json);
    paramsLoc.set("rows", "0");

    QueryRequest request = new QueryRequest(paramsLoc, SolrRequest.METHOD.POST);
    try {
      NamedList response = cloudSolrClient.request(request, collection);
      getTuples(response, buckets, metrics);

      if(resortNeeded) {
        Collections.sort(tuples, getStreamSort());
      }

      index=this.offset;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private boolean expectedJson(String json) {
    if(this.method != null) {
      if(!json.contains("\"method\":\""+this.method+"\"")) {
        return false;
      }
    }

    if(this.refine) {
      if(!json.contains("\"refine\":true")) {
        return false;
      }
    }

    if(serializeBucketSizeLimit) {
      if(!json.contains("\"limit\":"+bucketSizeLimit)) {
        return false;
      }
    } else {
      if(!json.contains("\"limit\":"+(this.rows+this.offset+this.overfetch))) {
        return false;
      }
    }

    for(Bucket bucket : buckets) {
      if(!json.contains("\""+bucket.toString()+"\":")) {
        return false;
      }
    }

    for(Metric metric: metrics) {
      String func = metric.getFunctionName();
      if(!func.equals("count")) {
        if (!json.contains(metric.getIdentifier())) {
          return false;
        }
      }
    }

    return true;
  }

  public void close() throws IOException {
    if(cache == null) {
      if (cloudSolrClient != null) {
        cloudSolrClient.close();
      }
    }
  }

  public Tuple read() throws IOException {
    if(index < tuples.size() && index < (offset+rows)) {
      Tuple tuple = tuples.get(index);
      ++index;
      return tuple;
    } else {
      Map fields = new HashMap();

      if(bucketSizeLimit == Integer.MAX_VALUE) {
        fields.put("totalRows", tuples.size());
      }

      fields.put("EOF", true);

      Tuple tuple = new Tuple(fields);
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
    appendJson(buf, _buckets, _metrics, _sorts, _limit, _method, _refine,0);
    return "{"+buf.toString()+"}";
  }

  private FieldComparator[] adjustSorts(Bucket[] _buckets, FieldComparator[] _sorts) throws IOException {
    if(_buckets.length == _sorts.length) {
      return _sorts;
    } else if(_sorts.length == 1) {
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
    for(FieldComparator fieldComparator : fieldComparators) {
      if(fieldComparator.getLeftFieldName().contains("(")) {
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

    if(refine) {
      buf.append(",\"refine\":true");
    }

    if(method != null) {
      buf.append(",\"method\":\"").append(method).append('"');
    }

    String fsort = getFacetSort(_sorts[level].getLeftFieldName(), _metrics);

    buf.append(",\"sort\":{\"").append(fsort).append("\":\"").append(_sorts[level].getOrder()).append("\"}");

    buf.append(",\"facet\":{");
    int metricCount = 0;


    ++level;
    for(Metric metric : _metrics) {
      //Only compute the metric if it's a leaf node or if the branch level sort equals is the metric
      String facetKey = "facet_"+metricCount;
      if(level == _buckets.length || fsort.equals(facetKey) ) {
        String identifier = metric.getIdentifier();
        if (!identifier.startsWith("count(")) {
          if (metricCount > 0) {
            buf.append(",");
          }
          buf.append('"').append(facetKey).append("\":\"").append(identifier).append('"');
          ++metricCount;
        }
      }
    }

    if(level < _buckets.length) {
      if(metricCount>0) {
        buf.append(",");
      }
      appendJson(buf, _buckets, _metrics, _sorts, _limit, method, refine, level);
    }
    buf.append("}}");
  }

  private String getFacetSort(String id, Metric[] _metrics) {
    int index = 0;
    int metricCount=0;
    for(Metric metric : _metrics) {
      if(metric.getIdentifier().startsWith("count(")) {
        if(id.startsWith("count(")) {
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

  private void getTuples(NamedList response,
                                Bucket[] buckets,
                                Metric[] metrics) {

    Tuple tuple = new Tuple(new HashMap());
    NamedList facets = (NamedList)response.get("facets");
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
                          NamedList facets,
                          Bucket[] _buckets,
                          Metric[] _metrics) {

    String bucketName = _buckets[level].toString();
    NamedList nl = (NamedList)facets.get(bucketName);
    if(nl == null) {
      return;
    }
    List allBuckets = (List)nl.get("buckets");
    for(int b=0; b<allBuckets.size(); b++) {
      NamedList bucket = (NamedList)allBuckets.get(b);
      Object val = bucket.get("val");
      if (val instanceof Integer) {
        val=((Integer)val).longValue();  // calcite currently expects Long values here
      }
      Tuple t = currentTuple.clone();
      t.put(bucketName, val);
      int nextLevel = level+1;
      if(nextLevel<_buckets.length) {
        fillTuples(nextLevel,
                   tuples,
                   t.clone(),
                   bucket,
                   _buckets,
                   _metrics);
      } else {
        int m = 0;
        for(Metric metric : _metrics) {
          String identifier = metric.getIdentifier();
          if(!identifier.startsWith("count(")) {
            Number d = ((Number)bucket.get("facet_"+m));
            if(metric.outputLong) {
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
            long l = ((Number)bucket.get("count")).longValue();
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
    if(bucketSorts.length > 1) {
      return new MultipleFieldComparator(bucketSorts);
    } else {
      return bucketSorts[0];
    }
  }
}
