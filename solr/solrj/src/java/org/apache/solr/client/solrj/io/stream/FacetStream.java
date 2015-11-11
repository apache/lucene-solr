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
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

/**
 *  The FacetStream abstracts the output from the JSON facet API as a Stream of Tuples. This provides an alternative to the
 *  RollupStream which uses Map/Reduce to perform aggregations.
 **/

public class FacetStream extends TupleStream  {

  private static final long serialVersionUID = 1;

  private Bucket[] buckets;
  private Metric[] metrics;
  private int limit;
  private FieldComparator[] sorts;
  private List<Tuple> tuples = new ArrayList();
  private int index;
  private String zkHost;
  private Map<String, String> props;
  private String collection;
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;

  public FacetStream(String zkHost,
                     String collection,
                     Map<String, String> props,
                     Bucket[] buckets,
                     Metric[] metrics,
                     FieldComparator[] sorts,
                     int limit) throws IOException {
    this.zkHost  = zkHost;
    this.props   = props;
    this.buckets = buckets;
    this.metrics = metrics;
    this.limit   = limit;
    this.collection = collection;
    this.sorts = sorts;
    
    // In a facet world it only makes sense to have the same field name in all of the sorters
    // Because FieldComparator allows for left and right field names we will need to validate
    // that they are the same
    for(FieldComparator sort : sorts){
      if(sort.hasDifferentFieldNames()){
        throw new IOException("Invalid FacetStream - all sorts must be constructed with a single field name.");
      }
    }
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

    FieldComparator[] adjustedSorts = adjustSorts(buckets, sorts);
    String json = getJsonFacetString(buckets, metrics, adjustedSorts, limit);

    ModifiableSolrParams params = getParams(this.props);
    params.add("json.facet", json);
    params.add("rows", "0");

    QueryRequest request = new QueryRequest(params);
    try {
      NamedList response = cloudSolrClient.request(request, collection);
      getTuples(response, buckets, metrics);
      Collections.sort(tuples, getStreamSort());
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
    if(index < tuples.size() && index < limit) {
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

  private ModifiableSolrParams getParams(Map<String, String> props) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    for(String key : props.keySet()) {
      String value = props.get(key);
      params.add(key, value);
    }
    return params;
  }

  private String getJsonFacetString(Bucket[] _buckets, Metric[] _metrics, FieldComparator[] _sorts, int _limit) {
    StringBuilder buf = new StringBuilder();
    appendJson(buf, _buckets, _metrics, _sorts, _limit, 0);
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

  private void appendJson(StringBuilder buf,
                          Bucket[] _buckets,
                          Metric[] _metrics,
                          FieldComparator[] _sorts,
                          int _limit,
                          int level) {
    buf.append('"');
    buf.append(_buckets[level].toString());
    buf.append('"');
    buf.append(":{");
    buf.append("\"type\":\"terms\"");
    buf.append(",\"field\":\""+_buckets[level].toString()+"\"");
    buf.append(",\"limit\":"+_limit);
    buf.append(",\"sort\":{\""+getFacetSort(_sorts[level].getLeftFieldName(), _metrics)+"\":\""+_sorts[level].getOrder()+"\"}");

    buf.append(",\"facet\":{");
    int metricCount = 0;
    for(Metric metric : _metrics) {
      String identifier = metric.getIdentifier();
      if(!identifier.startsWith("count(")) {
        if(metricCount>0) {
          buf.append(",");
        }
        buf.append("\"facet_" + metricCount + "\":\"" +identifier+"\"");
        ++metricCount;
      }
    }
    ++level;
    if(level < _buckets.length) {
      if(metricCount>0) {
        buf.append(",");
      }
      appendJson(buf, _buckets, _metrics, _sorts, _limit, level);
    }
    buf.append("}}");
  }

  private String getFacetSort(String id, Metric[] _metrics) {
    int index = 0;
    for(Metric metric : _metrics) {
      if(metric.getIdentifier().startsWith("count(")) {
        if(id.startsWith("count(")) {
          return "count";
        }
      } else {
        if (id.equals(_metrics[index].getIdentifier())) {
          return "facet_" + index;
        }
        ++index;
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
    List allBuckets = (List)nl.get("buckets");
    for(int b=0; b<allBuckets.size(); b++) {
      NamedList bucket = (NamedList)allBuckets.get(b);
      Object val = bucket.get("val");
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
            double d = (double)bucket.get("facet_"+m);
            t.put(identifier, d);
            ++m;
          } else {
            long l = (long)bucket.get("count");
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
    if(sorts.length > 1) {
      return new MultipleFieldComparator(sorts);
    } else {
      return sorts[0];
    }
  }
}