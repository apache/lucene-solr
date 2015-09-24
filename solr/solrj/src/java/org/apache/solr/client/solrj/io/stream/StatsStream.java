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
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

public class StatsStream extends TupleStream  {

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
    this.zkHost  = zkHost;
    this.props   = props;
    this.metrics = metrics;
    this.collection = collection;
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