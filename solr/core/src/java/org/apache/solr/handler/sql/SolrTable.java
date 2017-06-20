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
package org.apache.solr.handler.sql;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.Pair;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.eval.AndEvaluator;
import org.apache.solr.client.solrj.io.eval.BooleanEvaluator;
import org.apache.solr.client.solrj.io.eval.EqualsEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEvaluator;
import org.apache.solr.client.solrj.io.eval.LessThanEqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.LessThanEvaluator;
import org.apache.solr.client.solrj.io.eval.NotEvaluator;
import org.apache.solr.client.solrj.io.eval.OrEvaluator;
import org.apache.solr.client.solrj.io.eval.RawValueEvaluator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.StreamHandler;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import static org.apache.solr.common.params.CommonParams.SORT;

/**
 * Table based on a Solr collection
 */
class SolrTable extends AbstractQueryableTable implements TranslatableTable {
  private static final String DEFAULT_QUERY = "*:*";

  private final String collection;
  private final SolrSchema schema;
  private RelProtoDataType protoRowType;

  SolrTable(SolrSchema schema, String collection) {
    super(Object[].class);
    this.schema = schema;
    this.collection = collection;
  }

  public String toString() {
    return "SolrTable {" + collection + "}";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType == null) {
      protoRowType = schema.getRelDataType(collection);
    }
    return protoRowType.apply(typeFactory);
  }
  
  private Enumerable<Object> query(final Properties properties) {
    return query(properties, Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList(), null, null, null);
  }

  /** Executes a Solr query on the underlying table.
   *
   * @param properties Connections properties
   * @param fields List of fields to project
   * @param query A string for the query
   * @return Enumerator of results
   */
  private Enumerable<Object> query(final Properties properties,
                                   final List<Map.Entry<String, Class>> fields,
                                   final String query,
                                   final List<Pair<String, String>> orders,
                                   final List<String> buckets,
                                   final List<Pair<String, String>> metricPairs,
                                   final String limit,
                                   final String negativeQuery,
                                   final String havingPredicate) {
    // SolrParams should be a ModifiableParams instead of a map
    boolean mapReduce = "map_reduce".equals(properties.getProperty("aggregationMode"));
    boolean negative = Boolean.parseBoolean(negativeQuery);

    String q = null;

    if (query == null) {
      q = DEFAULT_QUERY;
    } else {
      if(negative) {
        q = DEFAULT_QUERY + " AND " + query;
      } else {
        q = query;
      }
    }

    TupleStream tupleStream;
    String zk = properties.getProperty("zk");
    try {
      if (metricPairs.isEmpty() && buckets.isEmpty()) {
        tupleStream = handleSelect(zk, collection, q, fields, orders, limit);
      } else {
        if(buckets.isEmpty()) {
          tupleStream = handleStats(zk, collection, q, metricPairs, fields);
        } else {
          if(mapReduce) {
            tupleStream = handleGroupByMapReduce(zk,
                                                 collection,
                                                 properties,
                                                 fields,
                                                 q,
                                                 orders,
                                                 buckets,
                                                 metricPairs,
                                                 limit,
                                                 havingPredicate);
          } else {
            tupleStream = handleGroupByFacet(zk,
                                             collection,
                                             fields,
                                             q,
                                             orders,
                                             buckets,
                                             metricPairs,
                                             limit,
                                             havingPredicate);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    StreamContext streamContext = new StreamContext();
    streamContext.setSolrClientCache(StreamHandler.getClientCache());
    tupleStream.setStreamContext(streamContext);

    final TupleStream finalStream = tupleStream;

    return new AbstractEnumerable<Object>() {
      // Use original fields list to make sure only the fields specified are enumerated
      public Enumerator<Object> enumerator() {
        return new SolrEnumerator(finalStream, fields);
      }
    };
  }

  private static StreamComparator bucketSortComp(List<Bucket> buckets, Map<String,String> dirs) {
    FieldComparator[] comps = new FieldComparator[buckets.size()];
    for(int i=0; i<buckets.size(); i++) {
      ComparatorOrder comparatorOrder = ComparatorOrder.fromString(dirs.get(buckets.get(i).toString()));
      String sortKey = buckets.get(i).toString();
      comps[i] = new FieldComparator(sortKey, comparatorOrder);
    }

    if(comps.length == 1) {
      return comps[0];
    } else {
      return new MultipleFieldComparator(comps);
    }
  }

  private static StreamComparator bucketSortComp(Bucket[] buckets, String dir) {
    FieldComparator[] comps = new FieldComparator[buckets.length];
    for(int i=0; i<buckets.length; i++) {
      ComparatorOrder comparatorOrder = ascDescComp(dir);
      String sortKey = buckets[i].toString();
      comps[i] = new FieldComparator(sortKey, comparatorOrder);
    }

    if(comps.length == 1) {
      return comps[0];
    } else {
      return new MultipleFieldComparator(comps);
    }
  }

  private String getSortDirection(Map.Entry<String, String> order) {
    String direction = order.getValue();
    return direction == null ? "asc" : direction;
  }

  private StreamComparator getComp(List<? extends Map.Entry<String, String>> orders) {
    FieldComparator[] comps = new FieldComparator[orders.size()];
    for(int i = 0; i < orders.size(); i++) {
      Map.Entry<String, String> order = orders.get(i);
      String direction = getSortDirection(order);
      ComparatorOrder comparatorOrder = ComparatorOrder.fromString(direction);
      String sortKey = order.getKey();
      comps[i] = new FieldComparator(sortKey, comparatorOrder);
    }

    if(comps.length == 1) {
      return comps[0];
    } else {
      return new MultipleFieldComparator(comps);
    }
  }

  private List<Metric> buildMetrics(List<Pair<String, String>> metricPairs, boolean ifEmptyCount) {
    List<Metric> metrics = new ArrayList<>(metricPairs.size());
    metrics.addAll(metricPairs.stream().map(this::getMetric).collect(Collectors.toList()));
    if(metrics.size() == 0 && ifEmptyCount) {
      metrics.add(new CountMetric());
    }
    return metrics;
  }

  private Metric getMetric(Pair<String, String> metricPair) {
    switch (metricPair.getKey()) {
      case "COUNT":
        return new CountMetric(metricPair.getValue());
      case "SUM":
      case "$SUM0":
        return new SumMetric(metricPair.getValue());
      case "MIN":
        return new MinMetric(metricPair.getValue());
      case "MAX":
        return new MaxMetric(metricPair.getValue());
      case "AVG":
        return new MeanMetric(metricPair.getValue());
      default:
        throw new IllegalArgumentException(metricPair.getKey());
    }
  }

  private TupleStream handleSelect(String zk,
                                   String collection,
                                   String query,
                                   List<Map.Entry<String, Class>> fields,
                                   List<Pair<String, String>> orders,
                                   String limit) throws IOException {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, query);

    //Validate the fields
    for(Map.Entry<String, Class> entry : fields) {
      String fname = entry.getKey();
      if(limit == null && "score".equals(fname)) {
        throw new IOException("score is not a valid field for unlimited queries.");
      }

      if(fname.contains("*")) {
        throw new IOException("* is not supported for column selection.");
      }
    }

    String fl = getFields(fields);

    if(orders.size() > 0) {
      params.add(SORT, getSort(orders));
    } else {
      if(limit == null) {
        params.add(SORT, "_version_ desc");
        fl = fl+",_version_";
      } else {
        params.add(SORT, "score desc");
        if(fl.indexOf("score") == -1) {
          fl = fl + ",score";
        }
      }
    }

    params.add(CommonParams.FL, fl);

    if (limit != null) {
      params.add(CommonParams.ROWS, limit);
      return new LimitStream(new CloudSolrStream(zk, collection, params), Integer.parseInt(limit));
    } else {
      params.add(CommonParams.QT, "/export");
      return new CloudSolrStream(zk, collection, params);
    }
  }

  private String getSort(List<Pair<String, String>> orders) {
    StringBuilder buf = new StringBuilder();
    for(Pair<String, String> pair : orders) {
      if(buf.length() > 0) {
        buf.append(",");
      }
      buf.append(pair.getKey()).append(" ").append(pair.getValue());
    }

    return buf.toString();
  }

  private String getSingleSort(Pair<String, String> order) {
    StringBuilder buf = new StringBuilder();
    buf.append(order.getKey()).append(" ").append(order.getValue());
    return buf.toString();
  }

  private String getFields(List<Map.Entry<String, Class>> fields) {
    StringBuilder buf = new StringBuilder();
    for(Map.Entry<String, Class> field : fields) {

      if(buf.length() > 0) {
        buf.append(",");
      }

      buf.append(field.getKey());
    }

    return buf.toString();
  }

  private String getFields(Set<String> fieldSet) {
    StringBuilder buf = new StringBuilder();
    for(String field : fieldSet) {

      if(buf.length() > 0) {
        buf.append(",");
      }


      buf.append(field);
    }

    return buf.toString();
  }


  private Set<String> getFieldSet(Metric[] metrics, List<Map.Entry<String, Class>> fields) {
    HashSet set = new HashSet();
    for(Metric metric : metrics) {
      for(String column : metric.getColumns()) {
        set.add(column);
      }
    }

    for(Map.Entry<String, Class> field : fields) {
      if(field.getKey().indexOf('(') == -1) {
        set.add(field.getKey());
      }
    }

    return set;
  }

  private static String getSortDirection(List<Pair<String, String>> orders) {
    if(orders != null && orders.size() > 0) {
      for(Pair<String,String> item : orders) {
        return item.getValue();
      }
    }

    return "asc";
  }

  private static String bucketSort(Bucket[] buckets, String dir) {
    StringBuilder buf = new StringBuilder();
    boolean comma = false;
    for(Bucket bucket : buckets) {
      if(comma) {
        buf.append(",");
      }
      buf.append(bucket.toString()).append(" ").append(dir);
      comma = true;
    }

    return buf.toString();
  }

  private static String getPartitionKeys(Bucket[] buckets) {
    StringBuilder buf = new StringBuilder();
    boolean comma = false;
    for(Bucket bucket : buckets) {
      if(comma) {
        buf.append(",");
      }
      buf.append(bucket.toString());
      comma = true;
    }
    return buf.toString();
  }

  private static boolean sortsEqual(Bucket[] buckets, String direction, List<Pair<String, String>> orders) {

    if(buckets.length != orders.size()) {
      return false;
    }

    for(int i=0; i< buckets.length; i++) {
      Bucket bucket = buckets[i];
      Pair<String, String> order = orders.get(i);
      if(!bucket.toString().equals(order.getKey())) {
        return false;
      }

      if(!order.getValue().toLowerCase(Locale.ROOT).contains(direction.toLowerCase(Locale.ROOT))) {
        return false;
      }
    }

    return true;
  }

  private TupleStream handleGroupByMapReduce(String zk,
                                             String collection,
                                             Properties properties,
                                             final List<Map.Entry<String, Class>> fields,
                                             final String query,
                                             final List<Pair<String, String>> orders,
                                             final List<String> _buckets,
                                             final List<Pair<String, String>> metricPairs,
                                             final String limit,
                                             final String havingPredicate) throws IOException {

    Map<String, Class> fmap = new HashMap();
    for(Map.Entry<String, Class> entry : fields) {
      fmap.put(entry.getKey(), entry.getValue());
    }

    int numWorkers = Integer.parseInt(properties.getProperty("numWorkers", "1"));

    Bucket[] buckets = buildBuckets(_buckets, fields);
    Metric[] metrics = buildMetrics(metricPairs, false).toArray(new Metric[0]);

    if(metrics.length == 0) {
      return handleSelectDistinctMapReduce(zk, collection, properties, fields, query, orders, buckets, limit);
    } else {
      for(Metric metric : metrics) {
        Class c = fmap.get(metric.getIdentifier());
        if(Long.class.equals(c)) {
          metric.outputLong = true;
        }
      }
    }

    Set<String> fieldSet = getFieldSet(metrics, fields);

    if(metrics.length == 0) {
      throw new IOException("Group by queries must include atleast one aggregate function.");
    }

    String fl = getFields(fieldSet);
    String sortDirection = getSortDirection(orders);
    String sort = bucketSort(buckets, sortDirection);

    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CommonParams.FL, fl);
    params.set(CommonParams.Q, query);
    params.set(CommonParams.WT, CommonParams.JAVABIN);
    //Always use the /export handler for Group By Queries because it requires exporting full result sets.
    params.set(CommonParams.QT, "/export");

    if(numWorkers > 1) {
      params.set("partitionKeys", getPartitionKeys(buckets));
    }

    params.set(SORT, sort);

    TupleStream tupleStream = null;

    CloudSolrStream cstream = new CloudSolrStream(zk, collection, params);
    tupleStream = new RollupStream(cstream, buckets, metrics);

    StreamFactory factory = new StreamFactory()
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("rollup", RollupStream.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("and", AndEvaluator.class)
        .withFunctionName("or", OrEvaluator.class)
        .withFunctionName("not", NotEvaluator.class)
        .withFunctionName("eq", EqualsEvaluator.class)
        .withFunctionName("gt", GreaterThanEvaluator.class)
        .withFunctionName("lt", LessThanEvaluator.class)
        .withFunctionName("val", RawValueEvaluator.class)
        .withFunctionName("lteq", LessThanEqualToEvaluator.class)
        .withFunctionName("having", HavingStream.class)
        .withFunctionName("gteq", GreaterThanEqualToEvaluator.class);

    if(havingPredicate != null) {
      BooleanEvaluator booleanOperation = (BooleanEvaluator)factory.constructEvaluator(StreamExpressionParser.parse(havingPredicate));
      tupleStream = new HavingStream(tupleStream, booleanOperation);
    }

    if(numWorkers > 1) {
      // Do the rollups in parallel
      // Maintain the sort of the Tuples coming from the workers.
      StreamComparator comp = bucketSortComp(buckets, sortDirection);
      ParallelStream parallelStream = new ParallelStream(zk, collection, tupleStream, numWorkers, comp);


      parallelStream.setStreamFactory(factory);
      tupleStream = parallelStream;
    }

    //TODO: Currently we are not pushing down the having clause.
    //      We need to push down the having clause to ensure that LIMIT does not cut off records prior to the having filter.

    if(orders != null && orders.size() > 0) {
      if(!sortsEqual(buckets, sortDirection, orders)) {
        int lim = (limit == null) ? 100 : Integer.parseInt(limit);
        StreamComparator comp = getComp(orders);
        //Rank the Tuples
        //If parallel stream is used ALL the Rolled up tuples from the workers will be ranked
        //Providing a true Top or Bottom.
        tupleStream = new RankStream(tupleStream, lim, comp);
      } else {
        // Sort is the same as the same as the underlying stream
        // Only need to limit the result, not Rank the result
        if(limit != null) {
          tupleStream = new LimitStream(tupleStream, Integer.parseInt(limit));
        }
      }
    } else {
      //No order by, check for limit
      if(limit != null) {
        tupleStream = new LimitStream(tupleStream, Integer.parseInt(limit));
      }
    }

    return tupleStream;
  }

  private Bucket[] buildBuckets(List<String> buckets, List<Map.Entry<String, Class>> fields) {
    Bucket[] bucketsArray = new Bucket[buckets.size()];

    int i=0;
    for(Map.Entry<String,Class> field : fields) {
      String fieldName = field.getKey();
      if(buckets.contains(fieldName)) {
        bucketsArray[i++] = new Bucket(fieldName);
      }
    }

    return bucketsArray;
  }

  private TupleStream handleGroupByFacet(String zkHost,
                                         String collection,
                                         final List<Map.Entry<String, Class>> fields,
                                         final String query,
                                         final List<Pair<String, String>> orders,
                                         final List<String> bucketFields,
                                         final List<Pair<String, String>> metricPairs,
                                         final String lim,
                                         final String havingPredicate) throws IOException {


    Map<String, Class> fmap = new HashMap();
    for(Map.Entry<String, Class> f : fields) {
      fmap.put(f.getKey(), f.getValue());
    }

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add(CommonParams.Q, query);

    Bucket[] buckets = buildBuckets(bucketFields, fields);
    Metric[] metrics = buildMetrics(metricPairs, true).toArray(new Metric[0]);
    if(metrics.length == 0) {
      metrics = new Metric[1];
      metrics[0] = new CountMetric();
    } else {
      for(Metric metric : metrics) {
        Class c = fmap.get(metric.getIdentifier());
        if(Long.class.equals(c)) {
          metric.outputLong = true;
        }
      }
    }

    int limit = lim != null ? Integer.parseInt(lim) : 1000;

    FieldComparator[] sorts = null;

    if(orders == null || orders.size() == 0) {
      sorts = new FieldComparator[buckets.length];
      for(int i=0; i<sorts.length; i++) {
        sorts[i] = new FieldComparator("index", ComparatorOrder.ASCENDING);
      }
    } else {
      sorts = getComps(orders);
    }

    int overfetch = (int)(limit * 1.25);

    TupleStream tupleStream = new FacetStream(zkHost,
                                              collection,
                                              solrParams,
                                              buckets,
                                              metrics,
                                              sorts,
                                              overfetch);



    StreamFactory factory = new StreamFactory()
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("rollup", RollupStream.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("and", AndEvaluator.class)
        .withFunctionName("or", OrEvaluator.class)
        .withFunctionName("not", NotEvaluator.class)
        .withFunctionName("eq", EqualsEvaluator.class)
        .withFunctionName("val", RawValueEvaluator.class)
        .withFunctionName("gt", GreaterThanEvaluator.class)
        .withFunctionName("lt", LessThanEvaluator.class)
        .withFunctionName("lteq", LessThanEqualToEvaluator.class)
        .withFunctionName("gteq", GreaterThanEqualToEvaluator.class);

    if(havingPredicate != null) {
      BooleanEvaluator booleanOperation = (BooleanEvaluator)factory.constructEvaluator(StreamExpressionParser.parse(havingPredicate));
      tupleStream = new HavingStream(tupleStream, booleanOperation);
    }

    if(lim != null)
    {
      tupleStream = new LimitStream(tupleStream, limit);
    }

    return tupleStream;
  }

  private TupleStream handleSelectDistinctMapReduce(final String zkHost,
                                                    final String collection,
                                                    final Properties properties,
                                                    final List<Map.Entry<String, Class>> fields,
                                                    final String query,
                                                    final List<Pair<String, String>> orders,
                                                    final Bucket[] buckets,
                                                    final String limit) throws IOException{

    int numWorkers = Integer.parseInt(properties.getProperty("numWorkers", "1"));

    String fl = getFields(fields);

    String sort = null;
    StreamEqualitor ecomp = null;
    StreamComparator comp = null;

    if(orders != null && orders.size() > 0) {
      StreamComparator[] adjustedSorts = adjustSorts(orders, buckets);
      // Because of the way adjustSorts works we know that each FieldComparator has a single
      // field name. For this reason we can just look at the leftFieldName
      FieldEqualitor[] fieldEqualitors = new FieldEqualitor[adjustedSorts.length];
      StringBuilder buf = new StringBuilder();
      for(int i=0; i<adjustedSorts.length; i++) {
        FieldComparator fieldComparator = (FieldComparator)adjustedSorts[i];
        fieldEqualitors[i] = new FieldEqualitor(fieldComparator.getLeftFieldName());
        if(i>0) {
          buf.append(",");
        }
        buf.append(fieldComparator.getLeftFieldName()).append(" ").append(fieldComparator.getOrder().toString());
      }

      sort = buf.toString();

      if(adjustedSorts.length == 1) {
        ecomp = fieldEqualitors[0];
        comp = adjustedSorts[0];
      } else {
        ecomp = new MultipleFieldEqualitor(fieldEqualitors);
        comp = new MultipleFieldComparator(adjustedSorts);
      }
    } else {
      StringBuilder sortBuf = new StringBuilder();
      FieldEqualitor[] equalitors = new FieldEqualitor[buckets.length];
      StreamComparator[] streamComparators = new StreamComparator[buckets.length];
      for(int i=0; i<buckets.length; i++) {
        equalitors[i] = new FieldEqualitor(buckets[i].toString());
        streamComparators[i] = new FieldComparator(buckets[i].toString(), ComparatorOrder.ASCENDING);
        if(i>0) {
          sortBuf.append(',');
        }
        sortBuf.append(buckets[i].toString()).append(" asc");
      }

      sort = sortBuf.toString();

      if(equalitors.length == 1) {
        ecomp = equalitors[0];
        comp = streamComparators[0];
      } else {
        ecomp = new MultipleFieldEqualitor(equalitors);
        comp = new MultipleFieldComparator(streamComparators);
      }
    }

    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CommonParams.FL, fl);
    params.set(CommonParams.Q, query);
    params.set(CommonParams.WT, CommonParams.JAVABIN);
    //Always use the /export handler for Distinct Queries because it requires exporting full result sets.
    params.set(CommonParams.QT, "/export");

    if(numWorkers > 1) {
      params.set("partitionKeys", getPartitionKeys(buckets));
    }

    params.set(SORT, sort);

    TupleStream tupleStream = null;

    CloudSolrStream cstream = new CloudSolrStream(zkHost, collection, params);
    tupleStream = new UniqueStream(cstream, ecomp);

    if(numWorkers > 1) {
      // Do the unique in parallel
      // Maintain the sort of the Tuples coming from the workers.
      ParallelStream parallelStream = new ParallelStream(zkHost, collection, tupleStream, numWorkers, comp);

      StreamFactory factory = new StreamFactory()
          .withFunctionName("search", CloudSolrStream.class)
          .withFunctionName("parallel", ParallelStream.class)
          .withFunctionName("unique", UniqueStream.class);

      parallelStream.setStreamFactory(factory);
      tupleStream = parallelStream;
    }

    if(limit != null) {
      tupleStream = new LimitStream(tupleStream, Integer.parseInt(limit));
    }

    return tupleStream;
  }


  private StreamComparator[] adjustSorts(List<Pair<String, String>> orders, Bucket[] buckets) throws IOException {
    List<FieldComparator> adjustedSorts = new ArrayList();
    Set<String> bucketFields = new HashSet();
    Set<String> sortFields = new HashSet();

    ComparatorOrder comparatorOrder = ComparatorOrder.ASCENDING;
    for(Pair<String, String> order : orders) {
      sortFields.add(order.getKey());
      adjustedSorts.add(new FieldComparator(order.getKey(), ascDescComp(order.getValue())));
      comparatorOrder = ascDescComp(order.getValue());
    }

    for(Bucket bucket : buckets) {
      bucketFields.add(bucket.toString());
    }

    for(String sf : sortFields) {
      if(!bucketFields.contains(sf)) {
        throw new IOException("All sort fields must be in the field list.");
      }
    }

    //Add sort fields if needed
    if(sortFields.size() < buckets.length) {
      for(Bucket bucket : buckets) {
        String b = bucket.toString();
        if(!sortFields.contains(b)) {
          adjustedSorts.add(new FieldComparator(bucket.toString(), comparatorOrder));
        }
      }
    }

    return adjustedSorts.toArray(new FieldComparator[adjustedSorts.size()]);
  }

  private TupleStream handleStats(String zk,
                                  String collection,
                                  String query,
                                  List<Pair<String, String>> metricPairs,
                                  List<Map.Entry<String, Class>> fields) {


    Map<String, Class> fmap = new HashMap();
    for(Map.Entry<String, Class> entry : fields) {
      fmap.put(entry.getKey(), entry.getValue());
    }

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add(CommonParams.Q, query);
    Metric[] metrics = buildMetrics(metricPairs, false).toArray(new Metric[0]);

    for(Metric metric : metrics) {
      Class c = fmap.get(metric.getIdentifier());
      if(Long.class.equals(c)) {
        metric.outputLong = true;
      }
    }

    return new StatsStream(zk, collection, solrParams, metrics);
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new SolrQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new SolrTableScan(cluster, cluster.traitSetOf(SolrRel.CONVENTION), relOptTable, this, null);
  }

  @SuppressWarnings("WeakerAccess")
  public static class SolrQueryable<T> extends AbstractTableQueryable<T> {
    SolrQueryable(QueryProvider queryProvider, SchemaPlus schema, SolrTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      @SuppressWarnings("unchecked")
      final Enumerable<T> enumerable = (Enumerable<T>) getTable().query(getProperties());
      return enumerable.enumerator();
    }

    private SolrTable getTable() {
      return (SolrTable) table;
    }

    private Properties getProperties() {
      return schema.unwrap(SolrSchema.class).properties;
    }

    /** Called via code-generation.
     *
     * @see SolrMethod#SOLR_QUERYABLE_QUERY
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query(List<Map.Entry<String, Class>> fields, String query, List<Pair<String, String>> order,
                                    List<String> buckets, List<Pair<String, String>> metricPairs, String limit, String negativeQuery, String havingPredicate) {
      return getTable().query(getProperties(), fields, query, order, buckets, metricPairs, limit, negativeQuery, havingPredicate);
    }
  }

  private static FieldComparator[] getComps(List<Pair<String, String>> orders) {
    FieldComparator[] comps = new FieldComparator[orders.size()];
    for(int i=0; i<orders.size(); i++) {
      Pair<String,String> sortItem = orders.get(i);
      String ordering = sortItem.getValue();
      ComparatorOrder comparatorOrder = ascDescComp(ordering);
      String sortKey = sortItem.getKey();
      comps[i] = new FieldComparator(sortKey, comparatorOrder);
    }

    return comps;
  }

  private static ComparatorOrder ascDescComp(String s) {
    if(s.toLowerCase(Locale.ROOT).contains("desc")) {
      return ComparatorOrder.DESCENDING;
    } else {
      return ComparatorOrder.ASCENDING;
    }
  }
}
