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
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.update.VersionInfo;

import java.io.IOException;
import java.util.*;

/**
 * Table based on a Solr collection
 */
class SolrTable extends AbstractQueryableTable implements TranslatableTable {
  private static final String DEFAULT_QUERY = "*:*";
  private static final String DEFAULT_VERSION_FIELD = VersionInfo.VERSION_FIELD;
  private static final String DEFAULT_SCORE_FIELD = "score";

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
        Collections.emptyList(), null);
  }

  /** Executes a Solr query on the underlying table.
   *
   * @param properties Connections properties
   * @param fields List of fields to project
   * @param query A string for the query
   * @return Enumerator of results
   */
  private Enumerable<Object> query(final Properties properties, final List<String> fields,
                                   final String query, final List<String> order, final List<String> buckets,
                                   final List<Pair<String, String>> metricPairs, final String limit) {
    // SolrParams should be a ModifiableParams instead of a map
    Map<String, String> solrParams = new HashMap<>();
    solrParams.put(CommonParams.OMIT_HEADER, "true");

    if (query == null) {
      solrParams.put(CommonParams.Q, DEFAULT_QUERY);
    } else {
      solrParams.put(CommonParams.Q, DEFAULT_QUERY + " AND " + query);
    }

    // List<String> doesn't have add so must make a new ArrayList
    List<String> fieldsList = new ArrayList<>(fields);
    List<String> orderList = new ArrayList<>(order);

    List<Metric> metrics = buildMetrics(metricPairs);

    if (!metrics.isEmpty()) {
      for(String bucket : buckets) {
        orderList.add(bucket + " desc");
      }

      for(Metric metric : metrics) {
        List<String> newOrderList = new ArrayList<>();
        for(String orderItem : orderList) {
          if(!orderItem.startsWith(metric.getIdentifier())) {
            newOrderList.add(orderItem);
          }
        }
        orderList = newOrderList;

        for(String column : metric.getColumns()) {
          if (!fieldsList.contains(column)) {
            fieldsList.add(column);
          }
        }
      }
    }

    if (orderList.isEmpty()) {
      if (limit != null && Integer.parseInt(limit) > -1) {
        orderList.add(DEFAULT_SCORE_FIELD + " desc");

        // Make sure the default score field is in the field list
        if (!fieldsList.contains(DEFAULT_SCORE_FIELD)) {
          fieldsList.add(DEFAULT_SCORE_FIELD);
        }
      } else {
        orderList.add(DEFAULT_VERSION_FIELD + " desc");

        // Make sure the default sort field is in the field list
        if (!fieldsList.contains(DEFAULT_VERSION_FIELD)) {
          fieldsList.add(DEFAULT_VERSION_FIELD);
        }
      }
    }

    solrParams.put(CommonParams.SORT, String.join(",", orderList));

    if (fieldsList.isEmpty()) {
      solrParams.put(CommonParams.FL, "*");
    } else {
      solrParams.put(CommonParams.FL, String.join(",", fieldsList));
    }

    TupleStream tupleStream;
    String zk = properties.getProperty("zk");
    try {
      if (metrics.isEmpty()) {
        if (limit == null) {
          solrParams.put(CommonParams.QT, "/export");
          tupleStream = new CloudSolrStream(zk, collection, solrParams);
        } else {
          solrParams.put(CommonParams.ROWS, limit);
          tupleStream = new LimitStream(new CloudSolrStream(zk, collection, solrParams), Integer.parseInt(limit));
        }
      } else {
        Metric[] metricsArray = metrics.toArray(new Metric[metrics.size()]);
        if(buckets.isEmpty()) {
          solrParams.remove(CommonParams.FL);
          solrParams.remove(CommonParams.SORT);
          tupleStream = new StatsStream(zk, collection, solrParams, metricsArray);
        } else {
          List<Bucket> bucketsList = new ArrayList<>();
          for(String bucket : buckets) {
            bucketsList.add(new Bucket(bucket));
          }

          solrParams.put(CommonParams.QT, "/export");
          for(Metric metric : metrics) {
            fieldsList.remove(metric.getIdentifier());
          }
          solrParams.put(CommonParams.FL, String.join(",", fieldsList));
          tupleStream = new CloudSolrStream(zk, collection, solrParams);
          tupleStream = new RollupStream(tupleStream, bucketsList.toArray(new Bucket[bucketsList.size()]), metricsArray);

          String sortDirection = getSortDirection(orderList);

          int numWorkers = Integer.parseInt(properties.getProperty("numWorkers", "1"));
          if(numWorkers > 1) {
            String workerZkHost = properties.getProperty("workerZkhost");
            String workerCollection = properties.getProperty("workerCollection");
            // Do the rollups in parallel
            // Maintain the sort of the Tuples coming from the workers.
            StreamComparator comp = bucketSortComp(bucketsList, sortDirection);
            ParallelStream parallelStream = new ParallelStream(workerZkHost, workerCollection, tupleStream, numWorkers, comp);

            StreamFactory factory = new StreamFactory()
                .withFunctionName("search", CloudSolrStream.class)
                .withFunctionName("parallel", ParallelStream.class)
                .withFunctionName("rollup", RollupStream.class)
                .withFunctionName("sum", SumMetric.class)
                .withFunctionName("min", MinMetric.class)
                .withFunctionName("max", MaxMetric.class)
                .withFunctionName("avg", MeanMetric.class)
                .withFunctionName("count", CountMetric.class);

            parallelStream.setStreamFactory(factory);
            tupleStream = parallelStream;
          }

          if (!sortsEqual(bucketsList, sortDirection, orderList)) {
            int limitVal = limit == null ? 100 : Integer.parseInt(limit);
            StreamComparator comp = getComp(orderList);
            //Rank the Tuples
            //If parallel stream is used ALL the Rolled up tuples from the workers will be ranked
            //Providing a true Top or Bottom.
            tupleStream = new RankStream(tupleStream, limitVal, comp);
          } else {
            // Sort is the same as the same as the underlying stream
            // Only need to limit the result, not Rank the result
            if (limit != null) {
              solrParams.put(CommonParams.ROWS, limit);
              tupleStream = new LimitStream(new CloudSolrStream(zk, collection, solrParams), Integer.parseInt(limit));
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final TupleStream finalStream = tupleStream;

    return new AbstractEnumerable<Object>() {
      // Use original fields list to make sure only the fields specified are enumerated
      public Enumerator<Object> enumerator() {
        return new SolrEnumerator(finalStream, fields);
      }
    };
  }

  private static StreamComparator bucketSortComp(List<Bucket> buckets, String dir) {
    FieldComparator[] comps = new FieldComparator[buckets.size()];
    for(int i=0; i<buckets.size(); i++) {
      ComparatorOrder comparatorOrder = ComparatorOrder.fromString(dir);
      String sortKey = buckets.get(i).toString();
      comps[i] = new FieldComparator(sortKey, comparatorOrder);
    }

    if(comps.length == 1) {
      return comps[0];
    } else {
      return new MultipleFieldComparator(comps);
    }
  }

  private boolean sortsEqual(List<Bucket> buckets, String direction, List<String> orderList) {
    if(buckets.size() != orderList.size()) {
      return false;
    }

    for(int i=0; i< buckets.size(); i++) {
      Bucket bucket = buckets.get(i);
      String orderItem = orderList.get(i);
      if(!bucket.toString().equals(getSortField(orderItem))) {
        return false;
      }


      if(!getSortDirection(orderItem).equalsIgnoreCase(direction)) {
        return false;
      }
    }

    return true;
  }


  private String getSortDirection(List<String> orderList) {
    for(String orderItem : orderList) {
      return getSortDirection(orderItem);
    }

    return "asc";
  }

  private String getSortField(String orderItem) {
    String[] orderParts = orderItem.split(" ", 2);
    return orderParts[0];
  }

  private String getSortDirection(String orderItem) {
    String[] orderParts = orderItem.split(" ", 2);
    String direction = orderParts[1];
    return direction == null ? "asc" : direction;
  }

  private StreamComparator getComp(List<String> orderList) {
    FieldComparator[] comps = new FieldComparator[orderList.size()];
    for(int i = 0; i < orderList.size(); i++) {
      String orderItem = orderList.get(i);
      String direction = getSortDirection(orderItem);
      ComparatorOrder comparatorOrder = ComparatorOrder.fromString(direction);
      String sortKey = getSortField(orderItem);
      comps[i] = new FieldComparator(sortKey, comparatorOrder);
    }

    if(comps.length == 1) {
      return comps[0];
    } else {
      return new MultipleFieldComparator(comps);
    }
  }

  private List<Metric> buildMetrics(List<Pair<String, String>> metricPairs) {
    List<Metric> metrics = new ArrayList<>(metricPairs.size());
    for(Pair<String, String> metricPair : metricPairs) {
      metrics.add(getMetric(metricPair));
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
    public Enumerable<Object> query(List<String> fields, String query, List<String> order, List<String> buckets,
                                    List<Pair<String, String>> metricPairs, String limit) {
      return getTable().query(getProperties(), fields, query, order, buckets, metricPairs, limit);
    }
  }
}
