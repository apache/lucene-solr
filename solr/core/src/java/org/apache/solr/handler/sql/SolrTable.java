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
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Table based on a Solr collection
 */
class SolrTable extends AbstractQueryableTable implements TranslatableTable {
  private static final String DEFAULT_QUERY = "*:*";
  private static final String DEFAULT_VERSION_FIELD = "_version_";

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
  private Enumerable<Object> query(final Properties properties, final List<Map.Entry<String, Class>> fields,
                                   final String query, final List<Pair<String, String>> orders, final List<String> buckets,
                                   final List<Pair<String, String>> metricPairs, final String limit) {
    // SolrParams should be a ModifiableParams instead of a map
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add(CommonParams.OMIT_HEADER, "true");

    if (query == null) {
      solrParams.add(CommonParams.Q, DEFAULT_QUERY);
    } else {
      solrParams.add(CommonParams.Q, DEFAULT_QUERY + " AND " + query);
    }

    // List<String> doesn't have add so must make a new ArrayList
    List<String> fieldsList = new ArrayList<>(fields.size());
    fieldsList.addAll(fields.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
    List<Pair<String, String>> ordersList = new ArrayList<>(orders);
    List<Metric> metrics = buildMetrics(metricPairs);
    List<Bucket> bucketsList = buckets.stream().map(Bucket::new).collect(Collectors.toList());

    for(int i = buckets.size()-1; i >= 0; i--) {
      ordersList.add(0, new Pair<>(buckets.get(i), "asc"));
    }

    for(Metric metric : metrics) {
      String metricIdentifier = metric.getIdentifier();

      List<Pair<String, String>> newOrders= new ArrayList<>();
      for(Pair<String, String> order : ordersList) {
        String column = order.getKey();
        if(!column.startsWith(metricIdentifier)) {
          newOrders.add(order);
        }
      }
      ordersList = newOrders;

      if(fieldsList.contains(metricIdentifier)) {
        fieldsList.remove(metricIdentifier);
      }

      for(String column : metric.getColumns()) {
        if (!fieldsList.contains(column)) {
          fieldsList.add(column);
        }

        Pair<String, String> order = new Pair<>(column, "asc");
        if(!ordersList.contains(order)) {
          ordersList.add(order);
        }
      }
    }

    ordersList.add(new Pair<>(DEFAULT_VERSION_FIELD, "desc"));

    // Make sure the default sort field is in the field list
    if (!fieldsList.contains(DEFAULT_VERSION_FIELD)) {
      fieldsList.add(DEFAULT_VERSION_FIELD);
    }

    if(!ordersList.isEmpty()) {
      List<String> orderList = new ArrayList<>(ordersList.size());
      for(Pair<String, String> order : ordersList) {
        String column = order.getKey();
        if(!fieldsList.contains(column)) {
          fieldsList.add(column);
        }
        orderList.add(column + " " + order.getValue());
      }
      solrParams.add(CommonParams.SORT, String.join(",", orderList));
    }

    if (fieldsList.isEmpty()) {
      solrParams.add(CommonParams.FL, "*");
    } else {
      solrParams.add(CommonParams.FL, String.join(",", fieldsList));
    }

    TupleStream tupleStream;
    String zk = properties.getProperty("zk");
    try {
      if (metrics.isEmpty()) {
        if (limit == null) {
          solrParams.add(CommonParams.QT, "/export");
          tupleStream = new CloudSolrStream(zk, collection, solrParams);
        } else {
          solrParams.add(CommonParams.ROWS, limit);
          tupleStream = new LimitStream(new CloudSolrStream(zk, collection, solrParams), Integer.parseInt(limit));
        }
      } else {
        Metric[] metricsArray = metrics.toArray(new Metric[metrics.size()]);
        if(bucketsList.isEmpty()) {
          solrParams.remove(CommonParams.FL);
          solrParams.remove(CommonParams.SORT);
          tupleStream = new StatsStream(zk, collection, solrParams, metricsArray);
        } else {
          solrParams.add(CommonParams.QT, "/export");
          tupleStream = new CloudSolrStream(zk, collection, solrParams);
          tupleStream = new RollupStream(tupleStream, bucketsList.toArray(new Bucket[bucketsList.size()]), metricsArray);

          String sortDirection = getSortDirection(ordersList);

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

          if (!sortsEqual(bucketsList, sortDirection, ordersList)) {
            int limitVal = limit == null ? 100 : Integer.parseInt(limit);
            StreamComparator comp = getComp(ordersList);
            //Rank the Tuples
            //If parallel stream is used ALL the Rolled up tuples from the workers will be ranked
            //Providing a true Top or Bottom.
            tupleStream = new RankStream(tupleStream, limitVal, comp);
          } else {
            // Sort is the same as the same as the underlying stream
            // Only need to limit the result, not Rank the result
            if (limit != null) {
              solrParams.add(CommonParams.ROWS, limit);
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

  private boolean sortsEqual(List<Bucket> buckets, String direction, List<Pair<String, String>> orders) {
    if(buckets.size() != orders.size()) {
      return false;
    }

    for(int i=0; i< buckets.size(); i++) {
      Bucket bucket = buckets.get(i);
      Pair<String, String> order = orders.get(i);
      if(!bucket.toString().equals(getSortField(order))) {
        return false;
      }


      if(!getSortDirection(order).equalsIgnoreCase(direction)) {
        return false;
      }
    }

    return true;
  }


  private String getSortDirection(List<Pair<String, String>> orders) {
    for(Pair<String, String> order : orders) {
      return getSortDirection(order);
    }

    return "asc";
  }

  private String getSortField(Pair<String, String> order) {
    return order.getKey();
  }

  private String getSortDirection(Pair<String, String> order) {
    String direction = order.getValue();
    return direction == null ? "asc" : direction;
  }

  private StreamComparator getComp(List<Pair<String, String>> orders) {
    FieldComparator[] comps = new FieldComparator[orders.size()];
    for(int i = 0; i < orders.size(); i++) {
      Pair<String, String> order = orders.get(i);
      String direction = getSortDirection(order);
      ComparatorOrder comparatorOrder = ComparatorOrder.fromString(direction);
      String sortKey = getSortField(order);
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
    metrics.addAll(metricPairs.stream().map(this::getMetric).collect(Collectors.toList()));
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
    public Enumerable<Object> query(List<Map.Entry<String, Class>> fields, String query, List<Pair<String, String>> order,
                                    List<String> buckets, List<Pair<String, String>> metricPairs, String limit) {
      return getTable().query(getProperties(), fields, query, order, buckets, metricPairs, limit);
    }
  }
}
