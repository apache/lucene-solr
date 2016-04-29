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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.params.CommonParams;

/**
 * Table based on a Solr collection
 */
public class SolrTable extends AbstractQueryableTable implements TranslatableTable {
  private static final String DEFAULT_SORT_FIELD = "_version_";

  private final String collection;
  private final SolrSchema schema;
  private RelProtoDataType protoRowType;

  public SolrTable(SolrSchema schema, String collection) {
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
  
  public Enumerable<Object> query(final Properties properties) {
    return query(properties, Collections.emptyList(), null, Collections.emptyList(), null);
  }

  /** Executes a Solr query on the underlying table.
   *
   * @param properties Connections properties
   * @param fields List of fields to project
   * @param query A string for the query
   * @return Enumerator of results
   */
  public Enumerable<Object> query(final Properties properties, List<String> fields,
                                  String query, List<String> order, String limit) {
    Map<String, String> solrParams = new HashMap<>();
    //solrParams.put(CommonParams.OMIT_HEADER, "true");
    solrParams.put(CommonParams.Q, "*:*");
    //solrParams.put(CommonParams.QT, "/export");

    if (fields.isEmpty()) {
      solrParams.put(CommonParams.FL, "*");
    } else {
      solrParams.put(CommonParams.FL, String.join(",", fields));
    }

    if (query == null) {
      solrParams.put(CommonParams.FQ, "*:*");
    } else {
      // SolrParams should be a ModifiableParams instead of a map so we could add multiple FQs
      solrParams.put(CommonParams.FQ, query);
    }

    // Build and issue the query and return an Enumerator over the results
    if (order.isEmpty()) {
      solrParams.put(CommonParams.SORT, DEFAULT_SORT_FIELD + " desc");

      // Make sure the default sort field is in the field list
      String fl = solrParams.get(CommonParams.FL);
      if(!fl.contains(DEFAULT_SORT_FIELD)) {
        solrParams.put(CommonParams.FL, String.join(",", fl, DEFAULT_SORT_FIELD));
      }
    } else {
      solrParams.put(CommonParams.SORT, String.join(",", order));
    }

    TupleStream tupleStream;
    try {
      String zk = properties.getProperty("zk");
      tupleStream = new CloudSolrStream(zk, collection, solrParams);
      if(limit != null) {
        tupleStream = new LimitStream(tupleStream, Integer.parseInt(limit));
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final TupleStream finalStream = tupleStream;

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        return new SolrEnumerator(finalStream, fields);
      }
    };
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new SolrQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new SolrTableScan(cluster, cluster.traitSetOf(SolrRel.CONVENTION), relOptTable, this, null);
  }

  public static class SolrQueryable<T> extends AbstractTableQueryable<T> {
    SolrQueryable(QueryProvider queryProvider, SchemaPlus schema, SolrTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      //noinspection unchecked
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
    public Enumerable<Object> query(List<String> fields, String query, List<String> order, String limit) {
      return getTable().query(getProperties(), fields, query, order, limit);
    }
  }
}
