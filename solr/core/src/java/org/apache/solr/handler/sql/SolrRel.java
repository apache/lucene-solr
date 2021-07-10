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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.*;

import static org.apache.solr.handler.sql.SolrAggregate.solrAggMetricId;

/**
 * Relational expression that uses Solr calling convention.
 */
interface SolrRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in Solr. */
  Convention CONVENTION = new Convention.Impl("Solr", SolrRel.class);

  /** Callback for the implementation process that converts a tree of {@link SolrRel} nodes into a Solr query. */
  class Implementor {
    final Map<String, String> fieldMappings = new HashMap<>();
    final Map<String, String> reverseAggMappings = new HashMap<>();
    String query = null;
    String havingPredicate;
    boolean negativeQuery;
    String limitValue = null;
    String offsetValue = null;
    final List<Pair<String, String>> orders = new ArrayList<>();
    final List<String> buckets = new ArrayList<>();
    final List<Pair<String, String>> metricPairs = new ArrayList<>();

    RelOptTable table;
    SolrTable solrTable;

    void addFieldMapping(String key, String val, boolean overwrite) {
      if(key != null) {
        if(overwrite || !fieldMappings.containsKey(key)) {
          this.fieldMappings.put(key, val);
        }
      }
    }

    void addReverseAggMapping(String key, String val) {
      if(key != null && !reverseAggMappings.containsKey(key)) {
        this.reverseAggMappings.put(key, val);
      }
    }

    void addQuery(String query) {
      this.query = query;
    }

    void setNegativeQuery(boolean negativeQuery) {
      this.negativeQuery = negativeQuery;
    }

    void addOrder(String column, String direction) {
      column = this.fieldMappings.getOrDefault(column, column);
      this.orders.add(new Pair<>(column, direction));
    }

    void addBucket(String bucket) {
      bucket = this.fieldMappings.getOrDefault(bucket, bucket);
      this.buckets.add(bucket);
    }

    void addMetricPair(String outName, String metric, String column) {
      column = this.fieldMappings.getOrDefault(column, column);
      this.metricPairs.add(new Pair<>(metric, column));

      if(outName != null) {
        this.addFieldMapping(outName, solrAggMetricId(metric, column), true);
      }
    }

    void setHavingPredicate(String havingPredicate) {
      this.havingPredicate = havingPredicate;
    }


    void setLimit(String limit) {
      limitValue = limit;
    }

    void setOffset(String offset) {
      this.offsetValue = offset;
    }

    void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((SolrRel) input).implement(this);
    }
  }
}
