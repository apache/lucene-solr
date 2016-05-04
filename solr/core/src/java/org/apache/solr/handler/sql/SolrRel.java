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
import org.apache.solr.client.solrj.io.stream.metrics.Metric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    String query = null;
    String limitValue = null;
    final List<String> order = new ArrayList<>();
    final List<String> buckets = new ArrayList<>();
    final List<Metric> metrics = new ArrayList<>();

    RelOptTable table;
    SolrTable solrTable;

    void addFieldMappings(Map<String, String> fieldMappings) {
      this.fieldMappings.putAll(fieldMappings);
    }

    void addQuery(String query) {
      this.query = query;
    }

    void addOrder(List<String> order) {
      for(String orderItem : order) {
        String[] orderParts = orderItem.split(" ", 2);
        String fieldName = orderParts[0];
        String direction = orderParts[1];
       this.order.add(this.fieldMappings.getOrDefault(fieldName, fieldName) + " " + direction);
      }
    }

    void addBuckets(List<String> buckets) {
      this.buckets.addAll(buckets);
    }

    void addMetrics(List<Metric> metrics) {
      this.metrics.addAll(metrics);
    }

    void setLimit(String limit) {
      limitValue = limit;
    }

    void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((SolrRel) input).implement(this);
    }
  }
}
