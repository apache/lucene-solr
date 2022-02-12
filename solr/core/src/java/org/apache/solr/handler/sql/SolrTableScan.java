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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Relational expression representing a scan of a Solr collection.
 */
class SolrTableScan extends TableScan implements SolrRel {
  private final SolrTable solrTable;
  private final RelDataType projectRowType;

  /**
   * Creates a SolrTableScan.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param solrTable      Solr table
   * @param projectRowType Fields and types to project; null to project raw row
   */
  SolrTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, SolrTable solrTable,
                RelDataType projectRowType) {
    super(cluster, traitSet, table);
    this.solrTable = solrTable;
    this.projectRowType = projectRowType;

    assert solrTable != null;
    assert getConvention() == SolrRel.CONVENTION;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final float f = projectRowType == null ? 1f : (float) projectRowType.getFieldCount() / 100f;
    return super.computeSelfCost(planner, mq).multiplyBy(.1 * f);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override
  public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(SolrToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : SolrRules.RULES) {
      planner.addRule(rule);
    }

    // Solr's impl only supports LogicalAggregate, so don't let Calcite convert LogicalAggregate's to Enumerable (SOLR-15974)
    planner.removeRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
    planner.removeRule(CoreRules.FILTER_REDUCE_EXPRESSIONS); // prevent AND NOT from being reduced away, see SOLR-15461
  }

  public void implement(Implementor implementor) {
    implementor.solrTable = solrTable;
    implementor.table = table;
  }
}
