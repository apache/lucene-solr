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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.*;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Aggregate} relational expression in Solr.
 */
class SolrAggregate extends Aggregate implements SolrRel {
  private static final List<SqlAggFunction> SUPPORTED_AGGREGATIONS = Arrays.asList(
      SqlStdOperatorTable.COUNT,
      SqlStdOperatorTable.SUM,
      SqlStdOperatorTable.SUM0,
      SqlStdOperatorTable.MIN,
      SqlStdOperatorTable.MAX,
      SqlStdOperatorTable.AVG
  );

  SolrAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
    assert getConvention() == SolrRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input,
                        boolean indicator, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new SolrAggregate(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    final List<String> inNames = SolrRules.solrFieldNames(getInput().getRowType());


    for(Pair<AggregateCall, String> namedAggCall : getNamedAggCalls()) {

      AggregateCall aggCall = namedAggCall.getKey();

      Pair<String, String> metric = toSolrMetric(implementor, aggCall, inNames);
      implementor.addReverseAggMapping(namedAggCall.getValue(), metric.getKey().toLowerCase(Locale.ROOT)+"("+metric.getValue()+")");
      implementor.addMetricPair(namedAggCall.getValue(), metric.getKey(), metric.getValue());
      /*
      if(aggCall.getName() == null) {
        System.out.println("AGG:"+namedAggCall.getValue()+":"+ aggCall.getAggregation().getName() + "(" + inNames.get(aggCall.getArgList().get(0)) + ")");
        implementor.addFieldMapping(namedAggCall.getValue(),
          aggCall.getAggregation().getName() + "(" + inNames.get(aggCall.getArgList().get(0)) + ")");
      }
      */
    }

    for(int group : getGroupSet()) {
      String inName = inNames.get(group);
      implementor.addBucket(inName);
    }
  }

  private Pair<String, String> toSolrMetric(Implementor implementor, AggregateCall aggCall, List<String> inNames) {
    SqlAggFunction aggregation = aggCall.getAggregation();
    List<Integer> args = aggCall.getArgList();
    switch (args.size()) {
      case 0:
        if (aggregation.equals(SqlStdOperatorTable.COUNT)) {
          return new Pair<>(aggregation.getName(), "*");
        }
      case 1:
        String inName = inNames.get(args.get(0));
        String name = implementor.fieldMappings.getOrDefault(inName, inName);
        if(SUPPORTED_AGGREGATIONS.contains(aggregation)) {
          return new Pair<>(aggregation.getName(), name);
        }
      default:
        throw new AssertionError("Invalid aggregation " + aggregation + " with args " + args + " with names" + inNames);
    }
  }
}

// End SolrAggregate.java
