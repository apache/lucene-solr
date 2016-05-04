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

import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Relational expression representing a scan of a table in Solr
 */
class SolrToEnumerableConverter extends ConverterImpl implements EnumerableRel {
  SolrToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SolrToEnumerableConverter(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Generates a call to "query" with the appropriate fields
    final BlockBuilder list = new BlockBuilder();
    final SolrRel.Implementor solrImplementor = new SolrRel.Implementor();
    solrImplementor.visitChild(0, getInput());
    final RelDataType rowType = getRowType();
    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
    final Expression table = list.append("table", solrImplementor.table.getExpression(SolrTable.SolrQueryable.class));
    final Expression fields = list.append("fields",
        constantArrayList(generateFields(SolrRules.solrFieldNames(rowType), solrImplementor.fieldMappings), String.class));
    final Expression query = list.append("query", Expressions.constant(solrImplementor.query, String.class));
    final Expression order = list.append("order", constantArrayList(solrImplementor.order, String.class));
    final Expression buckets = list.append("buckets", constantArrayList(solrImplementor.buckets, String.class));
    final Expression metricPairs = list.append("metricPairs", constantArrayList(solrImplementor.metricPairs, Pair.class));
    final Expression limit = list.append("limit", Expressions.constant(solrImplementor.limitValue));
    Expression enumerable = list.append("enumerable", Expressions.call(table, SolrMethod.SOLR_QUERYABLE_QUERY.method,
        fields, query, order, buckets, metricPairs, limit));
    Hook.QUERY_PLAN.run(query);
    list.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  private List<String> generateFields(List<String> queryFields, Map<String, String> fieldMappings) {
    if(fieldMappings.isEmpty()) {
      return queryFields;
    } else {
      List<String> fields = new ArrayList<>();
      for(String field : queryFields) {
        fields.add(fieldMappings.getOrDefault(field, field));
      }
      return fields;
    }
  }

  /**
   * E.g. {@code constantArrayList("x", "y")} returns
   * "Arrays.asList('x', 'y')".
   */
  private static <T> MethodCallExpression constantArrayList(List<T> values, Class clazz) {
    return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(clazz, constantList(values)));
  }

  /**
   * E.g. {@code constantList("x", "y")} returns
   * {@code {ConstantExpression("x"), ConstantExpression("y")}}.
   */
  private static <T> List<Expression> constantList(List<T> values) {
    return Lists.transform(values, Expressions::constant);
  }
}
