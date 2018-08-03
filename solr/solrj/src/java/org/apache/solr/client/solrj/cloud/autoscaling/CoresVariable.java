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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.util.function.Consumer;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

public class CoresVariable extends VariableBase {
  public CoresVariable(Type type) {
    super(type);
  }

  @Override
  public Object validate(String name, Object val, boolean isRuleVal) {
    return VariableBase.getOperandAdjustedValue(super.validate(name, val, isRuleVal), val);
  }

  @Override
  public void addViolatingReplicas(Violation.Ctx ctx) {
    for (Row row : ctx.allRows) {
      if (row.node.equals(ctx.currentViolation.node)) {
        row.forEachReplica(replicaInfo -> ctx.currentViolation
            .addReplica(new Violation.ReplicaInfoAndErr(replicaInfo)
                .withDelta(ctx.currentViolation.replicaCountDelta)));
      }
    }


  }

  @Override
  public void getSuggestions(Suggestion.Ctx ctx) {
    if (ctx.violation == null || ctx.violation.replicaCountDelta == 0) return;
    if (ctx.violation.replicaCountDelta > 0) {//there are more replicas than necessary
      for (int i = 0; i < Math.abs(ctx.violation.replicaCountDelta); i++) {
        Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
            .hint(Suggester.Hint.SRC_NODE, ctx.violation.node);
        if (ctx.addSuggestion(suggester) == null) break;
      }
    }
  }

  @Override
  public void projectAddReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> ops, boolean strictMode) {
    cell.val = cell.val == null ? 0 : ((Number) cell.val).doubleValue() + 1;
  }

  @Override
  public void projectRemoveReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector) {
    cell.val = cell.val == null ? 0 : ((Number) cell.val).doubleValue() - 1;
  }

  @Override
  public Object computeValue(Policy.Session session, Clause.Condition condition, String collection, String shard, String node) {
    if (condition.computedType == Clause.ComputedType.EQUAL) {
      int[] coresCount = new int[1];
      int[] liveNodes = new int[1];
      for (Row row : session.matrix) {
        if (!row.isLive) continue;
        liveNodes[0]++;
        row.forEachReplica(replicaInfo -> coresCount[0]++);
      }
      return liveNodes[0] == 0 || coresCount[0] == 0 ? 0d : (double) coresCount[0] / (double) liveNodes[0];
    } else {
      throw new IllegalArgumentException("Invalid computed type in " + condition);
    }
  }

  @Override
  public String postValidate(Clause.Condition condition) {
    Clause.Condition nodeTag = condition.getClause().getTag();
    if (nodeTag.name.equals("node") && nodeTag.op == Operand.WILDCARD) {
      return null;
    } else {
      throw new IllegalArgumentException("cores: '#EQUAL' can be used only with node: '#ANY'");
    }
  }

  @Override
  public Operand getOperand(Operand expected, Object strVal, Clause.ComputedType computedType) {
    return ReplicaVariable.checkForRangeOperand(expected, strVal, computedType);
  }
}
