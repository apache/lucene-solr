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

import org.apache.solr.common.cloud.rule.ImplicitSnitch;

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
    for (Row r : ctx.allRows) {
      if (!ctx.clause.tag.isPass(r)) {
        r.forEachReplica(replicaInfo -> ctx.currentViolation
            .addReplica(new Violation.ReplicaInfoAndErr(replicaInfo)
                .withDelta(ctx.clause.tag.delta(r.getVal(ImplicitSnitch.CORES)))));
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
        ctx.addSuggestion(suggester);
      }
    }
  }

  @Override
  public void projectAddReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> ops, boolean strictMode) {
    cell.val = cell.val == null ? 0 : ((Number) cell.val).longValue() + 1;
  }

  @Override
  public void projectRemoveReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector) {
    cell.val = cell.val == null ? 0 : ((Number) cell.val).longValue() - 1;
  }
}
