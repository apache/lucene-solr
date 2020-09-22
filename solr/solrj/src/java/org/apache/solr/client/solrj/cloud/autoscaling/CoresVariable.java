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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class CoresVariable extends VariableBase {
  public CoresVariable(Type type) {
    super(type);
  }

  @Override
  public Object validate(String name, Object val, boolean isRuleVal) {
    return VariableBase.getOperandAdjustedValue(super.validate(name, val, isRuleVal), val);
  }

  public boolean addViolatingReplicas(Violation.Ctx ctx) {
    for (Row row : ctx.allRows) {
      if (row.node.equals(ctx.currentViolation.node)) {
        row.forEachReplica(replicaInfo -> ctx.currentViolation
            .addReplica(new Violation.ReplicaInfoAndErr(replicaInfo)
                .withDelta(ctx.currentViolation.replicaCountDelta)));
      }
    }
    return true;

  }

  @Override
  public void getSuggestions(Suggestion.Ctx ctx) {
    if (ctx.violation == null || ctx.violation.replicaCountDelta == 0) return;
    if (ctx.violation.replicaCountDelta > 0) {//there are more replicas than necessary
      for (int i = 0; i < Math.abs(ctx.violation.replicaCountDelta); i++) {
        if (!ctx.needMore()) return;
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
  public Object computeValue(Policy.Session session, Condition condition, String collection, String shard, String node) {
    if (condition.computedType == ComputedType.EQUAL) {
      AtomicInteger liveNodes = new AtomicInteger(0);
      int coresCount = getTotalCores(session, liveNodes);
      int numBuckets = condition.clause.tag.op == Operand.IN ?
          ((Collection) condition.clause.tag.val).size() :
          liveNodes.get();
      return numBuckets == 0 || coresCount == 0 ? 0d : (double) coresCount / (double) numBuckets;
    } else if (condition.computedType == ComputedType.PERCENT) {
      return ComputedType.PERCENT.compute(getTotalCores(session, new AtomicInteger()), condition);
    } else {
      throw new IllegalArgumentException("Invalid computed type in " + condition);
    }
  }

  static final String TOTALCORES = CoresVariable.class.getSimpleName() + ".totalcores";
  private int getTotalCores(Policy.Session session, AtomicInteger liveNodes) {
    int coresCount = 0;
    for (Row row : session.matrix) {
      if (!row.isLive) continue;
      liveNodes.incrementAndGet();
      Integer res = row.computeCacheIfAbsent(TOTALCORES, o -> {
        int[] result = new int[1];
        row.forEachReplica(replicaInfo -> result[0]++);
        return result[0];
      });
      if (res != null)
        coresCount += res;


    }
    return coresCount;
  }

  @Override
  public String postValidate(Condition condition) {
    Condition nodeTag = condition.getClause().getTag();
    if (nodeTag.varType != Type.NODE) return "'cores' attribute can only be used with 'node' attribute";
    if (condition.computedType == ComputedType.EQUAL) {
      if (nodeTag.name.equals("node") && (nodeTag.op == Operand.WILDCARD || nodeTag.op == Operand.IN)) {
        return null;
      } else {
        return "cores: '#EQUAL' can be used only with node: '#ANY', node :[....]";
      }
    } else {
      return ReplicaVariable.checkNonEqualOp(condition);
    }
  }

  @Override
  public Operand getOperand(Operand expected, Object strVal, ComputedType computedType) {
    return ReplicaVariable.checkForRangeOperand(expected, strVal, computedType);
  }
}
