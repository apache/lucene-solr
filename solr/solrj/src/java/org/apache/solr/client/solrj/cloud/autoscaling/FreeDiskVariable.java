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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.autoscaling.Suggester.Hint;
import org.apache.solr.common.util.Pair;

import static org.apache.solr.client.solrj.cloud.autoscaling.Suggestion.suggestNegativeViolations;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.CORE_IDX;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.FREEDISK;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.TOTALDISK;
import static org.apache.solr.common.cloud.rule.ImplicitSnitch.DISK;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class FreeDiskVariable extends VariableBase {

  public FreeDiskVariable(Type type) {
    super(type);
  }

  @Override
  public Object convertVal(Object val) {
    Number value = (Number) super.validate(FREEDISK.tagName, val, false);
    if (value != null) {
      value = value.doubleValue() / 1024.0d / 1024.0d / 1024.0d;
    }
    return value;
  }

  @Override
  public Object computeValue(Policy.Session session, Condition condition, String collection, String shard, String node) {
    if (condition.computedType == ComputedType.PERCENT) {
      Row r = session.getNode(node);
      if (r == null) return 0d;
      return ComputedType.PERCENT.compute(r.getVal(TOTALDISK.tagName), condition);
    }
    throw new IllegalArgumentException("Unsupported type " + condition.computedType);
  }



  @Override
  public int compareViolation(Violation v1, Violation v2) {
    //TODO use tolerance compare
    return Double.compare(
        v1.getViolatingReplicas().stream().mapToDouble(v -> v.delta == null ? 0 : v.delta).max().orElse(0d),
        v2.getViolatingReplicas().stream().mapToDouble(v3 -> v3.delta == null ? 0 : v3.delta).max().orElse(0d));
  }

  @Override
  public void computeDeviation(Policy.Session session, double[] deviation, ReplicaCount replicaCount,
                               SealedClause sealedClause) {
    if (deviation == null) return;
    for (Row node : session.matrix) {
      Object val = node.getVal(sealedClause.tag.name);
      Double delta = sealedClause.tag.delta(val);
      if (delta != null) {
        deviation[0] += Math.abs(delta);
      }
    }
  }

  @Override
  public void getSuggestions(Suggestion.Ctx ctx) {
    if (ctx.violation == null) return;
    if (ctx.violation.replicaCountDelta > 0) {
      List<Row> matchingNodes = ctx.session.matrix.stream().filter(
          row -> ctx.violation.getViolatingReplicas()
              .stream()
              .anyMatch(p -> row.node.equals(p.replicaInfo.getNode())))
          .sorted(Comparator.comparing(r -> ((Double) r.getVal(DISK, 0d))))
          .collect(Collectors.toList());


      for (Row node : matchingNodes) {
        //lets try to start moving the smallest cores off of the node
        ArrayList<ReplicaInfo> replicas = new ArrayList<>();
        node.forEachReplica(replicas::add);
        replicas.sort((r1, r2) -> {
          Long s1 = Clause.parseLong(CORE_IDX.tagName, r1.getVariables().get(CORE_IDX.tagName));
          Long s2 = Clause.parseLong(CORE_IDX.tagName, r2.getVariables().get(CORE_IDX.tagName));
          if (s1 != null && s2 != null) return s1.compareTo(s2);
          return 0;
        });
        double currentDelta = ctx.violation.getClause().tag.delta(node.getVal(DISK));
        for (ReplicaInfo replica : replicas) {
          if (currentDelta < 1) break;
          if (replica.getVariables().get(CORE_IDX.tagName) == null) continue;
          Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
              .hint(Hint.COLL_SHARD, new Pair<>(replica.getCollection(), replica.getShard()))
              .hint(Hint.SRC_NODE, node.node)
              .forceOperation(true);
          ctx.addSuggestion(suggester);
          currentDelta -= Clause.parseLong(CORE_IDX.tagName, replica.getVariable(CORE_IDX.tagName));
        }
      }
    } else if (ctx.violation.replicaCountDelta < 0) {
      suggestNegativeViolations(ctx, shards -> getSortedShards(ctx.session.matrix, shards, ctx.violation.coll));
    }
  }


  static List<String> getSortedShards(List<Row> matrix, Collection<String> shardSet, String coll) {
    return  shardSet.stream()
        .map(shard1 -> {
          AtomicReference<Pair<String, Long>> result = new AtomicReference<>();
          for (Row node : matrix) {
            node.forEachShard(coll, (s, ri) -> {
              if (result.get() != null) return;
              if (s.equals(shard1) && ri.size() > 0) {
                Number sz = ((Number) ri.get(0).getVariable(CORE_IDX.tagName));
                if (sz != null) result.set(new Pair<>(shard1, sz.longValue()));
              }
            });
          }
          return result.get() == null ? new Pair<>(shard1, 0L) : result.get();
        })
        .sorted(Comparator.comparingLong(Pair::second))
        .map(Pair::first)
        .collect(Collectors.toList());

  }

  //When a replica is added, freedisk should be incremented
  @Override
  public void projectAddReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> ops, boolean strictMode) {
    //go through other replicas of this shard and copy the index size value into this
    for (Row row : cell.getRow().session.matrix) {
      row.forEachReplica(replicaInfo -> {
        if (ri != replicaInfo &&
            ri.getCollection().equals(replicaInfo.getCollection()) &&
            ri.getShard().equals(replicaInfo.getShard()) &&
            ri.getVariable(CORE_IDX.tagName) == null &&
            replicaInfo.getVariable(CORE_IDX.tagName) != null) {
          ri.getVariables().put(CORE_IDX.tagName, validate(CORE_IDX.tagName, replicaInfo.getVariable(CORE_IDX.tagName), false));
        }
      });
    }
    Double idxSize = (Double) validate(CORE_IDX.tagName, ri.getVariable(CORE_IDX.tagName), false);
    if (idxSize == null) return;
    Double currFreeDisk = cell.val == null ? 0.0d : (Double) cell.val;
    cell.val = currFreeDisk - idxSize;
  }

  @Override
  public void projectRemoveReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector) {
    Double idxSize = (Double) validate(CORE_IDX.tagName, ri.getVariable(CORE_IDX.tagName), false);
    if (idxSize == null) return;
    Double currFreeDisk = cell.val == null ? 0.0d : (Double) cell.val;
    cell.val = currFreeDisk + idxSize;
  }
}
