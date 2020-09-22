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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.Pair;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

/**
 * Implements the 'withCollection' variable type
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class WithCollectionVariable extends VariableBase {

  public WithCollectionVariable(Type type) {
    super(type);
  }

  @Override
  public boolean match(Object inputVal, Operand op, Object val, String name, Row row) {
    @SuppressWarnings({"unchecked"})
    Map<String, String> withCollectionMap = (Map<String, String>) inputVal;
    if (withCollectionMap == null || withCollectionMap.isEmpty()) return true;

    Set<String> uniqueColls = new HashSet<>();
    row.forEachReplica(replicaInfo -> uniqueColls.add(replicaInfo.getCollection()));

    for (Map.Entry<String, String> e : withCollectionMap.entrySet()) {
      if (uniqueColls.contains(e.getKey()) && !uniqueColls.contains(e.getValue())) return false;
    }

    return true;
  }

  public void projectAddReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector, boolean strictMode) {
    if (strictMode) {
      // we do not want to add a replica of the 'withCollection' in strict mode
      return;
    }

    @SuppressWarnings({"unchecked"})
    Map<String, String> withCollectionMap = (Map<String, String>) cell.val;
    if (withCollectionMap == null || withCollectionMap.isEmpty()) return;

    Set<String> uniqueColls = new HashSet<>();
    Row row = cell.row;
    row.forEachReplica(replicaInfo -> uniqueColls.add(replicaInfo.getCollection()));

    for (Map.Entry<String, String> e : withCollectionMap.entrySet()) {
      if (uniqueColls.contains(e.getKey()) && !uniqueColls.contains(e.getValue())) {
        String withCollection = e.getValue();

        opCollector.accept(new Row.OperationInfo(withCollection, "shard1", row.node, cell.name, true, Replica.Type.NRT));
      }
    }
  }

  @Override
  public int compareViolation(Violation v1, Violation v2) {
    return Integer.compare(v1.getViolatingReplicas().size(), v2.getViolatingReplicas().size());
  }

  public boolean addViolatingReplicas(Violation.Ctx ctx) {
    String node = ctx.currentViolation.node;
    for (Row row : ctx.allRows) {
      if (node.equals(row.node)) {
        @SuppressWarnings({"unchecked"})
        Map<String, String> withCollectionMap = (Map<String, String>) row.getVal("withCollection");
        if (withCollectionMap != null) {
          row.forEachReplica(r -> {
            String withCollection = withCollectionMap.get(r.getCollection());
            if (withCollection != null) {
              // test whether this row has at least 1 replica of withCollection, else there is a violation
              Set<String> uniqueCollections = new HashSet<>();
              row.forEachReplica(replicaInfo -> uniqueCollections.add(replicaInfo.getCollection()));
              if (!uniqueCollections.contains(withCollection)) {
                ctx.currentViolation.addReplica(new Violation.ReplicaInfoAndErr(r).withDelta(1.0d));
              }
            }
          });
          ctx.currentViolation.replicaCountDelta = (double) ctx.currentViolation.getViolatingReplicas().size();
        }
      }
    }
    return true;
  }

  @Override
  public void getSuggestions(Suggestion.Ctx ctx) {
    if (ctx.violation.getViolatingReplicas().isEmpty()) return;

    Map<String, Object> nodeValues = ctx.session.nodeStateProvider.getNodeValues(ctx.violation.node, Collections.singleton("withCollection"));
    @SuppressWarnings({"unchecked"})
    Map<String, String> withCollectionsMap = (Map<String, String>) nodeValues.get("withCollection");
    if (withCollectionsMap == null) return;

    Set<String> uniqueCollections = new HashSet<>();
    for (Violation.ReplicaInfoAndErr replicaInfoAndErr : ctx.violation.getViolatingReplicas()) {
      uniqueCollections.add(replicaInfoAndErr.replicaInfo.getCollection());
    }

    collectionLoop:
    for (String collection : uniqueCollections) {
      String withCollection = withCollectionsMap.get(collection);
      if (withCollection == null) continue;

      // can we find a node from which we can move a replica of the `withCollection`
      // without creating another violation?
      for (Row row : ctx.session.matrix) {
        if (ctx.violation.node.equals(row.node))  continue; // filter the violating node

        Set<String> hostedCollections = new HashSet<>();
        row.forEachReplica(replicaInfo -> hostedCollections.add(replicaInfo.getCollection()));

        if (hostedCollections.contains(withCollection) && !hostedCollections.contains(collection))  {
          // find the candidate replicas that we can move
          List<ReplicaInfo> movableReplicas = new ArrayList<>();
          row.forEachReplica(replicaInfo -> {
            if (replicaInfo.getCollection().equals(withCollection)) {
              movableReplicas.add(replicaInfo);
            }
          });

          for (ReplicaInfo toMove : movableReplicas) {
            // candidate source node for a move replica operation
            Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
                .forceOperation(true)
                .hint(Suggester.Hint.COLL_SHARD, new Pair<>(withCollection, "shard1"))
                .hint(Suggester.Hint.SRC_NODE, row.node)
                .hint(Suggester.Hint.REPLICA, toMove.getName())
                .hint(Suggester.Hint.TARGET_NODE, ctx.violation.node);
            if (ctx.addSuggestion(suggester) != null)
              continue collectionLoop; // one suggestion is enough for this collection
          }
        }
      }

      // we could not find a valid move, so we suggest adding a replica
      Suggester suggester = ctx.session.getSuggester(ADDREPLICA)
          .forceOperation(true)
          .hint(Suggester.Hint.COLL_SHARD, new Pair<>(withCollection, "shard1"))
          .hint(Suggester.Hint.TARGET_NODE, ctx.violation.node);
      ctx.addSuggestion(suggester);
    }
  }
}
