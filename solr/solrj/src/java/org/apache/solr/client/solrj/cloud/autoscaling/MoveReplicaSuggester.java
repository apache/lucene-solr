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

import java.util.Comparator;
import java.util.List;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

public class MoveReplicaSuggester extends Suggester {

  @Override
  SolrRequest init() {
    SolrRequest operation = tryEachNode(true);
    if (operation == null) operation = tryEachNode(false);
    return operation;
  }

  SolrRequest tryEachNode(boolean strict) {
    //iterate through elements and identify the least loaded
    List<Violation> leastSeriousViolation = null;
    Row bestSrcRow = null;
    Row bestTargetRow = null;
    ReplicaInfo sourceReplicaInfo = null;
    List<Pair<ReplicaInfo, Row>> validReplicas = getValidReplicas(true, true, -1);
    validReplicas.sort(leaderLast);
    for (int i1 = 0; i1 < validReplicas.size(); i1++) {
      Pair<ReplicaInfo, Row> fromReplica = validReplicas.get(i1);
      Row fromRow = fromReplica.second();
      ReplicaInfo ri = fromReplica.first();
      if (ri == null) continue;
      final int i = session.indexOf(fromRow.node);
      int stopAt = force ? 0 : i;
      Row targetRow = null;
      for (int j = session.matrix.size() - 1; j >= stopAt; j--) {
        targetRow = session.matrix.get(j);
        if (targetRow.node.equals(fromRow.node)) continue;
        if (!isNodeSuitableForReplicaAddition(targetRow)) continue;
        targetRow = targetRow.addReplica(ri.getCollection(), ri.getShard(), ri.getType());//add replica to target first
        Pair<Row, ReplicaInfo> pair = targetRow.session.getNode(fromRow.node).removeReplica(ri.getCollection(), ri.getShard(), ri.getType());//then remove replica from source node
        if (pair == null) continue;//should not happen
        Row srcRowModified = pair.first();//this is the final state of the source row and session
        List<Violation> errs = testChangedMatrix(strict, srcRowModified.session.matrix);
        srcRowModified.session.applyRules();// now resort the nodes with the new values
        Policy.Session tmpSession = srcRowModified.session;
        if (!containsNewErrors(errs) &&
            isLessSerious(errs, leastSeriousViolation) &&
            (force || (tmpSession.indexOf(srcRowModified.node) < tmpSession.indexOf(targetRow.node)))) {
          leastSeriousViolation = errs;
          bestSrcRow = srcRowModified;
          sourceReplicaInfo = ri;
          bestTargetRow = targetRow;
        }
      }
    }
    if (bestSrcRow != null) {
      this.session = bestSrcRow.session;
      return new CollectionAdminRequest.MoveReplica(
          sourceReplicaInfo.getCollection(),
          sourceReplicaInfo.getName(),
          bestTargetRow.node);
    }
    return null;
  }

  static Comparator<Pair<ReplicaInfo, Row>> leaderLast = (r1, r2) -> {
    if (r1.first().isLeader) return 1;
    if (r2.first().isLeader) return -1;
    return 0;
  };


  @Override
  public CollectionParams.CollectionAction getAction() {
    return MOVEREPLICA;
  }
}
