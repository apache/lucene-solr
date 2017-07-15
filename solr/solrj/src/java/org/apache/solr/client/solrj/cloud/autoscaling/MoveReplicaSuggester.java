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

import java.util.List;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.Clause.Violation;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy.Suggester;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.util.Pair;

public class MoveReplicaSuggester extends Suggester {

  @Override
  SolrRequest init() {
    SolrRequest operation = tryEachNode(true);
    if (operation == null) operation = tryEachNode(false);
    return operation;
  }

  SolrRequest tryEachNode(boolean strict) {
    //iterate through elements and identify the least loaded
    List<Clause.Violation> leastSeriousViolation = null;
    Integer targetNodeIndex = null;
    Integer sourceNodeIndex = null;
    ReplicaInfo fromReplicaInfo = null;
    for (Pair<ReplicaInfo, Row> fromReplica : getValidReplicas(true, true, -1)) {
      Row fromRow = fromReplica.second();
      ReplicaInfo replicaInfo = fromReplica.first();
      String coll = replicaInfo.collection;
      String shard = replicaInfo.shard;
      Pair<Row, ReplicaInfo> pair = fromRow.removeReplica(coll, shard);
      Row tmpRow = pair.first();
      if (tmpRow == null) {
        //no such replica available
        continue;
      }
      tmpRow.violations.clear();

      final int i = getMatrix().indexOf(fromRow);
      for (int j = getMatrix().size() - 1; j > i; j--) {
        Row targetRow = getMatrix().get(j);
        if (!isAllowed(targetRow.node, Hint.TARGET_NODE)) continue;
        targetRow = targetRow.addReplica(coll, shard);
        targetRow.violations.clear();
        List<Violation> errs = testChangedMatrix(strict, getModifiedMatrix(getModifiedMatrix(getMatrix(), tmpRow, i), targetRow, j));
        if (!containsNewErrors(errs) && isLessSerious(errs, leastSeriousViolation)) {
          leastSeriousViolation = errs;
          targetNodeIndex = j;
          sourceNodeIndex = i;
          fromReplicaInfo = replicaInfo;
        }
      }
    }
    if (targetNodeIndex != null && sourceNodeIndex != null) {
      getMatrix().set(sourceNodeIndex, getMatrix().get(sourceNodeIndex).removeReplica(fromReplicaInfo.collection, fromReplicaInfo.shard).first());
      getMatrix().set(targetNodeIndex, getMatrix().get(targetNodeIndex).addReplica(fromReplicaInfo.collection, fromReplicaInfo.shard));
      return new CollectionAdminRequest.MoveReplica(
          fromReplicaInfo.collection,
          fromReplicaInfo.name,
          getMatrix().get(targetNodeIndex).node);
    }
    return null;
  }

}
