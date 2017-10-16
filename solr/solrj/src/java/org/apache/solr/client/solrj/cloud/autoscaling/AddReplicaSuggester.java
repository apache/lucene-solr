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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.Pair;

class AddReplicaSuggester extends Suggester {

  SolrRequest init() {
    SolrRequest operation = tryEachNode(true);
    if (operation == null) operation = tryEachNode(false);
    return operation;
  }

  SolrRequest tryEachNode(boolean strict) {
    Set<Pair<String, String>> shards = (Set<Pair<String, String>>) hints.getOrDefault(Hint.COLL_SHARD, Collections.emptySet());
    if (shards.isEmpty()) {
      throw new RuntimeException("add-replica requires 'collection' and 'shard'");
    }
    for (Pair<String,String> shard : shards) {
      Replica.Type type = Replica.Type.get((String) hints.get(Hint.REPLICATYPE));
      //iterate through elements and identify the least loaded
      List<Violation> leastSeriousViolation = null;
      Integer targetNodeIndex = null;
      for (int i = getMatrix().size() - 1; i >= 0; i--) {
        Row row = getMatrix().get(i);
        if (!row.isLive) continue;
        if (!isAllowed(row.node, Hint.TARGET_NODE)) continue;
        Row tmpRow = row.addReplica(shard.first(), shard.second(), type);

        List<Violation> errs = testChangedMatrix(strict, getModifiedMatrix(getMatrix(), tmpRow, i));
        if (!containsNewErrors(errs)) {
          if (isLessSerious(errs, leastSeriousViolation)) {
            leastSeriousViolation = errs;
            targetNodeIndex = i;
          }
        }
      }

      if (targetNodeIndex != null) {// there are no rule violations
        getMatrix().set(targetNodeIndex, getMatrix().get(targetNodeIndex).addReplica(shard.first(), shard.second(), type));
        return CollectionAdminRequest
            .addReplicaToShard(shard.first(), shard.second())
            .setType(type)
            .setNode(getMatrix().get(targetNodeIndex).node);
      }
    }

    return null;
  }


}
