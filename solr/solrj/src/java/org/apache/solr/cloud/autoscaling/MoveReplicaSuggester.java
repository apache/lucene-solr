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

package org.apache.solr.cloud.autoscaling;

import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.autoscaling.Policy.Suggester;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;
import static org.apache.solr.common.params.CoreAdminParams.NODE;
import static org.apache.solr.common.params.CoreAdminParams.REPLICA;

public class MoveReplicaSuggester extends Suggester {

  @Override
  SolrRequest init() {
    SolrRequest operation = tryEachNode(true);
    if (operation == null) operation = tryEachNode(false);
    return operation;
  }

  SolrRequest tryEachNode(boolean strict) {
    //iterate through elements and identify the least loaded
    for (Pair<Policy.ReplicaInfo, Row> fromReplica : getValidReplicas(true, true, -1)) {
      Row fromRow = fromReplica.second();
      String coll = fromReplica.first().collection;
      String shard = fromReplica.first().shard;
      Pair<Row, Policy.ReplicaInfo> pair = fromRow.removeReplica(coll, shard);
      Row tmpRow = pair.first();
      if (tmpRow == null) {
        //no such replica available
        continue;
      }
      tmpRow.violations.clear();
      for (Clause clause : session.expandedClauses) {
        if (strict || clause.strict) {
          clause.test(tmpRow);
        }
      }
      final int i = getMatrix().indexOf(fromRow);
      if (tmpRow.violations.isEmpty()) {
        for (int j = getMatrix().size() - 1; j > i; j--) {
          Row targetRow = getMatrix().get(j);
          if (!isAllowed(targetRow.node, Hint.TARGET_NODE)) continue;
          targetRow = targetRow.addReplica(coll, shard);
          targetRow.violations.clear();
          for (Clause clause : session.expandedClauses) {
            if (strict || clause.strict) clause.test(targetRow);
          }
          if (targetRow.violations.isEmpty()) {
            getMatrix().set(i, getMatrix().get(i).removeReplica(coll, shard).first());
            getMatrix().set(j, getMatrix().get(j).addReplica(coll, shard));
            return new CollectionAdminRequest.MoveReplica(
                coll,
                pair.second().name,
                targetRow.node);
          }
        }
      }
    }
    return null;
  }

}
