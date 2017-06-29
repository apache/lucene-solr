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
import org.apache.solr.client.solrj.cloud.autoscaling.Policy.Suggester;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;

class AddReplicaSuggester extends Suggester {

  SolrRequest init() {
    SolrRequest operation = tryEachNode(true);
    if (operation == null) operation = tryEachNode(false);
    return operation;
  }

  SolrRequest tryEachNode(boolean strict) {
    String coll = (String) hints.get(Hint.COLL);
    String shard = (String) hints.get(Hint.SHARD);
    if (coll == null || shard == null)
      throw new RuntimeException("add-replica requires 'collection' and 'shard'");
    //iterate through elements and identify the least loaded

    List<Clause.Violation> leastSeriousViolation = null;
    Integer targetNodeIndex = null;
    for (int i = getMatrix().size() - 1; i >= 0; i--) {
      Row row = getMatrix().get(i);
      if (!isAllowed(row.node, Hint.TARGET_NODE)) continue;
      Row tmpRow = row.addReplica(coll, shard);
      tmpRow.violations.clear();

      List<Clause.Violation> errs = testChangedMatrix(strict, getModifiedMatrix(getMatrix(), tmpRow, i));
      if(!containsNewErrors(errs)) {
        if(isLessSerious(errs, leastSeriousViolation)){
          leastSeriousViolation = errs;
          targetNodeIndex = i;
        }
      }
    }

    if (targetNodeIndex != null) {// there are no rule violations
      getMatrix().set(targetNodeIndex, getMatrix().get(targetNodeIndex).addReplica(coll, shard));
      return CollectionAdminRequest
          .addReplicaToShard(coll, shard)
          .setNode(getMatrix().get(targetNodeIndex).node);
    }

    return null;
  }


}
