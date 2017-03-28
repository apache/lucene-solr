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

package org.apache.solr.recipe;

import java.util.Map;

import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.recipe.RuleSorter.BaseSuggester;
import org.apache.solr.recipe.RuleSorter.Session;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;
import static org.apache.solr.common.params.CoreAdminParams.NODE;
import static org.apache.solr.common.params.CoreAdminParams.REPLICA;

public class MoveReplicaSuggester  extends BaseSuggester{

  MoveReplicaSuggester(String coll, String shard, Session session) {
    super(coll, shard, session);
  }

  Map get() {
    Map operation = tryEachNode(true);
    if (operation == null) operation = tryEachNode(false);
    return operation;
  }

  Map tryEachNode(boolean strict) {
    //iterate through elements and identify the least loaded
    for (int i = 0; i < matrix.size(); i++) {
      Row fromRow = matrix.get(i);
      Pair<Row, RuleSorter.ReplicaStat> pair = fromRow.removeReplica(coll, shard);
      fromRow = pair.first();
      if(fromRow == null){
        //no such replica available
        continue;
      }

      for (Clause clause : session.getRuleSorter().clauses) {
        if (strict || clause.strict) clause.test(fromRow);
      }
      if (fromRow.violations.isEmpty()) {
        for (int j = matrix.size() - 1; j > i; i--) {
          Row targetRow = matrix.get(i);
          targetRow = targetRow.addReplica(coll, shard);
          for (Clause clause : session.getRuleSorter().clauses) {
            if (strict || clause.strict) clause.test(targetRow);
          }
          if (targetRow.violations.isEmpty()) {
                return Utils.makeMap("operation", MOVEREPLICA.toLower(),
                    COLLECTION_PROP, coll,
                    SHARD_ID_PROP, shard,
                    NODE, fromRow.node,
                    REPLICA, pair.second().name,
                    "target", targetRow.node);
          }
        }
      }
    }
    return null;
  }

}
