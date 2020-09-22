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

import org.apache.solr.common.util.Pair;

import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.ANY;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class NodeVariable extends VariableBase {
  public NodeVariable(Type type) {
    super(type);
  }

  @Override
  public void getSuggestions(Suggestion.Ctx ctx) {
    if (ctx.violation == null || ctx.violation.replicaCountDelta == 0) return;
    if (ctx.violation.replicaCountDelta > 0) {//there are more replicas than necessary
      for (int i = 0; i < Math.abs(ctx.violation.replicaCountDelta); i++) {
        Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
            .forceOperation(true)
            .hint(Suggester.Hint.SRC_NODE, ctx.violation.node)
            .hint(ctx.violation.shard.equals(ANY) ? Suggester.Hint.COLL : Suggester.Hint.COLL_SHARD,
                ctx.violation.shard.equals(ANY) ? ctx.violation.coll : new Pair<>(ctx.violation.coll, ctx.violation.shard));
        if(ctx.addSuggestion(suggester) == null) break;
      }
    }
  }
}