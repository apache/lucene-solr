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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.cloud.autoscaling.Violation.ReplicaInfoAndErr;
import org.apache.solr.common.util.Pair;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class Suggestion {

  public enum Type {
    violation, repair, improvement, unresolved_violation;
  }

  static class Ctx {
    long endTime = -1;
    int max = Integer.MAX_VALUE;
    public Policy.Session session;
    public Violation violation;
    List<Suggester.SuggestionInfo> suggestions = new ArrayList<>();
    @SuppressWarnings({"rawtypes"})
    SolrRequest addSuggestion(Suggester suggester) {
      return addSuggestion(suggester, Type.violation);
    }

    @SuppressWarnings({"rawtypes"})
    SolrRequest addSuggestion(Suggester suggester, Type type) {
      @SuppressWarnings({"rawtypes"})
      SolrRequest op = suggester.getSuggestion();
      if (op != null) {
        session = suggester.getSession();
        suggestions.add(new Suggester.SuggestionInfo(violation,
            ((V2RequestSupport) op.setUseV2(true)).getV2Request(), type));
      }
      return op;
    }


    public Ctx setViolation(Violation violation) {
      this.violation = violation;
      return this;
    }

    public List<Suggester.SuggestionInfo> getSuggestions() {
      return suggestions;
    }

    public boolean hasTimedOut() {
      return session.cloudManager.getTimeSource().getTimeNs() >= endTime;

    }

    public boolean needMore() {
      return suggestions.size() < max && !hasTimedOut();
    }
  }

  static void suggestNegativeViolations(Suggestion.Ctx ctx, Function<Set<String>, List<String>> shardSorter) {
    if (ctx.violation.coll == null) return;
    Set<String> shardSet = new HashSet<>();
    if (!ctx.needMore()) return;
    for (Row node : ctx.session.matrix)
      node.forEachShard(ctx.violation.coll, (s, ri) -> {
        if (Policy.ANY.equals(ctx.violation.shard) || s.equals(ctx.violation.shard)) shardSet.add(s);
      });
    //Now, sort shards based on their index size ascending
    List<String> shards = shardSorter.apply(shardSet);
    outer:
    for (int i = 0; i < 5; i++) {
      if (!ctx.needMore()) break;
      int totalSuggestions = 0;
      for (String shard : shards) {
        Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
            .hint(Suggester.Hint.COLL_SHARD, new Pair<>(ctx.violation.coll, shard))
            .forceOperation(true);
        @SuppressWarnings({"rawtypes"})
        SolrRequest op = ctx.addSuggestion(suggester);
        if (op == null) continue;
        totalSuggestions++;
        boolean violationStillExists = false;
        for (Violation violation : suggester.session.getViolations()) {
          if (violation.getClause().original == ctx.violation.getClause().original) {
            violationStillExists = true;
            break;
          }
        }
        if (!violationStillExists) break outer;
      }
      if (totalSuggestions == 0) break;
    }
  }


  static void suggestPositiveViolations(Ctx ctx) {
    if (ctx.violation == null) return;
    Double currentDelta = ctx.violation.replicaCountDelta;
    for (ReplicaInfoAndErr e : ctx.violation.getViolatingReplicas()) {
      if (!ctx.needMore()) break;
      if (currentDelta <= 0) break;
      Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
          .forceOperation(true)
          .hint(Suggester.Hint.COLL_SHARD, new Pair<>(e.replicaInfo.getCollection(), e.replicaInfo.getShard()))
          .hint(Suggester.Hint.SRC_NODE, e.replicaInfo.getNode());
      if (ctx.addSuggestion(suggester) != null) {
        currentDelta--;
      }
    }
  }

}
