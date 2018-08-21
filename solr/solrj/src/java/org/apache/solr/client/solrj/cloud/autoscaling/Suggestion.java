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
import java.util.List;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.cloud.autoscaling.Violation.ReplicaInfoAndErr;
import org.apache.solr.common.util.Pair;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

public class Suggestion {
  static class Ctx {
    public Policy.Session session;
    public Violation violation;
    private List<Suggester.SuggestionInfo> suggestions = new ArrayList<>();

    SolrRequest addSuggestion(Suggester suggester) {
      SolrRequest op = suggester.getSuggestion();
      if (op != null) {
        session = suggester.getSession();
        suggestions.add(new Suggester.SuggestionInfo(violation,
            ((V2RequestSupport) op.setUseV2(true)).getV2Request()));
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
  }


  static void perNodeSuggestions(Ctx ctx) {
    if (ctx.violation == null) return;
    for (ReplicaInfoAndErr e : ctx.violation.getViolatingReplicas()) {
      Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
          .forceOperation(true)
          .hint(Suggester.Hint.COLL_SHARD, new Pair<>(e.replicaInfo.getCollection(), e.replicaInfo.getShard()))
          .hint(Suggester.Hint.SRC_NODE, e.replicaInfo.getNode());
      if (ctx.addSuggestion(suggester) == null) break;
    }
  }

}
