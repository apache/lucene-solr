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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester.Hint;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CoreAdminParams.NODE;

public class PolicyHelper {
  private static ThreadLocal<Map<String, String>> policyMapping = new ThreadLocal<>();
  public static List<ReplicaPosition> getReplicaLocations(String collName, AutoScalingConfig autoScalingConfig,
                                                          SolrCloudManager cloudManager,
                                                          Map<String, String> optionalPolicyMapping,
                                                          List<String> shardNames,
                                                          int nrtReplicas,
                                                          int tlogReplicas,
                                                          int pullReplicas,
                                                          List<String> nodesList) {
    List<ReplicaPosition> positions = new ArrayList<>();
    ClusterStateProvider stateProvider = new DelegatingClusterStateProvider(cloudManager.getClusterStateProvider()) {
        @Override
        public String getPolicyNameByCollection(String coll) {
          return policyMapping.get() != null && policyMapping.get().containsKey(coll) ?
              optionalPolicyMapping.get(coll) :
              delegate.getPolicyNameByCollection(coll);
        }
      };
    SolrCloudManager delegatingManager = new DelegatingCloudManager(cloudManager) {
      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return stateProvider;
      }
    };

    policyMapping.set(optionalPolicyMapping);
    Policy.Session session = null;
    try {
      session = SESSION_REF.get() != null ?
          SESSION_REF.get().initOrGet(delegatingManager, autoScalingConfig.getPolicy()) :
          autoScalingConfig.getPolicy().createSession(delegatingManager);

      Map<Replica.Type, Integer> typeVsCount = new EnumMap<>(Replica.Type.class);
      typeVsCount.put(Replica.Type.NRT, nrtReplicas);
      typeVsCount.put(Replica.Type.TLOG, tlogReplicas);
      typeVsCount.put(Replica.Type.PULL, pullReplicas);
      for (String shardName : shardNames) {
        int idx = 0;
        for (Map.Entry<Replica.Type, Integer> e : typeVsCount.entrySet()) {
          for (int i = 0; i < e.getValue(); i++) {
            Suggester suggester = session.getSuggester(ADDREPLICA)
                .hint(Hint.REPLICATYPE, e.getKey())
                .hint(Hint.COLL_SHARD, new Pair<>(collName, shardName));
            if (nodesList != null) {
              for (String nodeName : nodesList) {
                suggester = suggester.hint(Hint.TARGET_NODE, nodeName);
              }
            }
            SolrRequest op = suggester.getSuggestion();
            if (op == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No node can satisfy the rules " +
                  Utils.toJSONString(Utils.getDeepCopy(session.expandedClauses, 4, true)));
            }
            session = suggester.getSession();
            positions.add(new ReplicaPosition(shardName, ++idx, e.getKey(), op.getParams().get(NODE)));
          }
        }
      }
    } finally {
      if (session != null && SESSION_REF.get() != null) SESSION_REF.get().updateSession(session);
      policyMapping.remove();
    }
    return positions;
  }


  public static final int SESSION_EXPIRY = 180;//3 seconds
  public static ThreadLocal<Long> REF_VERSION = new ThreadLocal<>();

  public static MapWriter getDiagnostics(Policy policy, SolrClientCloudManager cloudManager) {
    Policy.Session session = policy.createSession(cloudManager);
    List<Row> sorted = session.getSorted();
    List<Violation> violations = session.getViolations();

    List<Preference> clusterPreferences = policy.getClusterPreferences();

    List<Map<String, Object>> sortedNodes = new ArrayList<>(sorted.size());
    for (Row row : sorted) {
      Map<String, Object> map = Utils.makeMap("node", row.node);
      for (Cell cell : row.getCells()) {
        for (Preference clusterPreference : clusterPreferences) {
          Policy.SortParam name = clusterPreference.getName();
          if (cell.getName().equalsIgnoreCase(name.name())) {
            map.put(name.name(), cell.getValue());
            break;
          }
        }
      }
      sortedNodes.add(map);
    }

    return ew -> {
      ew.put("sortedNodes", sortedNodes);
      ew.put("violations", violations);
    };


  }

  public static List<Suggester.SuggestionInfo> getSuggestions(AutoScalingConfig autoScalingConf, SolrCloudManager cloudManager) {
    Policy policy = autoScalingConf.getPolicy();
    Suggestion.SuggestionCtx suggestionCtx = new Suggestion.SuggestionCtx();
    suggestionCtx.session = policy.createSession(cloudManager);
    List<Violation> violations = suggestionCtx.session.getViolations();
    for (Violation violation : violations) {
      Suggestion.ConditionType tagType = Suggestion.getTagType(violation.getClause().isPerCollectiontag() ?
          violation.getClause().tag.name :
          violation.getClause().globalTag.name);
      tagType.getSuggestions(suggestionCtx.setViolation(violation));
      suggestionCtx.violation = null;
    }
    return suggestionCtx.getSuggestions();
  }

  public static class SessionRef {
    private final AtomicLong myVersion = new AtomicLong(0);
    AtomicInteger refCount = new AtomicInteger();
    private Policy.Session session;
    long lastUsedTime;

    public SessionRef() {
    }

    public long getRefVersion(){
      return myVersion.get();
    }


    public void decref(long version) {
      synchronized (SessionRef.class) {
        if (session == null) return;
        if(myVersion.get() != version) return;
        if (refCount.decrementAndGet() <= 0) {
          session = null;
          lastUsedTime = 0;
        }
      }
    }

    public int getRefCount() {
      return refCount.get();
    }

    public Policy.Session get() {
      synchronized (SessionRef.class) {
        if (session == null) return null;
        if (TimeUnit.SECONDS.convert(System.nanoTime() - lastUsedTime, TimeUnit.NANOSECONDS) > SESSION_EXPIRY) {
          session = null;
          return null;
        } else {
          REF_VERSION.set(myVersion.get());
          refCount.incrementAndGet();
          return session;
        }
      }
    }

    public Policy.Session initOrGet(SolrCloudManager cloudManager, Policy policy) {
      synchronized (SessionRef.class) {
        Policy.Session session = get();
        if (session != null) return session;
        this.session = policy.createSession(cloudManager);
        myVersion.incrementAndGet();
        lastUsedTime = System.nanoTime();
        REF_VERSION.set(myVersion.get());
        refCount.set(1);
        return this.session;
      }
    }


    private void updateSession(Policy.Session session) {
      this.session = session;
      lastUsedTime = System.nanoTime();
    }
  }

  public static void clearFlagAndDecref(SessionRef policySessionRef) {
    Long refVersion =  REF_VERSION.get();
    if (refVersion != null) policySessionRef.decref(refVersion);
    REF_VERSION.remove();
  }
  public static PolicyHelper.SessionRef getPolicySessionRef(SolrCloudManager cloudManager){
    return (SessionRef) cloudManager.getObjectCache().computeIfAbsent(SessionRef.class.getName(), s -> new SessionRef());
  }


  public static ThreadLocal<SessionRef> SESSION_REF = new ThreadLocal<>();


}
