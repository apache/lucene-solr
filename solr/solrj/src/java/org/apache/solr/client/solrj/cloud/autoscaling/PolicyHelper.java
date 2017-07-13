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
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy.Suggester.Hint;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CoreAdminParams.NODE;

public class PolicyHelper {
  public static List<ReplicaPosition> getReplicaLocations(String collName, AutoScalingConfig autoScalingConfig,
                                                          ClusterDataProvider cdp,
                                                          Map<String, String> optionalPolicyMapping,
                                                          List<String> shardNames,
                                                          int nrtReplicas,
                                                          int tlogReplicas,
                                                          int pullReplicas,
                                                          List<String> nodesList) {
    List<ReplicaPosition> positions = new ArrayList<>();
    if (optionalPolicyMapping != null) {
      final ClusterDataProvider delegate = cdp;
      cdp = new ClusterDataProvider() {
        @Override
        public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
          return delegate.getNodeValues(node, tags);
        }

        @Override
        public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
          return delegate.getReplicaInfo(node, keys);
        }

        @Override
        public Collection<String> getNodes() {
          return delegate.getNodes();
        }

        @Override
        public String getPolicyNameByCollection(String coll) {
          return optionalPolicyMapping.containsKey(coll) ?
              optionalPolicyMapping.get(coll) :
              delegate.getPolicyNameByCollection(coll);
        }
      };
    }

    Policy.Session session = autoScalingConfig.getPolicy().createSession(cdp);
    Map<Replica.Type, Integer> typeVsCount = new EnumMap<>(Replica.Type.class);
    typeVsCount.put(Replica.Type.NRT, nrtReplicas);
    typeVsCount.put(Replica.Type.TLOG, tlogReplicas);
    typeVsCount.put(Replica.Type.PULL, pullReplicas);
    for (String shardName : shardNames) {
      int idx = 0;
      for (Map.Entry<Replica.Type, Integer> e : typeVsCount.entrySet()) {
        for (int i = 0; i < e.getValue(); i++) {
          Policy.Suggester suggester = session.getSuggester(ADDREPLICA)
              .hint(Hint.COLL, collName)
              .hint(Hint.REPLICATYPE, e.getKey())
              .hint(Hint.SHARD, shardName);
          if (nodesList != null) {
            for (String nodeName : nodesList) {
              suggester = suggester.hint(Hint.TARGET_NODE, nodeName);
            }
          }
          SolrRequest op = suggester.getOperation();
          if (op == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No node can satisfy the rules " + Utils.toJSONString(Utils.getDeepCopy(session.expandedClauses, 4, true)));
          }
          session = suggester.getSession();
          positions.add(new ReplicaPosition(shardName, ++idx, e.getKey(), op.getParams().get(NODE)));
        }
      }
    }

    return positions;
  }
}
