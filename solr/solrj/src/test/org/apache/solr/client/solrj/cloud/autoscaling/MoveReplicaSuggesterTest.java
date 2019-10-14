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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Pair;
import org.junit.Test;

import static org.apache.solr.client.solrj.cloud.autoscaling.MoveReplicaSuggester.leaderLast;

public class MoveReplicaSuggesterTest extends SolrTestCaseJ4 {
  private String CORE = "collection_shard1_replica_n1";
  private String COLLECTION = "collection";
  private String SHARD = "shard1";
  private Map<String, Object> IS_LEADER = Collections.singletonMap(ZkStateReader.LEADER_PROP, true);
  private String NODE = "127.0.0.1:8080_solr";
  private Replica.Type REPLICA_TYPE = Replica.Type.NRT;
  private Row ROW = null;

  private ReplicaInfo REPLICA_INFO_ONE = new ReplicaInfo("core_node1", CORE, COLLECTION, SHARD, REPLICA_TYPE, NODE, IS_LEADER);
  private ReplicaInfo REPLICA_INFO_TWO = new ReplicaInfo("core_node2", CORE, COLLECTION, SHARD, REPLICA_TYPE, NODE, null);
  private ReplicaInfo REPLICA_INFO_THREE = new ReplicaInfo("core_node3", CORE, COLLECTION, SHARD, REPLICA_TYPE, NODE, IS_LEADER);
  private ReplicaInfo REPLICA_INFO_FOUR = new ReplicaInfo("core_node4", CORE, COLLECTION, SHARD, REPLICA_TYPE, NODE, null);

  private Pair<ReplicaInfo, Row> PAIR_ONE = new Pair<>(REPLICA_INFO_ONE, ROW);
  private Pair<ReplicaInfo, Row> PAIR_TWO = new Pair<>(REPLICA_INFO_TWO, ROW);
  private Pair<ReplicaInfo, Row> PAIR_THREE = new Pair<>(REPLICA_INFO_THREE, ROW);
  private Pair<ReplicaInfo, Row> PAIR_FOUR = new Pair<>(REPLICA_INFO_FOUR, ROW);

  @Test
  public void assertLeaderProperties() {
    assertTrue(REPLICA_INFO_ONE.isLeader);
    assertFalse(REPLICA_INFO_TWO.isLeader);
    assertTrue(REPLICA_INFO_THREE.isLeader);
    assertFalse(REPLICA_INFO_FOUR.isLeader);
  }

  @Test
  public void sortReplicasValidate() {
    List<Pair<ReplicaInfo, Row>> validReplicas = new ArrayList<Pair<ReplicaInfo, Row>>() {
      {
        add(PAIR_ONE);
        add(PAIR_FOUR);
        add(PAIR_TWO);
      }
    };
    if (random().nextBoolean()) {
      Collections.shuffle(validReplicas, random());
    }
    validReplicas.sort(leaderLast);

    assertFalse(isReplicaLeader(validReplicas, 0));
    assertFalse(isReplicaLeader(validReplicas, 1));
    assertTrue(isReplicaLeader(validReplicas, 2));
  }

  @Test
  public void sortReplicasValidateLeadersMultipleLeadersComeLast() {
    List<Pair<ReplicaInfo, Row>> validReplicas = new ArrayList<Pair<ReplicaInfo, Row>>() {
      {
        add(PAIR_THREE);
        add(PAIR_ONE);
        add(PAIR_FOUR);
        add(PAIR_TWO);
      }
    };
    if (random().nextBoolean()) {
      Collections.shuffle(validReplicas, random());
    }
    validReplicas.sort(leaderLast);

    assertFalse(isReplicaLeader(validReplicas, 0));
    assertFalse(isReplicaLeader(validReplicas, 1));
    assertTrue(isReplicaLeader(validReplicas, 2));
    assertTrue(isReplicaLeader(validReplicas, 3));
  }

  private boolean isReplicaLeader(List<Pair<ReplicaInfo, Row>> replicas, int index) {
    return replicas.get(index).first().isLeader;
  }

}
