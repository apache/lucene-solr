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

package org.apache.solr.client.solrj.routing;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.junit.Test;

public class NodePreferenceRulesComparatorTest extends SolrTestCaseJ4 {

  @Test
  public void replicaLocationTest() {
    List<Replica> replicas = getBasicReplicaList();

    // replicaLocation rule
    List<PreferenceRule> rules = PreferenceRule.from(ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION + ":http://host2:8983");
    NodePreferenceRulesComparator comparator = new NodePreferenceRulesComparator(rules, null);
    replicas.sort(comparator);
    assertEquals("node2", replicas.get(0).getNodeName());
    assertEquals("node1", replicas.get(1).getNodeName());

  }

  public void replicaTypeTest() {
    List<Replica> replicas = getBasicReplicaList();

    List<PreferenceRule> rules = PreferenceRule.from(ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT," +
        ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG");
    NodePreferenceRulesComparator comparator = new NodePreferenceRulesComparator(rules, null);

    replicas.sort(comparator);
    assertEquals("node1", replicas.get(0).getNodeName());
    assertEquals("node2", replicas.get(1).getNodeName());

    // reversed rule
    rules = PreferenceRule.from(ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG," +
        ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT");
    comparator = new NodePreferenceRulesComparator(rules, null);

    replicas.sort(comparator);
    assertEquals("node2", replicas.get(0).getNodeName());
    assertEquals("node1", replicas.get(1).getNodeName());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void replicaTypeAndReplicaLocationTest() {
    List<Replica> replicas = getBasicReplicaList();
    // Add a replica so that sorting by replicaType:TLOG can cause a tie
    replicas.add(
        new Replica(
            "node4",
            map(
                ZkStateReader.BASE_URL_PROP, "http://host2_2:8983/solr",
                ZkStateReader.NODE_NAME_PROP, "node4",
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "TLOG"
            ),"collection1","shard1"
        )
    );

    List<PreferenceRule> rules = PreferenceRule.from(
        ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT," +
            ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG," +
            ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION + ":http://host2_2");
    NodePreferenceRulesComparator comparator = new NodePreferenceRulesComparator(rules, null);

    replicas.sort(comparator);
    assertEquals("node1", replicas.get(0).getNodeName());
    assertEquals("node4", replicas.get(1).getNodeName());
    assertEquals("node2", replicas.get(2).getNodeName());
    assertEquals("node3", replicas.get(3).getNodeName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void badRuleTest() {
    try {
      PreferenceRule.from(ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE);
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid shards.preference rule: " + ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE, e.getMessage());
      throw e;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void unknownRuleTest() {
    List<Replica> replicas = getBasicReplicaList();
    List<PreferenceRule> rules = PreferenceRule.from("badRule:test");
    try {
      replicas.sort(new NodePreferenceRulesComparator(rules, null));
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid shards.preference type: badRule", e.getMessage());
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Replica> getBasicReplicaList() {
    List<Replica> replicas = new ArrayList<Replica>();
    replicas.add(
        new Replica(
            "node1",
            map(
                ZkStateReader.BASE_URL_PROP, "http://host1:8983/solr",
                ZkStateReader.NODE_NAME_PROP, "node1",
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "NRT"
            ),"collection1","shard1"
        )
    );
    replicas.add(
        new Replica(
            "node2",
            map(
                ZkStateReader.BASE_URL_PROP, "http://host2:8983/solr",
                ZkStateReader.NODE_NAME_PROP, "node2",
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "TLOG"
            ),"collection1","shard1"
        )
    );
    replicas.add(
        new Replica(
            "node3",
            map(
                ZkStateReader.BASE_URL_PROP, "http://host2_2:8983/solr",
                ZkStateReader.NODE_NAME_PROP, "node3",
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "PULL"
            ),"collection1","shard1"
        )
    );
    return replicas;
  }
}
