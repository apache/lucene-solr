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
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL // this test is all about http://
public class NodePreferenceRulesComparatorTest extends SolrTestCaseJ4 {

  @Test
  public void replicaLocationTest() {
    List<Replica> replicas = getBasicReplicaList();

    // replicaLocation rule
    List<PreferenceRule> rules = PreferenceRule.from(ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION + ":http://node2:8983");
    NodePreferenceRulesComparator comparator = new NodePreferenceRulesComparator(rules, null);
    replicas.sort(comparator);
    assertEquals("node2", getHost(replicas.get(0).getNodeName()));
    assertEquals("node1", getHost(replicas.get(1).getNodeName()));

  }

  public void replicaTypeTest() {
    List<Replica> replicas = getBasicReplicaList();

    List<PreferenceRule> rules = PreferenceRule.from(ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT," +
        ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG");
    NodePreferenceRulesComparator comparator = new NodePreferenceRulesComparator(rules, null);

    replicas.sort(comparator);
    assertEquals("node1", getHost(replicas.get(0).getNodeName()));
    assertEquals("node2", getHost(replicas.get(1).getNodeName()));

    // reversed rule
    rules = PreferenceRule.from(ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG," +
        ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT");
    comparator = new NodePreferenceRulesComparator(rules, null);

    replicas.sort(comparator);
    assertEquals("node2", getHost(replicas.get(0).getNodeName()));
    assertEquals("node1", getHost(replicas.get(1).getNodeName()));
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
                ZkStateReader.NODE_NAME_PROP, "node4:8983_solr",
                ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName("node4:8983_solr", "http"),
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "TLOG"
            ),"collection1","shard1"
        )
    );

    List<PreferenceRule> rules = PreferenceRule.from(
        ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT," +
            ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG," +
            ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION + ":http://node4");
    NodePreferenceRulesComparator comparator = new NodePreferenceRulesComparator(rules, null);

    replicas.sort(comparator);
    assertEquals("node1", getHost(replicas.get(0).getNodeName()));
    assertEquals("node4", getHost(replicas.get(1).getNodeName()));
    assertEquals("node2", getHost(replicas.get(2).getNodeName()));
    assertEquals("node3", getHost(replicas.get(3).getNodeName()));
  }

  @Test
  public void replicaLeaderTest() {
    List<Replica> replicas = getBasicReplicaList();
    // Add a non-leader NRT replica so that sorting by replica.type:NRT will cause a tie, order is decided by leader status
    replicas.add(
        new Replica(
            "node4",
            map(ZkStateReader.BASE_URL_PROP, "http://host2:8983/solr",
                ZkStateReader.NODE_NAME_PROP, "node4:8983_solr",
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "NRT"
            ), "collection1", "shard1"
        )
    );
    // Prefer non-leader only, therefore node1 has lowest priority
    List<PreferenceRule> rules = PreferenceRule.from(ShardParams.SHARDS_PREFERENCE_REPLICA_LEADER + ":false");
    NodePreferenceRulesComparator comparator = new NodePreferenceRulesComparator(rules, null);
    replicas.sort(comparator);
    assertEquals("node1:8983_solr", replicas.get(3).getNodeName());

    // Prefer NRT replica, followed by non-leader
    rules = PreferenceRule.from(
        ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT," +
            ShardParams.SHARDS_PREFERENCE_REPLICA_LEADER + ":false");
    comparator = new NodePreferenceRulesComparator(rules, null);
    replicas.sort(comparator);
    assertEquals("node4", getHost(replicas.get(0).getNodeName()));
    assertEquals("node1", getHost(replicas.get(1).getNodeName()));

    // Prefer non-leader first, followed by PULL replica and location host2
    rules = PreferenceRule.from(
        ShardParams.SHARDS_PREFERENCE_REPLICA_LEADER + ":false," +
            ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":PULL," +
            ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION + ":http://host2");
    comparator = new NodePreferenceRulesComparator(rules, null);

    replicas.sort(comparator);
    assertEquals("node3", getHost(replicas.get(0).getNodeName()));
    assertEquals("node4", getHost(replicas.get(1).getNodeName()));
    assertEquals("node2", getHost(replicas.get(2).getNodeName()));
    assertEquals("node1", getHost(replicas.get(3).getNodeName()));

    // make sure a single leader only still gets selected
    List<Replica> onlyLeader = new ArrayList<>();
    onlyLeader.add(
        new Replica(
            "node1",
            map(
                ZkStateReader.NODE_NAME_PROP, "node1:8983_solr",
                ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName("node1:8983_solr", "http"),
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "NRT",
                ZkStateReader.LEADER_PROP, "true"
            ),"collection1","shard1"
        )
    );
    rules = PreferenceRule.from(ShardParams.SHARDS_PREFERENCE_REPLICA_LEADER + ":false");
    comparator = new NodePreferenceRulesComparator(rules, null);
    replicas.sort(comparator);
    assertEquals("node1:8983_solr", onlyLeader.get(0).getNodeName());
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
                ZkStateReader.NODE_NAME_PROP, "node1:8983_solr",
                ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName("node1:8983_solr", "http"),
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "NRT",
                ZkStateReader.LEADER_PROP, "true"
            ),"collection1","shard1"
        )
    );
    replicas.add(
        new Replica(
            "node2",
            map(
                ZkStateReader.NODE_NAME_PROP, "node2:8983_solr",
                ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName("node2:8983_solr", "http"),
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "TLOG"
            ),"collection1","shard1"
        )
    );
    replicas.add(
        new Replica(
            "node3",
            map(
                ZkStateReader.NODE_NAME_PROP, "node3:8983_solr",
                ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName("node3:8983_solr", "http"),
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "PULL"
            ),"collection1","shard1"
        )
    );
    return replicas;
  }

  private String getHost(final String nodeName) {
    final int colonAt = nodeName.indexOf(':');
    return colonAt != -1 ? nodeName.substring(0,colonAt) : nodeName.substring(0, nodeName.indexOf('_'));
  }
}
