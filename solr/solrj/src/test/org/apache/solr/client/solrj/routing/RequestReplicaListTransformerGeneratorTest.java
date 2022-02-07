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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL
public class RequestReplicaListTransformerGeneratorTest extends SolrTestCaseJ4 {

  @Test
  public void testNodePreferenceRulesBase() {
    RequestReplicaListTransformerGenerator generator = new RequestReplicaListTransformerGenerator();
    ModifiableSolrParams params = new ModifiableSolrParams();
    List<Replica> replicas = getBasicReplicaList();

    String rulesParam = ShardParams.SHARDS_PREFERENCE_REPLICA_BASE + ":stable:dividend:routingPreference";

    params.add("routingPreference", "0");
    params.add(ShardParams.SHARDS_PREFERENCE, rulesParam);

    ReplicaListTransformer rlt = generator.getReplicaListTransformer(params);
    rlt.transform(replicas);
    assertEquals("node1", getHost(replicas.get(0).getNodeName()));
    assertEquals("node2", getHost(replicas.get(1).getNodeName()));
    assertEquals("node3", getHost(replicas.get(2).getNodeName()));

    params.set("routingPreference", "1");
    rlt = generator.getReplicaListTransformer(params);
    rlt.transform(replicas);
    assertEquals("node2", getHost(replicas.get(0).getNodeName()));
    assertEquals("node3", getHost(replicas.get(1).getNodeName()));
    assertEquals("node1", getHost(replicas.get(2).getNodeName()));

    params.set("routingPreference", "2");
    rlt = generator.getReplicaListTransformer(params);
    rlt.transform(replicas);
    assertEquals("node3", getHost(replicas.get(0).getNodeName()));
    assertEquals("node1", getHost(replicas.get(1).getNodeName()));
    assertEquals("node2", getHost(replicas.get(2).getNodeName()));

    params.set("routingPreference", "3");
    rlt = generator.getReplicaListTransformer(params);
    rlt.transform(replicas);
    assertEquals("node1", getHost(replicas.get(0).getNodeName()));
    assertEquals("node2", getHost(replicas.get(1).getNodeName()));
    assertEquals("node3", getHost(replicas.get(2).getNodeName()));
  }
  
  private String getHost(final String nodeName) {
    final int colonAt = nodeName.indexOf(':');
    return colonAt != -1 ? nodeName.substring(0,colonAt) : nodeName.substring(0, nodeName.indexOf('_'));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void replicaTypeAndReplicaBase() {
    RequestReplicaListTransformerGenerator generator = new RequestReplicaListTransformerGenerator();
    ModifiableSolrParams params = new ModifiableSolrParams();
    List<Replica> replicas = getBasicReplicaList();

    // Add a replica so that sorting by replicaType:TLOG can cause a tie
    replicas.add(
        new Replica(
            "node4",
            map(
                ZkStateReader.NODE_NAME_PROP, "node4:8983_solr",
                ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName("node4:8983_solr", "https"),
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "TLOG"
            ), "c1","s1"
        )
    );

    // Add a PULL replica so that there's a tie for "last place"
    replicas.add(
        new Replica(
            "node5",
            map(
                ZkStateReader.NODE_NAME_PROP, "node5:8983_solr",
                ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName("node5:8983_solr", "https"),
                ZkStateReader.CORE_NAME_PROP, "collection1",
                ZkStateReader.REPLICA_TYPE, "PULL"
            ), "c1","s1"
        )
    );

    // replica leader status, replicaType and replicaBase combined rule param
    String rulesParam = ShardParams.SHARDS_PREFERENCE_REPLICA_LEADER + ":true," +
        ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT," +
        ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG," +
        ShardParams.SHARDS_PREFERENCE_REPLICA_BASE + ":stable:dividend:routingPreference";

    params.add("routingPreference", "0");
    params.add(ShardParams.SHARDS_PREFERENCE, rulesParam);
    ReplicaListTransformer rlt = generator.getReplicaListTransformer(params);
    rlt.transform(replicas);
    assertEquals("node1", getHost(replicas.get(0).getNodeName()));
    assertEquals("node2", getHost(replicas.get(1).getNodeName()));
    assertEquals("node4", getHost(replicas.get(2).getNodeName()));
    assertEquals("node3", getHost(replicas.get(3).getNodeName()));
    assertEquals("node5", getHost(replicas.get(4).getNodeName()));

    params.set("routingPreference", "1");
    rlt = generator.getReplicaListTransformer(params);
    rlt.transform(replicas);
    assertEquals("node1", getHost(replicas.get(0).getNodeName()));
    assertEquals("node4", getHost(replicas.get(1).getNodeName()));
    assertEquals("node2", getHost(replicas.get(2).getNodeName()));
    assertEquals("node5", getHost(replicas.get(3).getNodeName()));
    assertEquals("node3", getHost(replicas.get(4).getNodeName()));
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
                ZkStateReader.REPLICA_TYPE, "NRT"
            ),"c1","s1"
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
            ),"c1","s1"
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
            ),"c1","s1"
        )
    );
    return replicas;
  }
}
