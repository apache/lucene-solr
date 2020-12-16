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

package org.apache.solr.common.cloud;


import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;

public class TestPerReplicaStates extends SolrCloudTestCase {
  @Before
  public void prepareCluster() throws Exception {
    configureCluster(4)
        .configure();
  }

  @After
  public void tearDownCluster() throws Exception {
    shutdownCluster();
  }

  public void testBasic() {
    PerReplicaStates.State rs = new PerReplicaStates.State("R1", State.ACTIVE, Boolean.FALSE, 1);
    assertEquals("R1:1:A", rs.asString);

    rs = new PerReplicaStates.State("R1", State.DOWN, Boolean.TRUE, 1);
    assertEquals("R1:1:D:L", rs.asString);
    rs = PerReplicaStates.State.parse (rs.asString);
    assertEquals(State.DOWN, rs.state);

  }

  public void testEntries() {
    PerReplicaStates entries = new PerReplicaStates("state.json", 0, ImmutableList.of("R1:2:A", "R1:1:A:L", "R1:0:D", "R2:0:D", "R3:0:A"));
    assertEquals(2, entries.get("R1").version);
    entries = new PerReplicaStates("state.json", 0, ImmutableList.of("R1:1:A:L", "R1:2:A", "R2:0:D", "R3:0:A", "R1:0:D"));
    assertEquals(2, entries.get("R1").version);
    assertEquals(2, entries.get("R1").getDuplicates().size());
    Set<String> modified = PerReplicaStates.findModifiedReplicas(entries,  new PerReplicaStates("state.json", 0, ImmutableList.of("R1:1:A:L", "R1:2:A", "R2:0:D", "R3:1:A", "R1:0:D")));
    assertEquals(1, modified.size());
    assertTrue(modified.contains("R3"));
    modified = PerReplicaStates.findModifiedReplicas( entries,
        new PerReplicaStates("state.json", 0, ImmutableList.of("R1:1:A:L", "R1:2:A", "R2:0:D", "R3:1:A", "R1:0:D", "R4:0:A")));
    assertEquals(2, modified.size());
    assertTrue(modified.contains("R3"));
    assertTrue(modified.contains("R4"));
    modified = PerReplicaStates.findModifiedReplicas( entries,
        new PerReplicaStates("state.json", 0, ImmutableList.of("R1:1:A:L", "R1:2:A", "R3:1:A", "R1:0:D", "R4:0:A")));
    assertEquals(3, modified.size());
    assertTrue(modified.contains("R3"));
    assertTrue(modified.contains("R4"));
    assertTrue(modified.contains("R2"));


  }

  public void testReplicaStateOperations() throws Exception {
    String root = "/testReplicaStateOperations";
    cluster.getZkClient().create(root, null, CreateMode.PERSISTENT, true);

    ImmutableList<String> states = ImmutableList.of("R1:2:A", "R1:1:A:L", "R1:0:D", "R3:0:A", "R4:13:A");

    for (String state : states) {
      cluster.getZkClient().create(root + "/" + state, null, CreateMode.PERSISTENT, true);
    }

    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    PerReplicaStates rs = zkStateReader.getReplicaStates(new PerReplicaStates(root, 0, Collections.emptyList()));
    assertEquals(3, rs.states.size());
    assertTrue(rs.cversion >= 5);

    List<PerReplicaStates.Op> ops = PerReplicaStates.WriteOps.addReplica("R5",State.ACTIVE, false, rs).get();

    assertEquals(1, ops.size());
    assertEquals(PerReplicaStates.Op.Type.ADD ,ops.get(0).typ );
    PerReplicaStates.persist(ops, root,cluster.getZkClient());
    rs = zkStateReader.getReplicaStates(root);
    assertEquals(4, rs.states.size());
    assertTrue(rs.cversion >= 6);
    assertEquals(6,  cluster.getZkClient().getChildren(root, null,true).size());
    ops =  PerReplicaStates.WriteOps.flipState("R1", State.DOWN , rs).get();

    assertEquals(4, ops.size());
    assertEquals(PerReplicaStates.Op.Type.ADD,  ops.get(0).typ);
    assertEquals(PerReplicaStates.Op.Type.DELETE,  ops.get(1).typ);
    assertEquals(PerReplicaStates.Op.Type.DELETE,  ops.get(2).typ);
    assertEquals(PerReplicaStates.Op.Type.DELETE,  ops.get(3).typ);
    PerReplicaStates.persist(ops, root,cluster.getZkClient());
    rs = zkStateReader.getReplicaStates(root);
    assertEquals(4, rs.states.size());
    assertEquals(3, rs.states.get("R1").version);

    ops =  PerReplicaStates.WriteOps.deleteReplica("R5" , rs).get();
    assertEquals(1, ops.size());
    PerReplicaStates.persist(ops, root,cluster.getZkClient());

    rs = zkStateReader.getReplicaStates(root);
    assertEquals(3, rs.states.size());

    ops = PerReplicaStates.WriteOps.flipLeader(ImmutableSet.of("R4","R3","R1"), "R4",rs).get();

    assertEquals(2, ops.size());
    assertEquals(PerReplicaStates.Op.Type.ADD, ops.get(0).typ);
    assertEquals(PerReplicaStates.Op.Type.DELETE, ops.get(1).typ);
    PerReplicaStates.persist(ops, root,cluster.getZkClient());
    rs = zkStateReader.getReplicaStates(root);
    ops =  PerReplicaStates.WriteOps.flipLeader(ImmutableSet.of("R4","R3","R1"),"R3",rs).get();
    assertEquals(4, ops.size());
    PerReplicaStates.persist(ops, root,cluster.getZkClient());
    rs = zkStateReader.getReplicaStates(root);
    assertTrue(rs.get("R3").isLeader);
  }

}
