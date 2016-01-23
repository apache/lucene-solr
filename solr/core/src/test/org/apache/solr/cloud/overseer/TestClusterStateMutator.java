package org.apache.solr.cloud.overseer;

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

import java.util.Collections;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.MockZkStateReader;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Utils;

public class TestClusterStateMutator extends SolrTestCaseJ4 {
  public void testCreateCollection() throws Exception {
    ClusterState state = new ClusterState(-1, Collections.<String>emptySet(), Collections.<String, DocCollection>emptyMap());
    MockZkStateReader zkStateReader = new MockZkStateReader(state, Collections.<String>emptySet());

    ClusterState clusterState = zkStateReader.getClusterState();
    ClusterStateMutator mutator = new ClusterStateMutator(zkStateReader);
    ZkNodeProps message = new ZkNodeProps(Utils.makeMap(
        "name", "xyz",
        "numShards", "1"
    ));
    ZkWriteCommand cmd = mutator.createCollection(clusterState, message);
    DocCollection collection = cmd.collection;
    assertEquals("xyz", collection.getName());
    assertEquals(1, collection.getSlicesMap().size());
    assertEquals(1, collection.getMaxShardsPerNode());

    state = new ClusterState(-1, Collections.<String>emptySet(), Collections.singletonMap("xyz", collection));
    message = new ZkNodeProps(Utils.makeMap(
        "name", "abc",
        "numShards", "2",
        "router.name", "implicit",
        "shards", "x,y",
        "replicationFactor", "3",
        "maxShardsPerNode", "4"
    ));
    cmd = mutator.createCollection(state, message);
    collection = cmd.collection;
    assertEquals("abc", collection.getName());
    assertEquals(2, collection.getSlicesMap().size());
    assertNotNull(collection.getSlicesMap().get("x"));
    assertNotNull(collection.getSlicesMap().get("y"));
    assertNull(collection.getSlicesMap().get("x").getRange());
    assertNull(collection.getSlicesMap().get("y").getRange());
    assertSame(Slice.State.ACTIVE, collection.getSlicesMap().get("x").getState());
    assertSame(Slice.State.ACTIVE, collection.getSlicesMap().get("y").getState());
    assertEquals(4, collection.getMaxShardsPerNode());
    assertEquals(ImplicitDocRouter.class, collection.getRouter().getClass());
    assertNotNull(state.getCollectionOrNull("xyz")); // we still have the old collection
  }
}

