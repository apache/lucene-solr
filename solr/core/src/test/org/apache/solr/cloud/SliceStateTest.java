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
package org.apache.solr.cloud;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SliceStateTest extends SolrTestCaseJ4 {
  
  @Test
  public void testDefaultSliceState() {
    Map<String, DocCollection> collectionStates = new HashMap<>();
    Set<String> liveNodes = new HashSet<>();
    liveNodes.add("node1");

    Map<String, Slice> slices = new HashMap<>();
    Map<String, Replica> sliceToProps = new HashMap<>();
    Map<String, Object> props = new HashMap<>();

    Replica replica = new Replica("node1", props);
    sliceToProps.put("node1", replica);
    Slice slice = new Slice("shard1", sliceToProps, null);
    assertSame("Default state not set to active", Slice.State.ACTIVE, slice.getState());
    slices.put("shard1", slice);
    collectionStates.put("collection1", new DocCollection("collection1", slices, null, DocRouter.DEFAULT));

    ClusterState clusterState = new ClusterState(-1,liveNodes, collectionStates);
    byte[] bytes = Utils.toJSON(clusterState);
    ClusterState loadedClusterState = ClusterState.load(-1, bytes, liveNodes);

    assertSame("Default state not set to active", Slice.State.ACTIVE, loadedClusterState.getCollection("collection1").getSlice("shard1").getState());
  }
}
