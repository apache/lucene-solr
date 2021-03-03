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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

public class ClusterStateTest extends SolrTestCaseJ4 {

  @Test
  public void testStoreAndRead() throws Exception {
    Map<String,DocCollection> collectionStates = new HashMap<>();
    Set<String> liveNodes = new HashSet<>();
    liveNodes.add("node1");
    liveNodes.add("node2");
    
    Map<String,Slice> slices = new HashMap<>();
    Map<String,Replica> sliceToProps = new HashMap<>();
    Map<String,Object> props = new HashMap<>();
    props.put("node_name", "node1:10000_solr");
    props.put("core", "core1");
    props.put("id", "1");
    props.put("prop1", "value");
    props.put("prop2", "value2");
    Replica replica = new Replica("node1", props, "collection1", -1l,"shard1", nodeName -> "http://" + nodeName);
    sliceToProps.put("node1", replica);
    Slice slice = new Slice("shard1", sliceToProps, null, "collection1",-1l, nodeName -> "http://" + nodeName);
    slices.put("shard1", slice);
    Slice slice2 = new Slice("shard2", sliceToProps, null, "collection1",-1l, nodeName -> "http://" + nodeName);
    slices.put("shard2", slice2);

    Map<String, Object> cprops = new HashMap<>();
    cprops.put("id", -1l);
    collectionStates.put("collection1", new DocCollection("collection1", slices, cprops, DocRouter.DEFAULT));
    collectionStates.put("collection2", new DocCollection("collection2", slices, cprops, DocRouter.DEFAULT));

    ClusterState clusterState = ClusterState.getRefCS(collectionStates, -1);
    byte[] bytes = Utils.toJSON(clusterState);
    // System.out.println("#################### " + new String(bytes));
    ClusterState loadedClusterState = ClusterState.createFromJson(nodeName -> "http://" + nodeName ,-1, bytes);

    assertEquals("No collections found", 2, loadedClusterState.getCollectionsMap().size());
    assertEquals("Properties not copied properly", replica.getStr("prop1"), loadedClusterState.getCollection("collection1").getSlice("shard1").getReplicasMap().get("node1").getStr("prop1"));
    assertEquals("Properties not copied properly", replica.getStr("prop2"), loadedClusterState.getCollection("collection1").getSlice("shard1").getReplicasMap().get("node1").getStr("prop2"));

    loadedClusterState = ClusterState.createFromJson(nodeName -> "http://" + nodeName, -1, new byte[0]);

    assertEquals("Should not have collections", 0, loadedClusterState.getCollectionsMap().size());

    loadedClusterState = ClusterState.createFromJson(nodeName -> "http://" + nodeName, -1, (byte[])null);

    assertEquals("Should not have collections", 0, loadedClusterState.getCollectionsMap().size());
  }

}
