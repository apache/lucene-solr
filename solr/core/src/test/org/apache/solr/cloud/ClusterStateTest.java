package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expectLastCall;

public class ClusterStateTest extends SolrTestCaseJ4 {
  @Test
  public void testStoreAndRead() throws Exception {
    Map<String,DocCollection> collectionStates = new HashMap<String,DocCollection>();
    Set<String> liveNodes = new HashSet<String>();
    liveNodes.add("node1");
    liveNodes.add("node2");
    
    Map<String,Slice> slices = new HashMap<String,Slice>();
    Map<String,Replica> sliceToProps = new HashMap<String,Replica>();
    Map<String,Object> props = new HashMap<String,Object>();

    props.put("prop1", "value");
    props.put("prop2", "value2");
    Replica replica = new Replica("node1", props);
    sliceToProps.put("node1", replica);
    Slice slice = new Slice("shard1", sliceToProps, null);
    slices.put("shard1", slice);
    Slice slice2 = new Slice("shard2", sliceToProps, null);
    slices.put("shard2", slice2);
    collectionStates.put("collection1", new DocCollection("collection1", slices, null, DocRouter.DEFAULT));
    collectionStates.put("collection2", new DocCollection("collection2", slices, null, DocRouter.DEFAULT));
    ZkStateReader zkStateReaderMock = getMockZkStateReader(collectionStates.keySet());
    
    ClusterState clusterState = new ClusterState(-1,liveNodes, collectionStates,zkStateReaderMock);
    byte[] bytes = ZkStateReader.toJSON(clusterState);
    // System.out.println("#################### " + new String(bytes));
    ClusterState loadedClusterState = ClusterState.load(-1, bytes, liveNodes,zkStateReaderMock);
    
    assertEquals("Provided liveNodes not used properly", 2, loadedClusterState
        .getLiveNodes().size());
    assertEquals("No collections found", 2, loadedClusterState.getCollections().size());
    assertEquals("Poperties not copied properly", replica.getStr("prop1"), loadedClusterState.getSlice("collection1", "shard1").getReplicasMap().get("node1").getStr("prop1"));
    assertEquals("Poperties not copied properly", replica.getStr("prop2"), loadedClusterState.getSlice("collection1", "shard1").getReplicasMap().get("node1").getStr("prop2"));

    loadedClusterState = ClusterState.load(-1, new byte[0], liveNodes, getMockZkStateReader(Collections.<String>emptySet()));
    
    assertEquals("Provided liveNodes not used properly", 2, loadedClusterState
        .getLiveNodes().size());
    assertEquals("Should not have collections", 0, loadedClusterState.getCollections().size());

    loadedClusterState = ClusterState.load(-1, (byte[])null, liveNodes,getMockZkStateReader(Collections.<String>emptySet()));
    
    assertEquals("Provided liveNodes not used properly", 2, loadedClusterState
        .getLiveNodes().size());
    assertEquals("Should not have collections", 0, loadedClusterState.getCollections().size());
  }

  public static ZkStateReader getMockZkStateReader(final Set<String> collections) {
    ZkStateReader mock = createMock(ZkStateReader.class);
    EasyMock.reset(mock);
    mock.getAllCollections();
    EasyMock.expectLastCall().andAnswer(new IAnswer<Set<String>>() {
      @Override
      public Set<String> answer() throws Throwable {
        return collections;
      }
    }).anyTimes();
    EasyMock.replay(mock);

    return mock;
  }
}
