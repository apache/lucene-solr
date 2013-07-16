package org.apache.solr.cloud;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignTest extends SolrTestCaseJ4 {
  protected static Logger log = LoggerFactory.getLogger(AssignTest.class);

  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @Test
  public void testAssignNode() throws Exception {
    String cname = "collection1";
    
    Map<String,DocCollection> collectionStates = new HashMap<String,DocCollection>();
    
    Map<String,Slice> slices = new HashMap<String,Slice>();
    
    Map<String,Replica> replicas = new HashMap<String,Replica>();
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, "state", 
        ZkStateReader.STATE_PROP, "ACTIVE", 
        ZkStateReader.BASE_URL_PROP, "0.0.0.0", 
        ZkStateReader.CORE_NAME_PROP, "core1",
        ZkStateReader.ROLES_PROP, null,
        ZkStateReader.NODE_NAME_PROP, "0_0_0_0",
        ZkStateReader.SHARD_ID_PROP, "shard1",
        ZkStateReader.COLLECTION_PROP, cname,
        ZkStateReader.NUM_SHARDS_PROP, "1",
        ZkStateReader.CORE_NODE_NAME_PROP, "core_node1");
    Replica replica = new Replica("core_node1" , m.getProperties());
    replicas.put("core_node1", replica);
    
    Slice slice = new Slice("slice1", replicas , new HashMap<String,Object>(0));
    slices.put("slice1", slice);
    
    DocRouter router = new ImplicitDocRouter();
    DocCollection docCollection = new DocCollection(cname, slices, new HashMap<String,Object>(0), router);

    collectionStates.put(cname, docCollection);
    
    Set<String> liveNodes = new HashSet<String>();
    ClusterState state = new ClusterState(liveNodes, collectionStates);
    String nodeName = Assign.assignNode("collection1", state);
    
    assertEquals("core_node2", nodeName);
  }
  
}
