package org.apache.solr.cloud;

/**
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.junit.Test;

// TODO: assert something
public class CloudStateTest extends SolrTestCaseJ4 {
  @Test
  public void testStoreAndRead() throws Exception {
    Map<String,Map<String,Slice>> collectionStates = new HashMap<String,Map<String,Slice>>();
    Set<String> liveNodes = new HashSet<String>();
    
    Map<String,Slice> slices = new HashMap<String,Slice>();
    Map<String,ZkNodeProps> sliceToProps = new HashMap<String,ZkNodeProps>();
    Map<String,String> props = new HashMap<String,String>();

    props.put("prop1", "value");
    ZkNodeProps zkNodeProps = new ZkNodeProps(props);
    sliceToProps.put("node1", zkNodeProps);
    Slice slice = new Slice("shard1", sliceToProps);
    slices.put("shard1", slice);
    collectionStates.put("collection1", slices);
    
    CloudState cloudState = new CloudState(liveNodes, collectionStates);
    byte[] bytes = CloudState.store(cloudState);
    
    CloudState loadedCloudState = CloudState.load(bytes, liveNodes);
    
    System.out.println("cloud state:" + loadedCloudState);
  }
}
