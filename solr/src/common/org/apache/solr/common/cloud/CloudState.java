package org.apache.solr.common.cloud;

/**
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// immutable
public class CloudState {
  protected static Logger log = LoggerFactory.getLogger(CloudState.class);
  
  private final Map<String,Map<String,Slice>> collectionStates;
  private final Set<String> liveNodes;
  
  public CloudState(Set<String> liveNodes, Map<String,Map<String,Slice>> collectionStates) {
    this.liveNodes = liveNodes;
    this.collectionStates = collectionStates;
  }
  
  public Map<String,Slice> getSlices(String collection) {
    Map<String,Slice> collectionState = collectionStates.get(collection);
    if(collectionState == null) {
      return null;
    }
    return Collections.unmodifiableMap(collectionState);
  }
  
  public Set<String> getCollections() {
    return Collections.unmodifiableSet(collectionStates.keySet());
  }
  
  public Map<String,Map<String,Slice>> getCollectionStates() {
    return Collections.unmodifiableMap(collectionStates);
  }
  
  public Set<String> getLiveNodes() {
    return Collections.unmodifiableSet(liveNodes);
  }
  
  public boolean liveNodesContain(String name) {
    return liveNodes.contains(name);
  }
  
  public static CloudState buildCloudState(SolrZkClient zkClient, CloudState oldCloudState, boolean onlyLiveNodes) throws KeeperException, InterruptedException, IOException {
    Map<String,Map<String,Slice>> collectionStates;
    if (!onlyLiveNodes) {
      List<String> collections = zkClient.getChildren(
          ZkStateReader.COLLECTIONS_ZKNODE, null);

      collectionStates = new HashMap<String,Map<String,Slice>>();
      for (String collection : collections) {
        String shardIdPaths = ZkStateReader.COLLECTIONS_ZKNODE + "/"
            + collection + ZkStateReader.SHARDS_ZKNODE;
        List<String> shardIdNames;
        try {
          shardIdNames = zkClient.getChildren(shardIdPaths, null);
        } catch (KeeperException.NoNodeException e) {
          // node is not valid currently
          continue;
        }
        Map<String,Slice> slices = new HashMap<String,Slice>();
        for (String shardIdZkPath : shardIdNames) {
          Map<String,ZkNodeProps> shardsMap = readShards(zkClient, shardIdPaths
              + "/" + shardIdZkPath);
          Slice slice = new Slice(shardIdZkPath, shardsMap);
          slices.put(shardIdZkPath, slice);
        }
        collectionStates.put(collection, slices);
      }
    } else {
      collectionStates = oldCloudState.getCollectionStates();
    }
    
    CloudState cloudInfo = new CloudState(getLiveNodes(zkClient), collectionStates);
    
    return cloudInfo;
  }
  
  /**
   * @param zkClient
   * @param shardsZkPath
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException
   */
  private static Map<String,ZkNodeProps> readShards(SolrZkClient zkClient, String shardsZkPath)
      throws KeeperException, InterruptedException, IOException {

    Map<String,ZkNodeProps> shardNameToProps = new HashMap<String,ZkNodeProps>();

    if (zkClient.exists(shardsZkPath, null) == null) {
      throw new IllegalStateException("Cannot find zk shards node that should exist:"
          + shardsZkPath);
    }

    List<String> shardZkPaths = zkClient.getChildren(shardsZkPath, null);
    
    for(String shardPath : shardZkPaths) {
      byte[] data = zkClient.getData(shardsZkPath + "/" + shardPath, null,
          null);
      
      ZkNodeProps props = new ZkNodeProps();
      props.load(data);
      shardNameToProps.put(shardPath, props);
    }

    return Collections.unmodifiableMap(shardNameToProps);
  }
  
  private static Set<String> getLiveNodes(SolrZkClient zkClient) throws KeeperException, InterruptedException {
    List<String> liveNodes = zkClient.getChildren(ZkStateReader.LIVE_NODES_ZKNODE, null);
    Set<String> liveNodesSet = new HashSet<String>(liveNodes.size());
    liveNodesSet.addAll(liveNodes);

    return liveNodesSet;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("live nodes:" + liveNodes);
    sb.append(" collections:" + collectionStates);
    return sb.toString();
  }

}
