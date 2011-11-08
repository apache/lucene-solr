package org.apache.solr.update.processor;

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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;

public class DistributedUpdateProcessorFactory extends
    UpdateRequestProcessorFactory {
  public static final String SEEN_LEADER = "leader";
  NamedList args;
  List<String> shards;
  String selfStr;
  String shardsString;
  
  @Override
  public void init(NamedList args) {
    selfStr = (String) args.get("self");
    Object o = args.get("shards");
    if (o != null && o instanceof List) {
      shards = (List<String>) o;
      shardsString = StrUtils.join((List<String>) o, ',');
    } else if (o != null && o instanceof String) {
      shards = StrUtils.splitSmart((String) o, ",", true);
      shardsString = (String) o;
    }
  }
  
  /** return the list of shards, or null if not configured */
  public List<String> getShards() {
    return shards;
  }
  
  public String getShardsString() {
    return shardsString;
  }
  
  /** return "self", or null if not configured */
  public String getSelf() {
    return selfStr;
  }
  
  @Override
  public DistributedUpdateProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
    boolean isLeader = false;
    boolean forwardToLeader = false;
    // if we are in zk mode...
    if (coreDesc.getCoreContainer().getZkController() != null) {
      // the leader is...
      // TODO: if there is no leader, wait and look again
      // TODO: we are reading the leader from zk every time - we should cache
      // this
      // and watch for changes
      List<String> leaderChildren;
      String collection = coreDesc.getCloudDescriptor().getCollectionName();
      String shardId = coreDesc.getCloudDescriptor().getShardId();
      
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      
      String leaderNode = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
          + ZkStateReader.LEADER_ELECT_ZKNODE + "/" + shardId + "/leader";
      SolrZkClient zkClient = coreDesc.getCoreContainer().getZkController()
          .getZkClient();

      try {
        leaderChildren = zkClient.getChildren(leaderNode, null);
        if (leaderChildren.size() > 0) {
          String leader = leaderChildren.get(0);
          
          ZkNodeProps zkNodeProps = new ZkNodeProps();
          byte[] bytes = zkClient
              .getData(leaderNode + "/" + leader, null, null);
          zkNodeProps.load(bytes);
          
          String leaderUrl = zkNodeProps.get("url");
          
          String nodeName = req.getCore().getCoreDescriptor()
              .getCoreContainer().getZkController().getNodeName();
          String shardZkNodeName = nodeName + "_" + req.getCore().getName();

          System.out.println("params:" + params);
          if (params.getBool(SEEN_LEADER, false)) {
            // we got a version, just go local
            
            // still mark if i am the leader though
            if (shardZkNodeName.equals(leader)) {
              isLeader = true;
            }
            System.out.println(" go local");
          } else if (shardZkNodeName.equals(leader)) {
            isLeader = true;
            // that means I want to forward onto my replicas...
            // so get the replicas...
            addReplicasAndSelf(req, collection, shardId, params,
                shardZkNodeName);
            
            // mark that this req has been to the leader
            params.set(SEEN_LEADER, true);
            System.out.println("mark leader seen");
          } else {
            // I need to forward onto the leader...
            params.add("shards", leaderUrl);
            forwardToLeader  = true;
          }
          System.out.println("set params on req:" + params);
          req.setParams(params);
        }
      } catch (KeeperException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      } catch (IOException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      }
    }
    
    String shardStr = req.getParams().get("shards");
    if (shards == null && shardStr == null) return null;
    return new DistributedUpdateProcessor(shardStr, req, rsp, this, isLeader, forwardToLeader, next);
  }

  private void addReplicasAndSelf(SolrQueryRequest req, String collection,
      String shardId, ModifiableSolrParams params, String shardZkNodeName) {
    CloudState cloudState = req.getCore().getCoreDescriptor()
        .getCoreContainer().getZkController().getCloudState();
    Slice replicas = cloudState.getSlices(collection).get(shardId);
    Map<String,ZkNodeProps> shardMap = replicas.getShards();
    String self = null;
    StringBuilder replicasUrl = new StringBuilder();
    for (Entry<String,ZkNodeProps> entry : shardMap.entrySet()) {
      if (replicasUrl.length() > 0) {
        replicasUrl.append("|");
      }
      String replicaUrl = entry.getValue().get("url");
      if (shardZkNodeName.equals(entry.getKey())) {
        self = replicaUrl;
      }
      replicasUrl.append(replicaUrl);
    }

    params.add("self", self);
    params.add("shards", replicasUrl.toString());
  }
  
  private void versionDoc(ModifiableSolrParams params) {
    params.set(SEEN_LEADER, true);
  }
}
