package org.apache.solr.cloud;

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
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestRecovery;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.PeerSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncStrategy {
  protected final Logger log = LoggerFactory.getLogger(getClass());
  
  public boolean sync(ZkController zkController, SolrCore core,
      ZkNodeProps leaderProps) {
    zkController.publish(core, ZkStateReader.SYNC);
    
    // nocommit
    System.out.println("SYNC UP");
    boolean success = syncReplicas(zkController, core, leaderProps);
    return success;
  }
  
  private boolean syncReplicas(ZkController zkController, SolrCore core,
      ZkNodeProps leaderProps) {
    boolean success = false;
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    String collection = cloudDesc.getCollectionName();
    String shardId = cloudDesc.getShardId();
    try {
      // nocommit
      
      // first sync ourselves - we are the potential leader after all
      try {
        success = syncWithReplicas(zkController, core, leaderProps, collection, shardId);
      } catch (Exception e) {
        e.printStackTrace();
      }
      
      // if !success but no one else is in active mode,
      // we are the leader anyway
      // nocommit: should we also be leader if there is only one other active?
      // if we couldnt sync with it, it shouldnt be able to sync with us
      
      if (!success
          && !areAnyOtherReplicasActive(zkController, leaderProps, collection,
              shardId)) {
        System.out
            .println("wasnt a success but no on else i active! I am the leader");
        
        success = true;
      }
      
      if (success) {
        // nocommit
        System.out.println("Sync success");
        // we are the leader - tell all of our replias to sync with us
        
        // sync everyone else
        // TODO: we should do this in parallel at least
        List<ZkCoreNodeProps> nodes = zkController.getZkStateReader()
            .getReplicaProps(collection, shardId,
                leaderProps.get(ZkStateReader.NODE_NAME_PROP),
                leaderProps.get(ZkStateReader.CORE_NAME_PROP),
                ZkStateReader.ACTIVE);
        if (nodes != null) {
          for (ZkCoreNodeProps node : nodes) {
            try {
              syncToMe(zkController, collection, shardId, leaderProps,
                  node.getNodeProps());
            } catch (Exception exception) {
              exception.printStackTrace();
              // nocommit
            }
          }
        }
      } else {
        // nocommit: we cannot be the leader - go into recovery
        // but what if no one can be the leader in a loop?
        // perhaps we look down the list and if no one is active, we
        // accept leader role anyhow
        
        // nocommit
        System.out.println("Sync failure");
      }
      
    } catch (Exception e) {
      // nocommit
      e.printStackTrace();
    }
    
    return success;
  }
  
  private boolean areAnyOtherReplicasActive(ZkController zkController,
      ZkNodeProps leaderProps, String collection, String shardId) {
    CloudState cloudState = zkController.getZkStateReader().getCloudState();
    Map<String,Slice> slices = cloudState.getSlices(collection);
    Slice slice = slices.get(shardId);
    Map<String,ZkNodeProps> shards = slice.getShards();
    for (Map.Entry<String,ZkNodeProps> shard : shards.entrySet()) {
      String state = shard.getValue().get(ZkStateReader.STATE_PROP);
      System.out.println("state:"
          + state
          + shard.getValue().get(ZkStateReader.NODE_NAME_PROP)
          + " live: "
          + cloudState.liveNodesContain(shard.getValue().get(
              ZkStateReader.NODE_NAME_PROP)));
      if ((state.equals(ZkStateReader.ACTIVE))
          && cloudState.liveNodesContain(shard.getValue().get(
              ZkStateReader.NODE_NAME_PROP))
          && !new ZkCoreNodeProps(shard.getValue()).getCoreUrl().equals(
              new ZkCoreNodeProps(leaderProps).getCoreUrl())) {
        System.out.println(" FOUND ACTIVE");
        return true;
      }
    }
    
    System.out.println(" DIDNT FOUND ACTIVE");
    return false;
  }
  
  private boolean syncWithReplicas(ZkController zkController, SolrCore core,
      ZkNodeProps props, String collection, String shardId)
      throws MalformedURLException, SolrServerException, IOException {
    List<ZkCoreNodeProps> nodes = zkController.getZkStateReader()
        .getReplicaProps(collection, shardId,
            props.get(ZkStateReader.NODE_NAME_PROP),
            props.get(ZkStateReader.CORE_NAME_PROP), ZkStateReader.ACTIVE); // TODO:
    // should
    // there
    // be a
    // state
    // filter?
    
    if (nodes == null) {
      // I have no replicas
      return true;
    }
    
    List<String> syncWith = new ArrayList<String>();
    for (ZkCoreNodeProps node : nodes) {
      // if we see a leader, must be stale state, and this is the guy that went down
      if (!node.getNodeProps().keySet().contains(ZkStateReader.LEADER_PROP)) {
        syncWith.add(node.getCoreUrl());
      }
    }
    
 
    PeerSync peerSync = new PeerSync(core, syncWith, 1000);
    return peerSync.sync();
  }
  
  private void syncToMe(ZkController zkController, String collection,
      String shardId, ZkNodeProps leaderProps, ZkNodeProps props)
      throws MalformedURLException, SolrServerException, IOException {
    List<ZkCoreNodeProps> nodes = zkController.getZkStateReader()
        .getReplicaProps(collection, shardId,
            props.get(ZkStateReader.NODE_NAME_PROP),
            props.get(ZkStateReader.CORE_NAME_PROP));
    
    if (nodes == null) {
      System.out.println("I have no replicas");
      // I have no replicas
      return;
    }
    System.out.println("tell my replicas to sync");
    ZkCoreNodeProps zkLeader = new ZkCoreNodeProps(leaderProps);
    for (ZkCoreNodeProps node : nodes) {
      try {
        // TODO: do we first everyone register as sync phase? get the overseer
        // to do it?
        // TODO: this should be done in parallel
        QueryRequest qr = new QueryRequest(params("qt", "/get", "getVersions",
            Integer.toString(1000), "sync", zkLeader.getCoreUrl(), "distrib", "false"));
        CommonsHttpSolrServer server = new CommonsHttpSolrServer(node.getCoreUrl());
        
        NamedList rsp = server.request(qr);
        System.out.println("response about syncing to leader:" + rsp);
        boolean success = (Boolean) rsp.get("sync");
        System.out.println("success:" + success);
        if (!success) {
          System.out.println("try and ask " + node.getCoreUrl() + " to recover");
          log.info("try and ask " + node.getCoreUrl() + " to recover");
          try {
            server = new CommonsHttpSolrServer(node.getBaseUrl());
            server.setSoTimeout(5000);
            server.setConnectionTimeout(5000);
            
            RequestRecovery recoverRequestCmd = new RequestRecovery();
            recoverRequestCmd.setAction(CoreAdminAction.REQUESTRECOVERY);
            recoverRequestCmd.setCoreName(node.getCoreName());
            
            server.request(recoverRequestCmd);
          } catch (Exception e) {
            log.info("Could not tell a replica to recover", e);
          }
        }
      } catch (Exception e) {
        // nocommit
        e.printStackTrace();
      }
    }
  }
  
  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }
}
