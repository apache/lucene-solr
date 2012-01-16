package org.apache.solr.cloud;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.PeerSync;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

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

public abstract class ElectionContext {
  
  final String electionPath;
  final ZkNodeProps leaderProps;
  final String id;
  final String leaderPath;
  
  public ElectionContext(final String shardZkNodeName,
      final String electionPath, final String leaderPath, final ZkNodeProps leaderProps) {
    this.id = shardZkNodeName;
    this.electionPath = electionPath;
    this.leaderPath = leaderPath;
    this.leaderProps = leaderProps;
  }
  
  abstract void runLeaderProcess(boolean weAreReplacement) throws KeeperException, InterruptedException;
}

final class ShardLeaderElectionContext extends ElectionContext {
  
  private final SolrZkClient zkClient;
  private ZkStateReader zkStateReader;
  private String shardId;
  private String collection;
  private SolrCore core;

  public ShardLeaderElectionContext(SolrCore core, final String shardId,
      final String collection, final String shardZkNodeName, ZkNodeProps props, ZkStateReader zkStateReader) {
    super(shardZkNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/leader_elect/"
        + shardId, ZkStateReader.getShardLeadersPath(collection, shardId),
        props);
    this.core = core;
    this.zkClient = zkStateReader.getZkClient();
    this.zkStateReader = zkStateReader;
    this.shardId = shardId;
    this.collection = collection;
  }

  @Override
  void runLeaderProcess(boolean weAreReplacement) throws KeeperException, InterruptedException {
    System.out.println("run leader process");
    System.out.println(weAreReplacement + " " + core);
    // TODO: move sync stuff to a better spot
    if (weAreReplacement && core != null) { // TODO: core can be null in tests
      if (zkClient.exists(leaderPath, true)) {
        zkClient.delete(leaderPath, -1, true);
      }
      System.out.println("SYNC UP");
      syncReplicas();
    }
    try {
      zkClient.makePath(leaderPath,
          leaderProps == null ? null : ZkStateReader.toJSON(leaderProps),
          CreateMode.EPHEMERAL, true);
    } catch (NodeExistsException e) {
      // if a previous leader ephemeral still exists for some reason, try and remove it
      zkClient.delete(leaderPath, -1, true);
      zkClient.makePath(leaderPath,
          leaderProps == null ? null : ZkStateReader.toJSON(leaderProps),
          CreateMode.EPHEMERAL, true);
    }
  }

  private void syncReplicas() {
    try {
      // nocommit
//      System.out.println("I am the new Leader:" + leaderPath
//          + " - I need to request all of my replicas to go into sync mode");
      
      // first sync ourselves - we are the potential leader after all
      boolean success = sync(leaderProps);
      if (success) {
        System.out.println("Sync success");
        // we are the leader - tell all of our replias to sync with us
        
        // sync everyone else
        // TODO: we should do this in parallel at least
        List<ZkCoreNodeProps> nodes = zkStateReader.getReplicaProps(collection, shardId,
            leaderProps.get(ZkStateReader.NODE_NAME_PROP), leaderProps.get(ZkStateReader.CORE_PROP));
        if (nodes != null) {
          for (ZkCoreNodeProps node : nodes) {
            try {
              sync(leaderProps, node.getNodeProps());
            } catch(Exception exception) {
              exception.printStackTrace();
              //nocommit
            }
          }
        }
      } else {
        // nocommit: we cannot be the leader - go into recovery
        // but what if no one can be the leader in a loop?
        System.out.println("Sync failure");
      }
      

      
    } catch (Exception e) {
      // nocommit
      e.printStackTrace();
    }
  }

  private boolean sync(ZkNodeProps props) throws MalformedURLException, SolrServerException,
      IOException {
    List<ZkCoreNodeProps> nodes = zkStateReader.getReplicaProps(collection, shardId,
        props.get(ZkStateReader.NODE_NAME_PROP), props.get(ZkStateReader.CORE_PROP));
    
    if (nodes == null) {
      // I have no replicas
      return true;
    }
    
    List<String> syncWith = new ArrayList<String>();
    for (ZkCoreNodeProps node : nodes) {
      syncWith.add(node.getCoreUrl());
    }

    // TODO: do we first everyone register as sync phase? get the overseer to do it?
    PeerSync peerSync = new PeerSync(core, syncWith, 1000);
    return peerSync.sync();
  }
  
  private void sync(ZkNodeProps leader, ZkNodeProps props)
      throws MalformedURLException, SolrServerException, IOException {
    List<ZkCoreNodeProps> nodes = zkStateReader.getReplicaProps(collection,
        shardId, props.get(ZkStateReader.NODE_NAME_PROP),
        props.get(ZkStateReader.CORE_PROP));
    
    if (nodes == null) {
      // I have no replicas
      return;
    }
    ZkCoreNodeProps zkLeader = new ZkCoreNodeProps(leaderProps);
    for (ZkCoreNodeProps node : nodes) {
      try {
        // TODO: do we first everyone register as sync phase? get the overseer
        // to
        // do
        // it?
        QueryRequest qr = new QueryRequest(params("qt", "/get", "getVersions",
            Integer.toString(1000), "sync", StrUtils.join(
                Collections.singletonList(zkLeader.getCoreUrl()), ',')));
        CommonsHttpSolrServer server = null;
        
        server = new CommonsHttpSolrServer(node.getCoreUrl());
        
        NamedList rsp = server.request(qr);
      } catch (Exception e) {
        // nocommit
        e.printStackTrace();
      }
    }
  }
  
  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i=0; i<params.length; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }
}

final class OverseerElectionContext extends ElectionContext {
  
  private final SolrZkClient zkClient;
  private final ZkStateReader stateReader;

  public OverseerElectionContext(final String zkNodeName, SolrZkClient zkClient, ZkStateReader stateReader) {
    super(zkNodeName, "/overseer_elect", null, null);
    this.zkClient = zkClient;
    this.stateReader = stateReader;
  }

  @Override
  void runLeaderProcess(boolean weAreReplacement) throws KeeperException, InterruptedException {
    new Overseer(zkClient, stateReader);
  }
  
}
