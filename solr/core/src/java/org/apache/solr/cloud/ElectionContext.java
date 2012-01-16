package org.apache.solr.cloud;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
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

  public ShardLeaderElectionContext(final String shardId,
      final String collection, final String shardZkNodeName, ZkNodeProps props, ZkStateReader zkStateReader) {
    super(shardZkNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/leader_elect/"
        + shardId, ZkStateReader.getShardLeadersPath(collection, shardId),
        props);
    this.zkClient = zkStateReader.getZkClient();
    this.zkStateReader = zkStateReader;
    this.shardId = shardId;
    this.collection = collection;
  }

  @Override
  void runLeaderProcess(boolean weAreReplacement) throws KeeperException, InterruptedException {
    if (weAreReplacement) {
      if (zkClient.exists(leaderPath, true)) {
        zkClient.delete(leaderPath, -1, true);
      }
      syncReplicas();
    }
    try {
      zkClient.makePath(leaderPath,
          leaderProps == null ? null : ZkStateReader.toJSON(leaderProps),
          CreateMode.EPHEMERAL, true);
    } catch (NodeExistsException e) {
      // if a previous leader ephemeral exists for some reason, try and remove it
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
      sync(leaderProps);
      
      // sync everyone else
      // TODO: we should do this in parallel
      List<ZkCoreNodeProps> nodes = zkStateReader.getReplicaProps(collection, shardId,
          leaderProps.get(ZkStateReader.NODE_NAME_PROP), leaderProps.get(ZkStateReader.CORE_PROP));
      if (nodes != null) {
        for (ZkCoreNodeProps node : nodes) {
          try {
            sync(node.getNodeProps());
          } catch(Exception exception) {
            exception.printStackTrace();
            //nocommit
          }
        }
      }
      
    } catch (Exception e) {
      // nocommit
      e.printStackTrace();
    }
  }

  private void sync(ZkNodeProps props) throws MalformedURLException, SolrServerException,
      IOException {
    List<ZkCoreNodeProps> nodes = zkStateReader.getReplicaProps(collection, shardId,
        props.get(ZkStateReader.NODE_NAME_PROP), props.get(ZkStateReader.CORE_PROP));
    
    if (nodes == null) {
      // I have no replicas
      return;
    }
    
    List<String> syncWith = new ArrayList<String>();
    for (ZkCoreNodeProps node : nodes) {
      syncWith.add(node.getCoreUrl());
    }

    // TODO: do we first everyone register as sync phase? get the overseer to do it?
    QueryRequest qr = new QueryRequest(params("qt", "/get", "getVersions",
        Integer.toString(1000), "sync",
        StrUtils.join(Arrays.asList(syncWith), ',')));
    CommonsHttpSolrServer server = null;
    
    server = new CommonsHttpSolrServer(ZkCoreNodeProps.getCoreUrl(
        props.get(ZkStateReader.BASE_URL_PROP),
        props.get(ZkStateReader.CORE_PROP)));
    
    NamedList rsp = server.request(qr);
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
