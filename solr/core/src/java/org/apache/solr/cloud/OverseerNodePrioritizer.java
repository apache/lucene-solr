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

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

import org.apache.http.client.HttpClient;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;

/**
 * Responsible for prioritization of Overseer nodes, for example with the
 * ADDROLE collection command.
 */
public class OverseerNodePrioritizer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ZkStateReader zkStateReader;
  private final String adminPath;
  private final ShardHandlerFactory shardHandlerFactory;

  private ZkDistributedQueue stateUpdateQueue;

  private HttpClient httpClient;

  public OverseerNodePrioritizer(ZkStateReader zkStateReader, ZkDistributedQueue stateUpdateQueue, String adminPath, ShardHandlerFactory shardHandlerFactory, HttpClient httpClient) {
    this.zkStateReader = zkStateReader;
    this.adminPath = adminPath;
    this.shardHandlerFactory = shardHandlerFactory;
    this.stateUpdateQueue = stateUpdateQueue;
    this.httpClient = httpClient;
  }

  public synchronized void prioritizeOverseerNodes(String overseerId) throws Exception {
    SolrZkClient zk = zkStateReader.getZkClient();
    if(!zk.exists(ZkStateReader.ROLES,true))return;
    @SuppressWarnings({"rawtypes"})
    Map m = (Map) Utils.fromJSON(zk.getData(ZkStateReader.ROLES, null, new Stat(), true));

    @SuppressWarnings({"rawtypes"})
    List overseerDesignates = (List) m.get("overseer");
    if(overseerDesignates==null || overseerDesignates.isEmpty()) return;
    String ldr = OverseerTaskProcessor.getLeaderNode(zk);
    if(overseerDesignates.contains(ldr)) return;
    log.info("prioritizing overseer nodes at {} overseer designates are {}", overseerId, overseerDesignates);
    List<String> electionNodes = OverseerTaskProcessor.getSortedElectionNodes(zk, Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE);
    if(electionNodes.size()<2) return;
    log.info("sorted nodes {}", electionNodes);

    String designateNodeId = null;
    for (String electionNode : electionNodes) {
      if(overseerDesignates.contains( LeaderElector.getNodeName(electionNode))){
        designateNodeId = electionNode;
        break;
      }
    }

    if(designateNodeId == null){
      log.warn("No live overseer designate ");
      return;
    }
    if(!designateNodeId.equals( electionNodes.get(1))) { //checking if it is already at no:1
      log.info("asking node {} to come join election at head", designateNodeId);
      invokeOverseerOp(designateNodeId, "rejoinAtHead"); //ask designate to come first
      if (log.isInfoEnabled()) {
        log.info("asking the old first in line {} to rejoin election  ", electionNodes.get(1));
      }
      invokeOverseerOp(electionNodes.get(1), "rejoin");//ask second inline to go behind
    }
    //now ask the current leader to QUIT , so that the designate can takeover
    stateUpdateQueue.offer(
        Utils.toJSON(new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.QUIT.toLower(),
            ID, OverseerTaskProcessor.getLeaderId(zkStateReader.getZkClient()))));

  }

  private void invokeOverseerOp(String electionNode, String op) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    ShardHandler shardHandler = ((HttpShardHandlerFactory)shardHandlerFactory).getShardHandler(httpClient);
    params.set(CoreAdminParams.ACTION, CoreAdminAction.OVERSEEROP.toString());
    params.set("op", op);
    params.set("qt", adminPath);
    params.set("electionNode", electionNode);
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = 1;
    String replica = zkStateReader.getBaseUrlForNodeName(LeaderElector.getNodeName(electionNode));
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = params;
    shardHandler.submit(sreq, replica, sreq.params);
    shardHandler.takeCompletedOrError();
  }
}
