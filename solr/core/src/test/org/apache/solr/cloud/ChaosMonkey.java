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

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.FullDistributedZkTest.CloudJettyRunner;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.servlet.SolrDispatchFilter;

public class ChaosMonkey {

  private Map<String,List<CloudJettyRunner>> shardToJetty;
  private ZkTestServer zkServer;
  private ZkStateReader zkStateReader;
  private String collection;
  private Random random;

  public ChaosMonkey(ZkTestServer zkServer, ZkStateReader zkStateReader,
      String collection, Map<String,List<CloudJettyRunner>> shardToJetty,
      Random random) {
    this.shardToJetty = shardToJetty;
    this.zkServer = zkServer;
    this.zkStateReader = zkStateReader;
    this.collection = collection;
    this.random = random;
  }
  
  public void expireSession(CloudJettyRunner cloudJetty) {
    SolrDispatchFilter solrDispatchFilter = (SolrDispatchFilter) cloudJetty.jetty.getDispatchFilter().getFilter();
    long sessionId = solrDispatchFilter.getCores().getZkController().getZkClient().getSolrZooKeeper().getSessionId();
    zkServer.expire(sessionId);
  }
  
  public JettySolrRunner stopShard(String slice, int index) throws Exception {
    JettySolrRunner jetty = shardToJetty.get(slice).get(index).jetty;
    stopJetty(jetty);
    return jetty;
  }

  private void stopJetty(JettySolrRunner jetty) throws Exception {
    // get a clean shutdown so that no dirs are left open...
    ((SolrDispatchFilter)jetty.getDispatchFilter().getFilter()).destroy();
    jetty.stop();
  }
  
  public void stopShard(String slice) throws Exception {
    List<CloudJettyRunner> jetties = shardToJetty.get(slice);
    for (CloudJettyRunner jetty : jetties) {
      stopJetty(jetty.jetty);
    }
  }
  
  public void stopShardExcept(String slice, String shardName) throws Exception {
    List<CloudJettyRunner> jetties = shardToJetty.get(slice);
    for (CloudJettyRunner jetty : jetties) {
      if (!jetty.shardName.equals(shardName)) {
        stopJetty(jetty.jetty);
      }
    }
  }
  
  public JettySolrRunner getShard(String slice, int index) throws Exception {
    JettySolrRunner jetty = shardToJetty.get(slice).get(index).jetty;
    return jetty;
  }
  
  public JettySolrRunner stopRandomShard() throws Exception {
    // add all the shards to a list
//    CloudState clusterState = zk.getCloudState();
//    for (String collection : collections)   {
//    Slice theShards = zk.getCloudState().getSlices(collection);
    return null;
  }
  
  public JettySolrRunner stopRandomShard(String slice) throws Exception {
    // get latest cloud state
    zkStateReader.updateCloudState(true);
    Slice theShards = zkStateReader.getCloudState().getSlices(collection)
        .get(slice);
    int numRunning = 0;
    
    for (CloudJettyRunner cloudJetty : shardToJetty.get(slice)) {
      boolean running = true;
      
      ZkNodeProps props = theShards.getShards().get(cloudJetty.shardName);
      String state = props.get(ZkStateReader.STATE_PROP);
      String nodeName = props.get(ZkStateReader.NODE_NAME_PROP);
      
      if (!cloudJetty.jetty.isRunning()
          || state.equals(ZkStateReader.RECOVERING)
          || !zkStateReader.getCloudState().liveNodesContain(nodeName)) {
        running = false;
      }
      
      if (running) {
        numRunning++;
      }
    }
    
    if (numRunning < 2) {
      // we cannot kill anyone
      return null;
    }
    
    // kill random shard in shard2
    List<CloudJettyRunner> jetties = shardToJetty.get(slice);
    int index = random.nextInt(jetties.size() - 1);
    JettySolrRunner jetty = jetties.get(index).jetty;
    jetty.stop();
    return jetty;
  }

}