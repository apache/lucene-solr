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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.cloud.OverseerCollectionConfigSetProcessor.getLeaderNode;
import static org.apache.solr.cloud.OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.NUM_SLICES;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

@LuceneTestCase.Slow
@SuppressSSL(bugUrl = "SOLR-5776")
public class OverseerRolesTest  extends AbstractFullDistribZkTestBase{

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  public OverseerRolesTest() {
    sliceCount = 2;
    fixShardCount(TEST_NIGHTLY ? 6 : 2);
  }

  @Test
  public void test() throws Exception {
    try (CloudSolrClient client = createCloudClient(null))  {
      testQuitCommand(client);
      testOverseerRole(client);
    }
  }

  private void testQuitCommand(CloudSolrClient client) throws Exception{
    String collectionName = "testOverseerQuit";

    createCollection(collectionName, client);

    waitForRecoveriesToFinish(collectionName, false);

    SolrZkClient zk = client.getZkStateReader().getZkClient();
    byte[] data = new byte[0];
    data = zk.getData("/overseer_elect/leader", null, new Stat(), true);
    Map m = (Map) Utils.fromJSON(data);
    String s = (String) m.get("id");
    String leader = LeaderElector.getNodeName(s);
    Overseer.getInQueue(zk).offer(Utils.toJSON(new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.QUIT.toLower())));
    final TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS);
    String newLeader=null;
    for(;! timeout.hasTimedOut();){
      newLeader = OverseerCollectionConfigSetProcessor.getLeaderNode(zk);
      if(newLeader!=null && !newLeader.equals(leader)) break;
      Thread.sleep(100);
    }
    assertNotSame( "Leader not changed yet",newLeader,leader);



    assertTrue("The old leader should have rejoined election ", OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames(zk).contains(leader));
  }




  private void testOverseerRole(CloudSolrClient client) throws Exception {
    String collectionName = "testOverseerCol";

    createCollection(collectionName, client);

    waitForRecoveriesToFinish(collectionName, false);
    List<String> l = OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames(client.getZkStateReader().getZkClient()) ;

    log.info("All nodes {}", l);
    String currentLeader = OverseerCollectionConfigSetProcessor.getLeaderNode(client.getZkStateReader().getZkClient());
    log.info("Current leader {} ", currentLeader);
    l.remove(currentLeader);

    Collections.shuffle(l, random());
    String overseerDesignate = l.get(0);
    log.info("overseerDesignate {}",overseerDesignate);
    setOverseerRole(client, CollectionAction.ADDROLE,overseerDesignate);

    TimeOut timeout = new TimeOut(15, TimeUnit.SECONDS);

    boolean leaderchanged = false;
    for(;!timeout.hasTimedOut();){
      if(overseerDesignate.equals(OverseerCollectionConfigSetProcessor.getLeaderNode(client.getZkStateReader().getZkClient()))){
        log.info("overseer designate is the new overseer");
        leaderchanged =true;
        break;
      }
      Thread.sleep(100);
    }
    assertTrue("could not set the new overseer . expected "+
        overseerDesignate + " current order : " +
        getSortedOverseerNodeNames(client.getZkStateReader().getZkClient()) +
        " ldr :"+ OverseerCollectionConfigSetProcessor.getLeaderNode(client.getZkStateReader().getZkClient()) ,leaderchanged);



    //add another node as overseer


    l.remove(overseerDesignate);

    Collections.shuffle(l, random());

    String anotherOverseer = l.get(0);
    log.info("Adding another overseer designate {}", anotherOverseer);
    setOverseerRole(client, CollectionAction.ADDROLE, anotherOverseer);

    String currentOverseer = getLeaderNode(client.getZkStateReader().getZkClient());

    log.info("Current Overseer {}", currentOverseer);

    String hostPort = currentOverseer.substring(0,currentOverseer.indexOf('_'));

    StringBuilder sb = new StringBuilder();
//
//
    log.info("hostPort : {}", hostPort);

    JettySolrRunner leaderJetty = null;

    for (JettySolrRunner jetty : jettys) {
      String s = jetty.getBaseUrl().toString();
      log.info("jetTy {}",s);
      sb.append(s).append(" , ");
      if (s.contains(hostPort)) {
        leaderJetty = jetty;
        break;
      }
    }

    assertNotNull("Could not find a jetty2 kill",  leaderJetty);

    log.info("leader node {}", leaderJetty.getBaseUrl());
    log.info ("current election Queue",
        OverseerCollectionConfigSetProcessor.getSortedElectionNodes(client.getZkStateReader().getZkClient(),
            "/overseer_elect/election"));
    ChaosMonkey.stop(leaderJetty);
    timeout = new TimeOut(10, TimeUnit.SECONDS);
    leaderchanged = false;
    for (; !timeout.hasTimedOut(); ) {
      currentOverseer = getLeaderNode(client.getZkStateReader().getZkClient());
      if (anotherOverseer.equals(currentOverseer)) {
        leaderchanged = true;
        break;
      }
      Thread.sleep(100);
    }
    assertTrue("New overseer designate has not become the overseer, expected : " + anotherOverseer + "actual : " + getLeaderNode(client.getZkStateReader().getZkClient()), leaderchanged);
  }

  private void setOverseerRole(CloudSolrClient client, CollectionAction action, String overseerDesignate) throws Exception, IOException {
    log.info("Adding overseer designate {} ", overseerDesignate);
    Map m = makeMap(
        "action", action.toString().toLowerCase(Locale.ROOT),
        "role", "overseer",
        "node", overseerDesignate);
    SolrParams params = new MapSolrParams(m);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    client.request(request);
  }


  protected void createCollection(String COLL_NAME, CloudSolrClient client) throws Exception {
    int replicationFactor = 2;
    int numShards = 4;
    int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrClient()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    Map<String, Object> props = makeMap(
        REPLICATION_FACTOR, replicationFactor,
        MAX_SHARDS_PER_NODE, maxShardsPerNode,
        NUM_SLICES, numShards);
    Map<String,List<Integer>> collectionInfos = new HashMap<>();
    createCollection(collectionInfos, COLL_NAME, props, client);
  }


}
