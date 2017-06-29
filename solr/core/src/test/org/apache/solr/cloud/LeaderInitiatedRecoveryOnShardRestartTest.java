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
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
@Nightly
@AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-10071")
public class LeaderInitiatedRecoveryOnShardRestartTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public LeaderInitiatedRecoveryOnShardRestartTest() throws Exception {
    super();
    sliceCount = 1;
    // we want 3 jetties, but we are using the control jetty as one
    fixShardCount(2);
    useFactory("solr.StandardDirectoryFactory");
  }
  
  @BeforeClass
  public static void before() {
    // we want more realistic leaderVoteWait so raise from
    // test default of 10s to 30s.
    System.setProperty("leaderVoteWait", "300000");
  }
  
  @AfterClass
  public static void after() {
    System.clearProperty("leaderVoteWait");
  }
  
  @Test
  public void testRestartWithAllInLIR() throws Exception {

    // still waiting to be able to properly start with no default collection1,
    // delete to remove confusion
    waitForRecoveriesToFinish(false);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", DEFAULT_COLLECTION);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    String baseUrl = ((HttpSolrClient) clients.get(0)).getBaseURL();
    HttpSolrClient delClient = getHttpSolrClient(baseUrl.substring(0, baseUrl.lastIndexOf("/")));
    delClient.request(request);
    delClient.close();
    
    String testCollectionName = "all_in_lir";
    String shardId = "shard1";
    createCollection(testCollectionName, "conf1", 1, 3, 1);
    
    waitForRecoveriesToFinish(testCollectionName, false);

    cloudClient.setDefaultCollection(testCollectionName);

    Map<String,Object> stateObj = Utils.makeMap();
    stateObj.put(ZkStateReader.STATE_PROP, "down");
    stateObj.put("createdByNodeName", "test");
    stateObj.put("createdByCoreNodeName", "test");
    
    byte[] znodeData = Utils.toJSON(stateObj);
    
    SolrZkClient zkClient = cloudClient.getZkStateReader().getZkClient();
    zkClient.makePath("/collections/" + testCollectionName + "/leader_initiated_recovery/" + shardId + "/core_node1", znodeData, true);
    zkClient.makePath("/collections/" + testCollectionName + "/leader_initiated_recovery/" + shardId + "/core_node2", znodeData, true);
    zkClient.makePath("/collections/" + testCollectionName + "/leader_initiated_recovery/" + shardId + "/core_node3", znodeData, true);
    
    // everyone gets a couple docs so that everyone has tlog entries
    // and won't become leader simply because they have no tlog versions
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, "id", "1");
    SolrInputDocument doc2 = new SolrInputDocument();
    addFields(doc2, "id", "2");
    cloudClient.add(doc);
    cloudClient.add(doc2);

    cloudClient.commit();
    
    assertEquals("We just added 2 docs, we should be able to find them", 2, cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    
    // randomly add too many docs to peer sync to one replica so that only one random replica is the valid leader
    // the versions don't matter, they just have to be higher than what the last 2 docs got
    HttpSolrClient client = (HttpSolrClient) clients.get(random().nextInt(clients.size()));
    client.setBaseURL(client.getBaseURL().substring(0, client.getBaseURL().lastIndexOf("/")) + "/" + testCollectionName);
    params = new ModifiableSolrParams();
    params.set(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
    
    try {
      for (int i = 0; i < 101; i++) {
        add(client, params, sdoc("id", 3 + i, "_version_", Long.MAX_VALUE - 1 - i));
      }
    } catch (RemoteSolrException e) {
      // if we got a conflict it's because we tried to send a versioned doc to the leader,
      // resend without version
      if (e.getMessage().contains("conflict")) {
        for (int i = 0; i < 101; i++) {
          add(client, params, sdoc("id", 3 + i));
        }
      }
    }

    client.commit();
    
    for (JettySolrRunner jetty : jettys) {
      ChaosMonkey.stop(jetty);
    }
    ChaosMonkey.stop(controlJetty);
    
    Thread.sleep(10000);
    
    log.info("Start back up");
    
    for (JettySolrRunner jetty : jettys) {
      ChaosMonkey.start(jetty);
    }
    ChaosMonkey.start(controlJetty);
    
    // recoveries will not finish without SOLR-8075 and SOLR-8367
    waitForRecoveriesToFinish(testCollectionName, true);
    
    // now expire each node
    try {
      zkClient.makePath("/collections/" + testCollectionName + "/leader_initiated_recovery/" + shardId + "/core_node1", znodeData, true);
    } catch (NodeExistsException e) {
    
    }
    try {
      zkClient.makePath("/collections/" + testCollectionName + "/leader_initiated_recovery/" + shardId + "/core_node2", znodeData, true);
    } catch (NodeExistsException e) {
    
    }
    try {
      zkClient.makePath("/collections/" + testCollectionName + "/leader_initiated_recovery/" + shardId + "/core_node3", znodeData, true);
    } catch (NodeExistsException e) {
    
    }
    
    for (JettySolrRunner jetty : jettys) {
      chaosMonkey.expireSession(jetty);
    }
    
    Thread.sleep(2000);
    
    // recoveries will not finish without SOLR-8075 and SOLR-8367
    waitForRecoveriesToFinish(testCollectionName, true);
  }
}
