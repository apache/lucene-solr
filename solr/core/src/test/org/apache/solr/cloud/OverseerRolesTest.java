package org.apache.solr.cloud;

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


import static org.apache.solr.cloud.OverseerCollectionProcessor.MAX_SHARDS_PER_NODE;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.REPLICATION_FACTOR;
import static org.apache.solr.cloud.OverseerCollectionProcessor.getSortedNodeNames;
import static org.apache.solr.cloud.OverseerCollectionProcessor.getLeaderNode;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
@LuceneTestCase.Slow
public class OverseerRolesTest  extends AbstractFullDistribZkTestBase{
  private CloudSolrServer client;

  static {
    // SSL does not work with this feature for some reason
    ALLOW_SSL = false;
  }
  
  @BeforeClass
  public static void beforeThisClass2() throws Exception {

  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
    client = createCloudClient(null);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    client.shutdown();
  }

  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  public OverseerRolesTest() {
    fixShardCount = true;

    sliceCount = 2;
    shardCount = 6;

    checkCreatedVsState = false;
  }

  @Override
  public void doTest() throws Exception {
    addOverseerRole2ExistingNodes();

  }

  private void addOverseerRole2ExistingNodes() throws Exception {
    String collectionName = "testOverseerCol";

    createCollection(collectionName, client);

    waitForRecoveriesToFinish(collectionName, false);
    List<String> l = OverseerCollectionProcessor.getSortedNodeNames(client.getZkStateReader().getZkClient()) ;

    log.info("All nodes {}", l);
    String currentLeader = OverseerCollectionProcessor.getLeaderNode(client.getZkStateReader().getZkClient());
    log.info("Current leader {} ", currentLeader);
    l.remove(currentLeader);

    Collections.shuffle(l);
    String overseerDesignate = l.get(0);
    log.info("overseerDesignate {}",overseerDesignate);
    setOverseerRole(CollectionAction.ADDROLE,overseerDesignate);

    long timeout = System.currentTimeMillis()+15000;

    boolean leaderchanged = false;
    for(;System.currentTimeMillis() < timeout;){
      if(overseerDesignate.equals(OverseerCollectionProcessor.getLeaderNode(client.getZkStateReader().getZkClient()))){
        log.info("overseer designate is the new overseer");
        leaderchanged =true;
        break;
      }
      Thread.sleep(100);
    }
    /*if(!leaderchanged){

      log.warn("expected {}, current order {}",
          overseerDesignate,
          getSortedNodeNames(client.getZkStateReader().getZkClient())+ " ldr :"+ OverseerCollectionProcessor.getLeaderNode(client.getZkStateReader().getZkClient()) );
    }*/
    assertTrue("could not set the new overseer . expected "+
        overseerDesignate + " current order : " +
        getSortedNodeNames(client.getZkStateReader().getZkClient()) +
        " ldr :"+ OverseerCollectionProcessor.getLeaderNode(client.getZkStateReader().getZkClient()) ,leaderchanged);



    //add another node as overseer


    l.remove(overseerDesignate);

    Collections.shuffle(l);

    String anotherOverseer = l.get(0);
    log.info("Adding another overseer designate {}", anotherOverseer);
    setOverseerRole(CollectionAction.ADDROLE, anotherOverseer);

    timeout = System.currentTimeMillis()+10000;
    leaderchanged = false;
    for(;System.currentTimeMillis() < timeout;){
      List<String> sortedNodeNames = getSortedNodeNames(client.getZkStateReader().getZkClient());
      if(sortedNodeNames.get(1) .equals(anotherOverseer) || sortedNodeNames.get(0).equals(anotherOverseer)){
        leaderchanged =true;
        break;
      }
      Thread.sleep(100);
    }

    assertTrue("New overseer not the frontrunner : "+ getSortedNodeNames(client.getZkStateReader().getZkClient()) + " expected : "+ anotherOverseer, leaderchanged);


    String currentOverseer = getLeaderNode(client.getZkStateReader().getZkClient());

    log.info("Current Overseer {}", currentOverseer);
    Pattern pattern = Pattern.compile("(.*):(\\d*)(.*)");
    Matcher m = pattern.matcher(currentOverseer);
    if(m.matches()){
      String hostPort =  m.group(1)+":"+m.group(2);

      log.info("hostPort : {}", hostPort);

      for (JettySolrRunner jetty : jettys) {
        String s = jetty.getBaseUrl().toString();
        if(s.contains(hostPort)){
          log.info("leader node {}",s);
          ChaosMonkey.stop(jetty);

          timeout = System.currentTimeMillis()+10000;
          leaderchanged = false;
          for(;System.currentTimeMillis() < timeout;){
            currentOverseer =  getLeaderNode(client.getZkStateReader().getZkClient());
            if(anotherOverseer.equals(currentOverseer)){
              leaderchanged =true;
              break;
            }
            Thread.sleep(100);
          }
          assertTrue("New overseer designate has not become the overseer, expected : "+ anotherOverseer + "actual : "+ currentOverseer, leaderchanged);
        }

      }

    }

    client.shutdown();


  }

  private void setOverseerRole(CollectionAction action, String overseerDesignate) throws Exception, IOException {
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


  protected void createCollection(String COLL_NAME, CloudSolrServer client) throws Exception {
    int replicationFactor = 2;
    int numShards = 4;
    int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrServer()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    Map<String, Object> props = makeMap(
        REPLICATION_FACTOR, replicationFactor,
        MAX_SHARDS_PER_NODE, maxShardsPerNode,
        NUM_SLICES, numShards);
    Map<String,List<Integer>> collectionInfos = new HashMap<String,List<Integer>>();
    createCollection(collectionInfos, COLL_NAME, props, client);
  }


}
