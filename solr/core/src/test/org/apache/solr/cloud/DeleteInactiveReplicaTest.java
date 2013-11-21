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

import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

import java.net.URL;
import java.util.Map;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;

public class DeleteInactiveReplicaTest extends DeleteReplicaTest{
  private CloudSolrServer client;

  @Override
  public void doTest() throws Exception {
    deleteInactiveReplicaTest();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    client = createCloudClient(null);
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    client.shutdown();
  }
  
  private void deleteInactiveReplicaTest() throws Exception{
    String COLL_NAME = "delDeadColl";

    createColl(COLL_NAME, client);

    boolean stopped = false;
    JettySolrRunner stoppedJetty = null;
    StringBuilder sb = new StringBuilder();
    Replica replica1=null;
    Slice shard1 = null;
    DocCollection testcoll = getCommonCloudSolrServer().getZkStateReader().getClusterState().getCollection(COLL_NAME);
    for (JettySolrRunner jetty : jettys) sb.append(jetty.getBaseUrl()).append(",");

    for (Slice slice : testcoll.getActiveSlices()) {
      for (Replica replica : slice.getReplicas())
        for (JettySolrRunner jetty : jettys) {
          URL baseUrl = null;
          try {
            baseUrl = jetty.getBaseUrl();
          } catch (Exception e) {
            continue;
          }
          if (baseUrl.toString().startsWith(replica.getStr(ZkStateReader.BASE_URL_PROP))) {
            stoppedJetty = jetty;
            ChaosMonkey.stop(jetty);
            replica1 = replica;
            shard1 = slice;
            stopped = true;
            break;
          }
        }
    }

    /*final Slice shard1 = testcoll.getSlices().iterator().next();
    if(!shard1.getState().equals(Slice.ACTIVE)) fail("shard is not active");
    Replica replica1 = shard1.getReplicas().iterator().next();
    JettySolrRunner stoppedJetty = null;
    StringBuilder sb = new StringBuilder();
    for (JettySolrRunner jetty : jettys) {
      sb.append(jetty.getBaseUrl()).append(",");
      if( jetty.getBaseUrl().toString().startsWith(replica1.getStr(ZkStateReader.BASE_URL_PROP)) ) {
        stoppedJetty = jetty;
        ChaosMonkey.stop(jetty);
        stopped = true;
        break;
      }
    }*/
    if(!stopped){
      fail("Could not find jetty to stop in collection "+ testcoll + " jettys: "+sb);
    }

    long endAt = System.currentTimeMillis()+3000;
    boolean success = false;
    while(System.currentTimeMillis() < endAt){
      testcoll = getCommonCloudSolrServer().getZkStateReader().getClusterState().getCollection(COLL_NAME);
      if(!"active".equals(testcoll.getSlice(shard1.getName()).getReplica(replica1.getName()).getStr(Slice.STATE))  ){
        success=true;
      }
      if(success) break;
      Thread.sleep(100);
    }
    log.info("removed_replicas {}/{} ",shard1.getName(),replica1.getName());
    removeAndWaitForReplicaGone(COLL_NAME, client, replica1, shard1.getName());

    ChaosMonkey.start(stoppedJetty);
    log.info("restarted jetty");


    Map m = makeMap("qt","/admin/cores",
        "action", "status");

    NamedList<Object> resp = new HttpSolrServer(replica1.getStr("base_url")).request(new QueryRequest(new MapSolrParams(m)));
    assertNull( "The core is up and running again" , ((NamedList)resp.get("status")).get(replica1.getStr("core")));

  }
}
