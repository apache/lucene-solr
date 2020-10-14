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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ZkFailoverTest extends SolrCloudTestCase {
  private ZkTestServer zkTestServer;

  @BeforeClass
  public static void setupCluster() throws Exception {
    useFactory("solr.StandardDirectoryFactory");
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void resetWaitForZk(){
    System.setProperty("waitForZk", "30");
  }

  public void testRestartZkWhenClusterDown() throws Exception {
    String coll = "coll1";
    CollectionAdminRequest.createCollection(coll, 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(coll, 2, 2);
    cluster.getSolrClient().add(coll, new SolrInputDocument("id", "1"));
    zkTestServer = cluster.getZkServer();

    // This attempt will fail since it will timeout after 1 second
    System.setProperty("waitForZk", "1");
    restartSolrAndZk();
    waitForLiveNodes(0);

    // This attempt will succeed since there will be enough time to connect
    System.setProperty("waitForZk", "20");
    restartSolrAndZk();
    waitForLiveNodes(2);
    waitForState("Timeout waiting for " + coll, coll, clusterShape(2, 2));
    QueryResponse rsp = new QueryRequest(new SolrQuery("*:*")).process(cluster.getSolrClient(), coll);
    assertEquals(1, rsp.getResults().getNumFound());
    zkTestServer.shutdown();
  }

  private void restartSolrAndZk()
      throws Exception {
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      runner.stop();
    }
    zkTestServer.shutdown();
    Thread[] threads = new Thread[cluster.getJettySolrRunners().size()];
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      final JettySolrRunner runner = cluster.getJettySolrRunner(i);
      threads[i] = new Thread(() -> {
        try {
          runner.start();
        } catch (Exception e) {
          e.printStackTrace();
        }
        });
      threads[i].start();
    }
    Thread.sleep(2500);
    zkTestServer = new ZkTestServer(zkTestServer.getZkDir(), zkTestServer.getPort());
    zkTestServer.run(false);
    for (Thread thread : threads) {
      thread.join();
    }
  }

  private void waitForLiveNodes(int numNodes) throws InterruptedException, KeeperException {
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    for (int i = 0; i < 100; i++) {
      zkStateReader.updateLiveNodes();
      if (zkStateReader.getClusterState().getLiveNodes().size() == numNodes) return;
      Thread.sleep(200);
    }
    fail("Timeout waiting for number of live nodes = " + numNodes);
  }
}
