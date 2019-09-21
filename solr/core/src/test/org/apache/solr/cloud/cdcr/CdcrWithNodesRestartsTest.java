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

package org.apache.solr.cloud.cdcr;

import java.lang.invoke.MethodHandles;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.Nightly;

@Nightly // test is too long for non nightly
public class CdcrWithNodesRestartsTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  MiniSolrCloudCluster target, source;
  CloudSolrClient sourceSolrClient, targetSolrClient;
  private static String SOURCE_COLLECTION = "cdcr-source";
  private static String TARGET_COLLECTION = "cdcr-target";
  private static String ALL_Q = "*:*";

  @BeforeClass
  public static void beforeClass() {
    System.clearProperty("solr.httpclient.retries");
    System.clearProperty("solr.retries.on.forward");
    System.clearProperty("solr.retries.to.followers"); 
  }
  
  @Before
  public void before() throws Exception {
    target = new MiniSolrCloudCluster(2, createTempDir(TARGET_COLLECTION), buildJettyConfig("/solr"));
    System.setProperty("cdcr.target.zkHost", target.getZkServer().getZkAddress());
    source = new MiniSolrCloudCluster(2, createTempDir(SOURCE_COLLECTION), buildJettyConfig("/solr"));
  }

  @After
  public void after() throws Exception {
    if (null != target) {
      target.shutdown();
      target = null;
    }
    if (null != source) {
      source.shutdown();
      source = null;
    }
  }

  @Test
  public void testBufferOnNonLeader() throws Exception {
    createCollections();
    CdcrTestsUtil.cdcrDisableBuffer(sourceSolrClient);
    CdcrTestsUtil.cdcrStart(sourceSolrClient);
    Thread.sleep(2000);

    // index 100 docs
    for (int i = 0; i < 100; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      CdcrTestsUtil.index(source, "cdcr-source", doc);
      sourceSolrClient.commit();
    }
    Thread.sleep(2000);

    // restart all the nodes at source cluster, one by one
    CdcrTestsUtil.restartClusterNodes(source, SOURCE_COLLECTION);

    //verify cdcr has replicated docs
    QueryResponse response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 100, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 100, CdcrTestsUtil.waitForClusterToSync(100, targetSolrClient));
    CdcrTestsUtil.assertShardInSync(SOURCE_COLLECTION, "shard1", sourceSolrClient);
    CdcrTestsUtil.assertShardInSync(TARGET_COLLECTION, "shard1", targetSolrClient);

    CdcrTestsUtil.cdcrStop(sourceSolrClient);
    CdcrTestsUtil.cdcrStop(targetSolrClient);

    deleteCollections();
  }

  @Test
  public void testUpdateLogSynchronisation() throws Exception {
    createCollections();
    CdcrTestsUtil.cdcrStart(sourceSolrClient);
    Thread.sleep(2000);

    // index 100 docs
    for (int i = 0; i < 100; i++) {
      // will perform a commit for every document and will create one tlog file per commit
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      CdcrTestsUtil.index(source, "cdcr-source", doc, true);
    }
    Thread.sleep(2000);

    //verify cdcr has replicated docs
    QueryResponse response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 100, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 100, CdcrTestsUtil.waitForClusterToSync(100, targetSolrClient));

    // Get the number of tlog files on the replicas (should be equal to the number of documents indexed)
    int nTlogs = CdcrTestsUtil.getNumberOfTlogFilesOnReplicas(source);

    // Disable the buffer - ulog synch should start on non-leader nodes
    CdcrTestsUtil.cdcrDisableBuffer(sourceSolrClient);
    Thread.sleep(2000);

    int cnt = 15; // timeout after 15 seconds
    int n = 0;
    while (cnt > 0) {
      // Index a new document with a commit to trigger update log cleaning
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + random().nextLong());
      CdcrTestsUtil.index(source, "cdcr-source", doc, true);

      // Check the update logs on non-leader nodes, the number of tlog files should decrease
      n = CdcrTestsUtil.getNumberOfTlogFilesOnReplicas(source);
      if (n < nTlogs) {
        cnt = Integer.MIN_VALUE;
        break;
      }
      cnt--;
      Thread.sleep(1000);
    }
    if (cnt == 0) {
      throw new AssertionError("Timeout while trying to assert update logs @ source_collection, " + n + " " + nTlogs);
    }

    CdcrTestsUtil.cdcrStop(sourceSolrClient);
    CdcrTestsUtil.cdcrStop(targetSolrClient);

    deleteCollections();
  }

  @Test
  public void testReplicationAfterRestart() throws Exception {
    createCollections();
    CdcrTestsUtil.cdcrStart(sourceSolrClient); // start CDCR
    Thread.sleep(2000);

    //index 100 docs
    for (int i = 0; i < 100; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      CdcrTestsUtil.index(source, "cdcr-source", doc);
      sourceSolrClient.commit();
    }
    Thread.sleep(2000);

    // verify cdcr has replicated docs
    QueryResponse response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 100, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 100, CdcrTestsUtil.waitForClusterToSync(100, targetSolrClient));
    CdcrTestsUtil.assertShardInSync("cdcr-source", "shard1", sourceSolrClient);

    // restart all the source cluster nodes
    CdcrTestsUtil.restartClusterNodes(source, "cdcr-source");
    sourceSolrClient = source.getSolrClient();
    sourceSolrClient.setDefaultCollection("cdcr-source");

    // verify still the docs are there
    response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 100, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 100, CdcrTestsUtil.waitForClusterToSync(100, targetSolrClient));

    // index 100 more
    for (int i = 100; i < 200; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      CdcrTestsUtil.index(source, "cdcr-source", doc);
      sourceSolrClient.commit();
    }
    Thread.sleep(2000);

    // verify still the docs are there
    response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 200, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 200, CdcrTestsUtil.waitForClusterToSync(200, targetSolrClient));

    CdcrTestsUtil.cdcrStop(sourceSolrClient);
    CdcrTestsUtil.cdcrStop(targetSolrClient);

    deleteCollections();
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testReplicationAfterLeaderChange() throws Exception {
    createCollections();
    CdcrTestsUtil.cdcrStart(sourceSolrClient);
    Thread.sleep(2000);

    // index 100 docs
    for (int i = 0; i < 100; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      CdcrTestsUtil.index(source, "cdcr-source", doc);
      sourceSolrClient.commit();
    }
    Thread.sleep(2000);

    // verify cdcr has replicated docs
    QueryResponse response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 100, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 100, CdcrTestsUtil.waitForClusterToSync(100, targetSolrClient));
    CdcrTestsUtil.assertShardInSync("cdcr-source", "shard1", sourceSolrClient);

    // restart one of the source cluster nodes
    CdcrTestsUtil.restartClusterNode(source, "cdcr-source", 0);
    sourceSolrClient = source.getSolrClient();
    sourceSolrClient.setDefaultCollection("cdcr-source");

    // add `100 more docs, 200 until now
    for (int i = 100; i < 200; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      CdcrTestsUtil.index(source, "cdcr-source", doc);
      sourceSolrClient.commit();
    }
    Thread.sleep(2000);

    // verify cdcr has replicated docs
    response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 200, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 200, CdcrTestsUtil.waitForClusterToSync(200, targetSolrClient));

    // restart the other source cluster node
    CdcrTestsUtil.restartClusterNode(source, "cdcr-source", 1);
    sourceSolrClient = source.getSolrClient();
    sourceSolrClient.setDefaultCollection("cdcr-source");

    // add `100 more docs, 300 until now
    for (int i = 200; i < 300; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      CdcrTestsUtil.index(source, "cdcr-source", doc);
      sourceSolrClient.commit();
    }
    Thread.sleep(2000);

    // verify cdcr has replicated docs
    response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 300, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 300, CdcrTestsUtil.waitForClusterToSync(300, targetSolrClient));

    // add a replica to 'target' collection
    CollectionAdminRequest.addReplicaToShard(TARGET_COLLECTION, "shard1").
        setNode(CdcrTestsUtil.getNonLeaderNode(target, TARGET_COLLECTION)).process(targetSolrClient);
    Thread.sleep(2000);

    // restart one of the target nodes
    CdcrTestsUtil.restartClusterNode(source, "cdcr-target", 0);
    targetSolrClient = target.getSolrClient();
    targetSolrClient.setDefaultCollection("cdcr-target");

    // add `100 more docs, 400 until now
    for (int i = 300; i < 400; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      CdcrTestsUtil.index(source, "cdcr-source", doc);
      sourceSolrClient.commit();
    }
    Thread.sleep(2000);

    // verify cdcr has replicated docs
    response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 400, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 400, CdcrTestsUtil.waitForClusterToSync(400, targetSolrClient));

    // restart the other target cluster node
    CdcrTestsUtil.restartClusterNode(source, "cdcr-target", 1);
    targetSolrClient = target.getSolrClient();
    targetSolrClient.setDefaultCollection("cdcr-target");

    // add `100 more docs, 500 until now
    for (int i = 400; i < 500; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc_" + i);
      CdcrTestsUtil.index(source, "cdcr-source", doc);
      sourceSolrClient.commit();
    }
    Thread.sleep(2000);

    // verify cdcr has replicated docs
    response = sourceSolrClient.query(new SolrQuery(ALL_Q));
    assertEquals("source docs mismatch", 500, response.getResults().getNumFound());
    assertEquals("target docs mismatch", 500, CdcrTestsUtil.waitForClusterToSync(500, targetSolrClient));

    CdcrTestsUtil.cdcrStop(sourceSolrClient);
    CdcrTestsUtil.cdcrStop(targetSolrClient);

    deleteCollections();
  }

  private void createSourceCollection() throws Exception {
    source.uploadConfigSet(configset("cdcr-source"), "cdcr-source");
    CollectionAdminRequest.createCollection("cdcr-source", "cdcr-source", 1, 2)
        .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
        .process(source.getSolrClient());
    Thread.sleep(1000);
    sourceSolrClient = source.getSolrClient();
    sourceSolrClient.setDefaultCollection("cdcr-source");
  }

  private void createTargetCollection() throws Exception {
    target.uploadConfigSet(configset("cdcr-target"), "cdcr-target");
    CollectionAdminRequest.createCollection("cdcr-target", "cdcr-target", 1, 1)
        .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
        .process(target.getSolrClient());
    Thread.sleep(1000);
    targetSolrClient = target.getSolrClient();
    targetSolrClient.setDefaultCollection("cdcr-target");
  }

  private void deleteSourceCollection() throws Exception {
    source.deleteAllCollections();
  }

  private void deleteTargetcollection() throws Exception {
    target.deleteAllCollections();
  }

  private void createCollections() throws Exception {
    createTargetCollection();
    createSourceCollection();
  }

  private void deleteCollections() throws Exception {
    deleteSourceCollection();
    deleteTargetcollection();
  }

}
