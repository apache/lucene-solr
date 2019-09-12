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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;

import org.apache.lucene.store.FSDirectory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.CdcrParams;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcrBootstrapTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Starts a source cluster with no CDCR configuration, indexes enough documents such that
   * the at least one old tlog is closed and thrown away so that the source cluster does not have
   * all updates available in tlogs only.
   * <p>
   * Then we start a target cluster with CDCR configuration and we change the source cluster configuration
   * to use CDCR (i.e. CdcrUpdateLog, CdcrRequestHandler and CdcrUpdateProcessor) and restart it.
   * <p>
   * We test that all updates eventually make it to the target cluster and that the collectioncheckpoint
   * call returns the same version as the last update indexed on the source.
   */
  @Test
  // commented 4-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testConvertClusterToCdcrAndBootstrap() throws Exception {
    // start the target first so that we know its zkhost
    MiniSolrCloudCluster target = new MiniSolrCloudCluster(1, createTempDir("cdcr-target"), buildJettyConfig("/solr"));
    try {
      log.info("Target zkHost = " + target.getZkServer().getZkAddress());
      System.setProperty("cdcr.target.zkHost", target.getZkServer().getZkAddress());

      // start a cluster with no cdcr
      MiniSolrCloudCluster source = new MiniSolrCloudCluster(1, createTempDir("cdcr-source"), buildJettyConfig("/solr"));
      try {
        source.uploadConfigSet(configset("cdcr-source-disabled"), "cdcr-source");

        // create a collection with the cdcr-source-disabled configset
        CollectionAdminRequest.createCollection("cdcr-source", "cdcr-source", 1, 1)
            // todo investigate why this is necessary??? because by default it selects a ram directory which deletes the tlogs on reloads?
            .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
            .process(source.getSolrClient());
        source.waitForActiveCollection("cdcr-source", 1, 1);
        CloudSolrClient sourceSolrClient = source.getSolrClient();
        int docs = (TEST_NIGHTLY ? 100 : 10);
        int numDocs = indexDocs(sourceSolrClient, "cdcr-source", docs);

        QueryResponse response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        // lets find and keep the maximum version assigned by source cluster across all our updates
        long maxVersion = Long.MIN_VALUE;
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.QT, "/get");
        params.set("getVersions", numDocs);
        params.set("fingerprint", true);
        response = sourceSolrClient.query(params);
        maxVersion = (long)(((LinkedHashMap)response.getResponse().get("fingerprint")).get("maxVersionEncountered"));

//       upload the cdcr-enabled config and restart source cluster
        source.uploadConfigSet(configset("cdcr-source"), "cdcr-source");
        JettySolrRunner runner = source.stopJettySolrRunner(0);
        source.waitForJettyToStop(runner);
        
        source.startJettySolrRunner(runner);
        source.waitForAllNodes(30);
        assertTrue(runner.isRunning());
        AbstractDistribZkTestBase.waitForRecoveriesToFinish("cdcr-source", source.getSolrClient().getZkStateReader(), true, true, 330);

        response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("Document mismatch on source after restart", numDocs, response.getResults().getNumFound());

        // setup the target cluster
        target.uploadConfigSet(configset("cdcr-target"), "cdcr-target");
        CollectionAdminRequest.createCollection("cdcr-target", "cdcr-target", 1, 2)
            .setMaxShardsPerNode(2)
            .process(target.getSolrClient());
        target.waitForActiveCollection("cdcr-target", 1, 2);
        CloudSolrClient targetSolrClient = target.getSolrClient();
        targetSolrClient.setDefaultCollection("cdcr-target");
        Thread.sleep(6000);

        CdcrTestsUtil.cdcrStart(targetSolrClient);
        CdcrTestsUtil.cdcrStart(sourceSolrClient);

        response = CdcrTestsUtil.getCdcrQueue(sourceSolrClient);
        log.info("Cdcr queue response: " + response.getResponse());
        long foundDocs = CdcrTestsUtil.waitForClusterToSync(numDocs, targetSolrClient);
        assertEquals("Document mismatch on target after sync", numDocs, foundDocs);
        assertTrue(CdcrTestsUtil.assertShardInSync("cdcr-target", "shard1", targetSolrClient)); // with more than 1 replica

        params = new ModifiableSolrParams();
        params.set(CommonParams.ACTION, CdcrParams.CdcrAction.COLLECTIONCHECKPOINT.toString());
        params.set(CommonParams.QT, "/cdcr");
        response = targetSolrClient.query(params);
        Long checkpoint = (Long) response.getResponse().get(CdcrParams.CHECKPOINT);
        assertNotNull(checkpoint);
        assertEquals("COLLECTIONCHECKPOINT from target cluster should have returned the maximum " +
            "version across all updates made to source", maxVersion, checkpoint.longValue());
      } finally {
        source.shutdown();
      }
    } finally {
      target.shutdown();
    }
  }

  private int indexDocs(CloudSolrClient sourceSolrClient, String collection, int batches) throws IOException, SolrServerException {
    sourceSolrClient.setDefaultCollection(collection);
    int numDocs = 0;
    for (int k = 0; k < batches; k++) {
      UpdateRequest req = new UpdateRequest();
      for (; numDocs < (k + 1) * 100; numDocs++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "source_" + numDocs);
        doc.addField("xyz", numDocs);
        req.add(doc);
      }
      req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
      req.process(sourceSolrClient);
    }
    log.info("Adding numDocs=" + numDocs);
    return numDocs;
  }
  /**
   * This test start cdcr source, adds data,starts target cluster, verifies replication,
   * stops cdcr replication and buffering, adds more data, re-enables cdcr and verify replication
   */
  public void testBootstrapWithSourceCluster() throws Exception {
    // start the target first so that we know its zkhost
    MiniSolrCloudCluster target = new MiniSolrCloudCluster(1, createTempDir("cdcr-target"), buildJettyConfig("/solr"));
    try {
      System.out.println("Target zkHost = " + target.getZkServer().getZkAddress());
      System.setProperty("cdcr.target.zkHost", target.getZkServer().getZkAddress());

      MiniSolrCloudCluster source = new MiniSolrCloudCluster(1, createTempDir("cdcr-source"), buildJettyConfig("/solr"));
      try {
        source.uploadConfigSet(configset("cdcr-source"), "cdcr-source");

        CollectionAdminRequest.createCollection("cdcr-source", "cdcr-source", 1, 1)
            .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
            .process(source.getSolrClient());
        source.waitForActiveCollection("cdcr-source", 1, 1);

        CloudSolrClient sourceSolrClient = source.getSolrClient();
        int docs = (TEST_NIGHTLY ? 100 : 10);
        int numDocs = indexDocs(sourceSolrClient, "cdcr-source", docs);

        QueryResponse response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        // setup the target cluster
        target.uploadConfigSet(configset("cdcr-target"), "cdcr-target");
        CollectionAdminRequest.createCollection("cdcr-target", "cdcr-target", 1, 1)
            .process(target.getSolrClient());
        target.waitForActiveCollection("cdcr-target", 1, 1);
        CloudSolrClient targetSolrClient = target.getSolrClient();
        targetSolrClient.setDefaultCollection("cdcr-target");

        CdcrTestsUtil.cdcrStart(targetSolrClient);
        CdcrTestsUtil.cdcrStart(sourceSolrClient);

        response = CdcrTestsUtil.getCdcrQueue(sourceSolrClient);
        log.info("Cdcr queue response: " + response.getResponse());
        long foundDocs = CdcrTestsUtil.waitForClusterToSync(numDocs, targetSolrClient);
        assertEquals("Document mismatch on target after sync", numDocs, foundDocs);

        int total_tlogs_in_index = FSDirectory.open(target.getBaseDir().resolve("node1").
            resolve("cdcr-target_shard1_replica_n1").resolve("data").
            resolve("tlog")).listAll().length;

        assertEquals("tlogs count should be ZERO",0, total_tlogs_in_index);

        CdcrTestsUtil.cdcrStop(sourceSolrClient);
        CdcrTestsUtil.cdcrDisableBuffer(sourceSolrClient);

        int c = 0;
        for (int k = 0; k < 10; k++) {
          UpdateRequest req = new UpdateRequest();
          for (; c < (k + 1) * 100; c++, numDocs++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "source_" + numDocs);
            doc.addField("xyz", numDocs);
            req.add(doc);
          }
          req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
          log.info("Adding 100 docs with commit=true, numDocs=" + numDocs);
          req.process(sourceSolrClient);
        }

        response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        CdcrTestsUtil.cdcrStart(sourceSolrClient);
        CdcrTestsUtil.cdcrEnableBuffer(sourceSolrClient);

        foundDocs = CdcrTestsUtil.waitForClusterToSync(numDocs, targetSolrClient);
        assertEquals("Document mismatch on target after sync", numDocs, foundDocs);

      } finally {
        source.shutdown();
      }
    } finally {
      target.shutdown();
    }
  }

  /**
   * This test successfully validates the follower nodes at target copies content
   * from their respective leaders
   */
  public void testBootstrapWithMultipleReplicas() throws Exception {
    // start the target first so that we know its zkhost
    MiniSolrCloudCluster target = new MiniSolrCloudCluster(3, createTempDir("cdcr-target"), buildJettyConfig("/solr"));
    try {
      System.out.println("Target zkHost = " + target.getZkServer().getZkAddress());
      System.setProperty("cdcr.target.zkHost", target.getZkServer().getZkAddress());

      MiniSolrCloudCluster source = new MiniSolrCloudCluster(3, createTempDir("cdcr-source"), buildJettyConfig("/solr"));
      try {
        source.uploadConfigSet(configset("cdcr-source"), "cdcr-source");

        CollectionAdminRequest.createCollection("cdcr-source", "cdcr-source", 1, 3)
            .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
            .process(source.getSolrClient());
        source.waitForActiveCollection("cdcr-source", 1, 3);

        CloudSolrClient sourceSolrClient = source.getSolrClient();
        int docs = (TEST_NIGHTLY ? 100 : 10);
        int numDocs = indexDocs(sourceSolrClient, "cdcr-source", docs);

        QueryResponse response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        // setup the target cluster
        target.uploadConfigSet(configset("cdcr-target"), "cdcr-target");
        CollectionAdminRequest.createCollection("cdcr-target", "cdcr-target", 1, 3)
            .process(target.getSolrClient());
        target.waitForActiveCollection("cdcr-target", 1, 3);
        CloudSolrClient targetSolrClient = target.getSolrClient();
        targetSolrClient.setDefaultCollection("cdcr-target");

        CdcrTestsUtil.cdcrStart(targetSolrClient);
        CdcrTestsUtil.cdcrStart(sourceSolrClient);

        response = CdcrTestsUtil.getCdcrQueue(sourceSolrClient);
        log.info("Cdcr queue response: " + response.getResponse());
        long foundDocs = CdcrTestsUtil.waitForClusterToSync(numDocs, targetSolrClient);
        assertEquals("Document mismatch on target after sync", numDocs, foundDocs);
        assertTrue("leader followers didnt' match", CdcrTestsUtil.assertShardInSync("cdcr-target", "shard1", targetSolrClient)); // with more than 1 replica

      } finally {
        source.shutdown();
      }
    } finally {
      target.shutdown();
    }
  }

  // 29-June-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 6-Sep-2018
  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-12028")
  public void testBootstrapWithContinousIndexingOnSourceCluster() throws Exception {
    // start the target first so that we know its zkhost
    MiniSolrCloudCluster target = new MiniSolrCloudCluster(1, createTempDir("cdcr-target"), buildJettyConfig("/solr"));
    try {
      log.info("Target zkHost = " + target.getZkServer().getZkAddress());
      System.setProperty("cdcr.target.zkHost", target.getZkServer().getZkAddress());

      MiniSolrCloudCluster source = new MiniSolrCloudCluster(1, createTempDir("cdcr-source"), buildJettyConfig("/solr"));
      try {
        source.uploadConfigSet(configset("cdcr-source"), "cdcr-source");

        CollectionAdminRequest.createCollection("cdcr-source", "cdcr-source", 1, 1)
            .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
            .process(source.getSolrClient());
        source.waitForActiveCollection("cdcr-source", 1, 1);
        CloudSolrClient sourceSolrClient = source.getSolrClient();
        int docs = (TEST_NIGHTLY ? 100 : 10);
        int numDocs = indexDocs(sourceSolrClient, "cdcr-source", docs);

        QueryResponse response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        // setup the target cluster
        target.uploadConfigSet(configset("cdcr-target"), "cdcr-target");
        CollectionAdminRequest.createCollection("cdcr-target", "cdcr-target", 1, 1)
            .process(target.getSolrClient());
        target.waitForActiveCollection("cdcr-target", 1, 1);
        CloudSolrClient targetSolrClient = target.getSolrClient();
        targetSolrClient.setDefaultCollection("cdcr-target");
        Thread.sleep(1000);

        CdcrTestsUtil.cdcrStart(targetSolrClient);
        CdcrTestsUtil.cdcrStart(sourceSolrClient);
        int c = 0;
        for (int k = 0; k < docs; k++) {
          UpdateRequest req = new UpdateRequest();
          for (; c < (k + 1) * 100; c++, numDocs++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "source_" + numDocs);
            doc.addField("xyz", numDocs);
            req.add(doc);
          }
          req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
          log.info("Adding " + docs + " docs with commit=true, numDocs=" + numDocs);
          req.process(sourceSolrClient);
        }

        response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        response = CdcrTestsUtil.getCdcrQueue(sourceSolrClient);
        log.info("Cdcr queue response: " + response.getResponse());
        long foundDocs = CdcrTestsUtil.waitForClusterToSync(numDocs, targetSolrClient);
        assertEquals("Document mismatch on target after sync", numDocs, foundDocs);

      } finally {
        source.shutdown();
      }
    } finally {
      target.shutdown();
    }
  }
}
