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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
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
  public void testConvertClusterToCdcrAndBootstrap() throws Exception {
    // start the target first so that we know its zkhost
    MiniSolrCloudCluster target = new MiniSolrCloudCluster(1, createTempDir("cdcr-target"), buildJettyConfig("/solr"));
    try {
      target.waitForAllNodes(30);
      System.out.println("Target zkHost = " + target.getZkServer().getZkAddress());
      System.setProperty("cdcr.target.zkHost", target.getZkServer().getZkAddress());

      // start a cluster with no cdcr
      MiniSolrCloudCluster source = new MiniSolrCloudCluster(1, createTempDir("cdcr-source"), buildJettyConfig("/solr"));
      try {
        source.waitForAllNodes(30);
        source.uploadConfigSet(configset("cdcr-source-disabled"), "cdcr-source");

        // create a collection with the cdcr-source-disabled configset
        CollectionAdminRequest.createCollection("cdcr-source", "cdcr-source", 1, 1)
            // todo investigate why this is necessary??? because by default it selects a ram directory which deletes the tlogs on reloads?
            .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
            .process(source.getSolrClient());

        // index 10000 docs with a hard commit every 1000 documents
        CloudSolrClient sourceSolrClient = source.getSolrClient();
        sourceSolrClient.setDefaultCollection("cdcr-source");
        int numDocs = 0;
        for (int k = 0; k < 100; k++) {
          UpdateRequest req = new UpdateRequest();
          for (; numDocs < (k + 1) * 100; numDocs++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "source_" + numDocs);
            doc.addField("xyz", numDocs);
            req.add(doc);
          }
          req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
          System.out.println("Adding 100 docs with commit=true, numDocs=" + numDocs);
          req.process(sourceSolrClient);
        }

        QueryResponse response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        // lets find and keep the maximum version assigned by source cluster across all our updates
        long maxVersion = Long.MIN_VALUE;
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.QT, "/get");
        params.set("getVersions", numDocs);
        response = sourceSolrClient.query(params);
        List<Long> versions = (List<Long>) response.getResponse().get("versions");
        for (Long version : versions) {
          maxVersion = Math.max(maxVersion, version);
        }

//       upload the cdcr-enabled config and restart source cluster
        source.uploadConfigSet(configset("cdcr-source"), "cdcr-source");
        JettySolrRunner runner = source.stopJettySolrRunner(0);
        source.startJettySolrRunner(runner);
        assertTrue(runner.isRunning());
        AbstractDistribZkTestBase.waitForRecoveriesToFinish("cdcr-source", source.getSolrClient().getZkStateReader(), true, true, 330);

        response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("Document mismatch on source after restart", numDocs, response.getResults().getNumFound());

        // setup the target cluster
        target.uploadConfigSet(configset("cdcr-target"), "cdcr-target");
        CollectionAdminRequest.createCollection("cdcr-target", "cdcr-target", 1, 1)
            .process(target.getSolrClient());
        CloudSolrClient targetSolrClient = target.getSolrClient();
        targetSolrClient.setDefaultCollection("cdcr-target");
        Thread.sleep(1000);

        cdcrStart(targetSolrClient);
        cdcrStart(sourceSolrClient);

        response = getCdcrQueue(sourceSolrClient);
        System.out.println("Cdcr queue response: " + response.getResponse());
        long foundDocs = waitForTargetToSync(numDocs, targetSolrClient);
        assertEquals("Document mismatch on target after sync", numDocs, foundDocs);

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

  /**
   * This test start cdcr source, adds data,starts target cluster, verifies replication,
   * stops cdcr replication and buffering, adds more data, re-enables cdcr and verify replication
   */
  public void testBootstrapWithSourceCluster() throws Exception {
    // start the target first so that we know its zkhost
    MiniSolrCloudCluster target = new MiniSolrCloudCluster(1, createTempDir("cdcr-target"), buildJettyConfig("/solr"));
    try {
      target.waitForAllNodes(30);
      System.out.println("Target zkHost = " + target.getZkServer().getZkAddress());
      System.setProperty("cdcr.target.zkHost", target.getZkServer().getZkAddress());

      MiniSolrCloudCluster source = new MiniSolrCloudCluster(1, createTempDir("cdcr-source"), buildJettyConfig("/solr"));
      try {
        source.waitForAllNodes(30);
        source.uploadConfigSet(configset("cdcr-source"), "cdcr-source");

        CollectionAdminRequest.createCollection("cdcr-source", "cdcr-source", 1, 1)
            .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
            .process(source.getSolrClient());

        // index 10000 docs with a hard commit every 1000 documents
        CloudSolrClient sourceSolrClient = source.getSolrClient();
        sourceSolrClient.setDefaultCollection("cdcr-source");
        int numDocs = 0;
        for (int k = 0; k < 100; k++) {
          UpdateRequest req = new UpdateRequest();
          for (; numDocs < (k + 1) * 100; numDocs++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "source_" + numDocs);
            doc.addField("xyz", numDocs);
            req.add(doc);
          }
          req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
          System.out.println("Adding 100 docs with commit=true, numDocs=" + numDocs);
          req.process(sourceSolrClient);
        }

        QueryResponse response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        // setup the target cluster
        target.uploadConfigSet(configset("cdcr-target"), "cdcr-target");
        CollectionAdminRequest.createCollection("cdcr-target", "cdcr-target", 1, 1)
            .process(target.getSolrClient());
        CloudSolrClient targetSolrClient = target.getSolrClient();
        targetSolrClient.setDefaultCollection("cdcr-target");

        cdcrStart(targetSolrClient);
        cdcrStart(sourceSolrClient);

        response = getCdcrQueue(sourceSolrClient);
        System.out.println("Cdcr queue response: " + response.getResponse());
        long foundDocs = waitForTargetToSync(numDocs, targetSolrClient);
        assertEquals("Document mismatch on target after sync", numDocs, foundDocs);

        cdcrStop(sourceSolrClient);
        cdcrDisableBuffer(sourceSolrClient);

        int c = 0;
        for (int k = 0; k < 100; k++) {
          UpdateRequest req = new UpdateRequest();
          for (; c < (k + 1) * 100; c++, numDocs++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "source_" + numDocs);
            doc.addField("xyz", numDocs);
            req.add(doc);
          }
          req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
          System.out.println("Adding 100 docs with commit=true, numDocs=" + numDocs);
          req.process(sourceSolrClient);
        }

        response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        cdcrStart(sourceSolrClient);
        cdcrEnableBuffer(sourceSolrClient);

        foundDocs = waitForTargetToSync(numDocs, targetSolrClient);
        assertEquals("Document mismatch on target after sync", numDocs, foundDocs);

      } finally {
        source.shutdown();
      }
    } finally {
      target.shutdown();
    }
  }

  public void testBootstrapWithContinousIndexingOnSourceCluster() throws Exception {
    // start the target first so that we know its zkhost
    MiniSolrCloudCluster target = new MiniSolrCloudCluster(1, createTempDir("cdcr-target"), buildJettyConfig("/solr"));
    target.waitForAllNodes(30);
    try {
      System.out.println("Target zkHost = " + target.getZkServer().getZkAddress());
      System.setProperty("cdcr.target.zkHost", target.getZkServer().getZkAddress());

      MiniSolrCloudCluster source = new MiniSolrCloudCluster(1, createTempDir("cdcr-source"), buildJettyConfig("/solr"));
      try {
        source.waitForAllNodes(30);
        source.uploadConfigSet(configset("cdcr-source"), "cdcr-source");

        CollectionAdminRequest.createCollection("cdcr-source", "cdcr-source", 1, 1)
            .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
            .process(source.getSolrClient());

        // index 10000 docs with a hard commit every 1000 documents
        CloudSolrClient sourceSolrClient = source.getSolrClient();
        sourceSolrClient.setDefaultCollection("cdcr-source");
        int numDocs = 0;
        for (int k = 0; k < 100; k++) {
          UpdateRequest req = new UpdateRequest();
          for (; numDocs < (k + 1) * 100; numDocs++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "source_" + numDocs);
            doc.addField("xyz", numDocs);
            req.add(doc);
          }
          req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
          System.out.println("Adding 100 docs with commit=true, numDocs=" + numDocs);
          req.process(sourceSolrClient);
        }

        QueryResponse response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        // setup the target cluster
        target.uploadConfigSet(configset("cdcr-target"), "cdcr-target");
        CollectionAdminRequest.createCollection("cdcr-target", "cdcr-target", 1, 1)
            .process(target.getSolrClient());
        CloudSolrClient targetSolrClient = target.getSolrClient();
        targetSolrClient.setDefaultCollection("cdcr-target");
        Thread.sleep(1000);

        cdcrStart(targetSolrClient);
        cdcrStart(sourceSolrClient);

        int c = 0;
        for (int k = 0; k < 100; k++) {
          UpdateRequest req = new UpdateRequest();
          for (; c < (k + 1) * 100; c++, numDocs++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "source_" + numDocs);
            doc.addField("xyz", numDocs);
            req.add(doc);
          }
          req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
          System.out.println("Adding 100 docs with commit=true, numDocs=" + numDocs);
          req.process(sourceSolrClient);
        }

        response = sourceSolrClient.query(new SolrQuery("*:*"));
        assertEquals("", numDocs, response.getResults().getNumFound());

        response = getCdcrQueue(sourceSolrClient);
        System.out.println("Cdcr queue response: " + response.getResponse());
        long foundDocs = waitForTargetToSync(numDocs, targetSolrClient);
        assertEquals("Document mismatch on target after sync", numDocs, foundDocs);

      } finally {
        source.shutdown();
      }
    } finally {
      target.shutdown();
    }
  }

  private long waitForTargetToSync(int numDocs, CloudSolrClient targetSolrClient) throws SolrServerException, IOException, InterruptedException {
    long start = System.nanoTime();
    QueryResponse response = null;
    while (System.nanoTime() - start <= TimeUnit.NANOSECONDS.convert(120, TimeUnit.SECONDS)) {
      try {
        targetSolrClient.commit();
        response = targetSolrClient.query(new SolrQuery("*:*"));
        if (response.getResults().getNumFound() == numDocs) {
          break;
        }
      } catch (Exception e) {
        log.warn("Exception trying to commit on target. This is expected and safe to ignore.", e);
      }
      Thread.sleep(1000);
    }
    return response != null ? response.getResults().getNumFound() : 0;
  }


  private void cdcrStart(CloudSolrClient client) throws SolrServerException, IOException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.START);
    assertEquals("started", ((NamedList) response.getResponse().get("status")).get("process"));
  }

  private void cdcrStop(CloudSolrClient client) throws SolrServerException, IOException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.STOP);
    assertEquals("stopped", ((NamedList) response.getResponse().get("status")).get("process"));
  }

  private void cdcrEnableBuffer(CloudSolrClient client) throws IOException, SolrServerException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.ENABLEBUFFER);
    assertEquals("enabled", ((NamedList) response.getResponse().get("status")).get("buffer"));
  }

  private void cdcrDisableBuffer(CloudSolrClient client) throws IOException, SolrServerException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.DISABLEBUFFER);
    assertEquals("disabled", ((NamedList) response.getResponse().get("status")).get("buffer"));
  }

  private QueryResponse invokeCdcrAction(CloudSolrClient client, CdcrParams.CdcrAction action) throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/cdcr");
    params.set(CommonParams.ACTION, action.toLower());
    return client.query(params);
  }

  private QueryResponse getCdcrQueue(CloudSolrClient client) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/cdcr");
    params.set(CommonParams.ACTION, CdcrParams.QUEUES);
    return client.query(params);
  }
}
