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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM;

@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class DistributedVersionInfoTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int maxWaitSecsToSeeAllActive = 30;

  @Test
  public void test() throws Exception {
    waitForThingsToLevelOut(30000);

    log.info("DistributedVersionInfoTest RUNNING");

    testReplicaVersionHandling();

    log.info("DistributedVersionInfoTest succeeded ... shutting down now!");
  }

  protected void testReplicaVersionHandling() throws Exception {
    final String testCollectionName = "c8n_vers_1x3";
    String shardId = "shard1";
    int rf = 3;
    createCollectionRetry(testCollectionName, 1, rf, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    final Replica leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, shardId);
    List<Replica> notLeaders =
        ensureAllReplicasAreActive(testCollectionName, shardId, 1, rf, maxWaitSecsToSeeAllActive);

    // start by reloading the empty collection so we try to calculate the max from an empty index
    reloadCollection(leader, testCollectionName);
    notLeaders =
        ensureAllReplicasAreActive(testCollectionName, shardId, 1, rf, maxWaitSecsToSeeAllActive);

    sendDoc(1);
    cloudClient.commit();

    // verify doc is on the leader and replica
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 1, null);

    // get max version from the leader and replica
    Replica replica = notLeaders.get(0);
    Long maxOnLeader = getMaxVersionFromIndex(leader);
    Long maxOnReplica = getMaxVersionFromIndex(replica);
    assertEquals("leader and replica should have same max version: " + maxOnLeader, maxOnLeader, maxOnReplica);

    // send the same doc but with a lower version than the max in the index
    try (SolrClient client = new HttpSolrClient(replica.getCoreUrl())) {
      String docId = String.valueOf(1);
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField(id, docId);
      doc.setField("_version_", maxOnReplica - 1); // bad version!!!

      // simulate what the leader does when sending a doc to a replica
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(DISTRIB_UPDATE_PARAM, DistributedUpdateProcessor.DistribPhase.FROMLEADER.toString());
      params.set(DISTRIB_FROM, leader.getCoreUrl());

      UpdateRequest req = new UpdateRequest();
      req.setParams(params);
      req.add(doc);

      log.info("Sending doc with out-of-date version ("+(maxOnReplica -1)+") document directly to replica");

      client.request(req);
      client.commit();

      Long docVersion = getVersionFromIndex(replica, docId);
      assertEquals("older version should have been thrown away", maxOnReplica, docVersion);
    }

    reloadCollection(leader, testCollectionName);

    maxOnLeader = getMaxVersionFromIndex(leader);
    maxOnReplica = getMaxVersionFromIndex(replica);
    assertEquals("leader and replica should have same max version after reload", maxOnLeader, maxOnReplica);

    // now start sending docs while collection is reloading

    delQ("*:*");
    commit();

    final Set<Integer> deletedDocs = new HashSet<>();
    final AtomicInteger docsSent = new AtomicInteger(0);
    final Random rand = new Random(5150);
    Thread docSenderThread = new Thread() {
      public void run() {

        // brief delay before sending docs
        try {
          Thread.sleep(rand.nextInt(30)+1);
        } catch (InterruptedException e) {}

        for (int i=0; i < 1000; i++) {
          if (i % (rand.nextInt(20)+1) == 0) {
            try {
              Thread.sleep(rand.nextInt(50)+1);
            } catch (InterruptedException e) {}
          }

          int docId = i+1;
          try {
            sendDoc(docId);
            docsSent.incrementAndGet();
          } catch (Exception e) {}
        }
      }
    };

    Thread reloaderThread = new Thread() {
      public void run() {
        try {
          Thread.sleep(rand.nextInt(300)+1);
        } catch (InterruptedException e) {}

        for (int i=0; i < 3; i++) {
          try {
            reloadCollection(leader, testCollectionName);
          } catch (Exception e) {}

          try {
            Thread.sleep(rand.nextInt(300)+300);
          } catch (InterruptedException e) {}
        }
      }
    };

    Thread deleteThread = new Thread() {
      public void run() {

        // brief delay before sending docs
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {}

        for (int i=0; i < 200; i++) {
          try {
            Thread.sleep(rand.nextInt(50)+1);
          } catch (InterruptedException e) {}

          int docToDelete = rand.nextInt(docsSent.get())+1;
          if (!deletedDocs.contains(docToDelete)) {
            delI(String.valueOf(docToDelete));
            deletedDocs.add(docToDelete);
          }
        }
      }
    };

    Thread committerThread = new Thread() {
      public void run() {
        try {
          Thread.sleep(rand.nextInt(200)+1);
        } catch (InterruptedException e) {}

        for (int i=0; i < 20; i++) {
          try {
            cloudClient.commit();
          } catch (Exception e) {}

          try {
            Thread.sleep(rand.nextInt(100)+100);
          } catch (InterruptedException e) {}
        }
      }
    };


    docSenderThread.start();
    reloaderThread.start();
    committerThread.start();
    deleteThread.start();

    docSenderThread.join();
    reloaderThread.join();
    committerThread.join();
    deleteThread.join();

    cloudClient.commit();

    log.info("Total of "+deletedDocs.size()+" docs deleted");

    maxOnLeader = getMaxVersionFromIndex(leader);
    maxOnReplica = getMaxVersionFromIndex(replica);
    assertEquals("leader and replica should have same max version before reload", maxOnLeader, maxOnReplica);

    reloadCollection(leader, testCollectionName);

    maxOnLeader = getMaxVersionFromIndex(leader);
    maxOnReplica = getMaxVersionFromIndex(replica);
    assertEquals("leader and replica should have same max version after reload", maxOnLeader, maxOnReplica);

    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 1000, deletedDocs);

    // try to clean up
    try {
      CollectionAdminRequest.Delete req = new CollectionAdminRequest.Delete()
              .setCollectionName(testCollectionName);
      req.process(cloudClient);
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} after test completed", testCollectionName);
    }
  }

  protected long getMaxVersionFromIndex(Replica replica) throws IOException, SolrServerException {
    return getVersionFromIndex(replica, null);
  }

  protected long getVersionFromIndex(Replica replica, String docId) throws IOException, SolrServerException {
    Long vers = null;
    String queryStr = (docId != null) ? "id:" + docId : "_version_:[0 TO *]";
    SolrQuery query = new SolrQuery(queryStr);
    query.setRows(1);
    query.setFields("id", "_version_");
    query.addSort(new SolrQuery.SortClause("_version_", SolrQuery.ORDER.desc));
    query.setParam("distrib", false);

    try (SolrClient client = new HttpSolrClient(replica.getCoreUrl())) {
      QueryResponse qr = client.query(query);
      SolrDocumentList hits = qr.getResults();
      if (hits.isEmpty())
        fail("No results returned from query: "+query);

      vers = (Long) hits.get(0).getFirstValue("_version_");
    }

    if (vers == null)
      fail("Failed to get version using query " + query + " from " + replica.getCoreUrl());

    return vers.longValue();
  }

  private void createCollectionRetry(String testCollectionName, int numShards, int replicationFactor, int maxShardsPerNode)
      throws SolrServerException, IOException {
    CollectionAdminResponse resp = createCollection(testCollectionName, numShards, replicationFactor, maxShardsPerNode);
    if (resp.getResponse().get("failure") != null) {
      CollectionAdminRequest.Delete req = new CollectionAdminRequest.Delete();
      req.setCollectionName(testCollectionName);
      req.process(cloudClient);

      resp = createCollection(testCollectionName, numShards, replicationFactor, maxShardsPerNode);

      if (resp.getResponse().get("failure") != null) {
        fail("Could not create " + testCollectionName);
      }
    }
  }

  protected void assertDocsExistInAllReplicas(List<Replica> notLeaders,
                                              String testCollectionName,
                                              int firstDocId,
                                              int lastDocId,
                                              Set<Integer> deletedDocs)
      throws Exception
  {
    Replica leader =
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1", 10000);
    HttpSolrClient leaderSolr = getHttpSolrClient(leader);
    List<HttpSolrClient> replicas = new ArrayList<HttpSolrClient>(notLeaders.size());
    for (Replica r : notLeaders)
      replicas.add(getHttpSolrClient(r));

    try {
      for (int d = firstDocId; d <= lastDocId; d++) {

        if (deletedDocs != null && deletedDocs.contains(d))
          continue;

        String docId = String.valueOf(d);
        Long leaderVers = assertDocExists(leaderSolr, testCollectionName, docId, null);
        for (HttpSolrClient replicaSolr : replicas)
          assertDocExists(replicaSolr, testCollectionName, docId, leaderVers);
      }
    } finally {
      if (leaderSolr != null) {
        leaderSolr.close();
      }
      for (HttpSolrClient replicaSolr : replicas) {
        replicaSolr.close();
      }
    }
  }

  protected HttpSolrClient getHttpSolrClient(Replica replica) throws Exception {
    return new HttpSolrClient(replica.getCoreUrl());
  }

  protected void sendDoc(int docId) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(docId));
    doc.addField("a_t", "hello" + docId);
    sendDocsWithRetry(Collections.singletonList(doc), 2, 3, 100);
  }

  /**
   * Query the real-time get handler for a specific doc by ID to verify it
   * exists in the provided server, using distrib=false so it doesn't route to another replica.
   */
  @SuppressWarnings("rawtypes")
  protected Long assertDocExists(HttpSolrClient solr, String coll, String docId, Long expVers) throws Exception {
    QueryRequest qr = new QueryRequest(params("qt", "/get", "id", docId, "distrib", "false", "fl", "id,_version_"));
    NamedList rsp = solr.request(qr);
    SolrDocument doc = (SolrDocument)rsp.get("doc");
    String match = JSONTestUtil.matchObj("/id", doc, new Integer(docId));
    assertTrue("Doc with id=" + docId + " not found in " + solr.getBaseURL() +
        " due to: " + match + "; rsp=" + rsp, match == null);

    Long vers = (Long)doc.getFirstValue("_version_");
    assertNotNull(vers);
    if (expVers != null)
      assertEquals("expected version of doc "+docId+" to be "+expVers, expVers, vers);

    return vers;
  }

  protected boolean reloadCollection(Replica replica, String testCollectionName) throws Exception {
    ZkCoreNodeProps coreProps = new ZkCoreNodeProps(replica);
    String coreName = coreProps.getCoreName();
    boolean reloadedOk = false;
    try (HttpSolrClient client = new HttpSolrClient(coreProps.getBaseUrl())) {
      CoreAdminResponse statusResp = CoreAdminRequest.getStatus(coreName, client);
      long leaderCoreStartTime = statusResp.getStartTime(coreName).getTime();

      Thread.sleep(1000);

      // send reload command for the collection
      log.info("Sending RELOAD command for " + testCollectionName);
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.RELOAD.toString());
      params.set("name", testCollectionName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      log.info("Sending reload command to " + testCollectionName);

      client.request(request);
      Thread.sleep(2000); // reload can take a short while

      // verify reload is done, waiting up to 30 seconds for slow test environments
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      while (System.nanoTime() < timeout) {
        statusResp = CoreAdminRequest.getStatus(coreName, client);
        long startTimeAfterReload = statusResp.getStartTime(coreName).getTime();
        if (startTimeAfterReload > leaderCoreStartTime) {
          reloadedOk = true;
          break;
        }
        // else ... still waiting to see the reloaded core report a later start time
        Thread.sleep(1000);
      }
    }
    return reloadedOk;
  }
}
