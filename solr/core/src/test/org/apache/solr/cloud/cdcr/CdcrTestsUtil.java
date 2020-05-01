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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.CdcrParams;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcrTestsUtil extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static void cdcrRestart(CloudSolrClient client) throws SolrServerException, IOException {
    cdcrStop(client);
    cdcrStart(client);
  }

  protected static void cdcrStart(CloudSolrClient client) throws SolrServerException, IOException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.START);
    assertEquals("started", ((NamedList) response.getResponse().get("status")).get("process"));
  }

  protected static void cdcrStop(CloudSolrClient client) throws SolrServerException, IOException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.STOP);
    assertEquals("stopped", ((NamedList) response.getResponse().get("status")).get("process"));
  }

  protected static void cdcrEnableBuffer(CloudSolrClient client) throws IOException, SolrServerException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.ENABLEBUFFER);
    assertEquals("enabled", ((NamedList) response.getResponse().get("status")).get("buffer"));
  }

  protected static void cdcrDisableBuffer(CloudSolrClient client) throws IOException, SolrServerException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.DISABLEBUFFER);
    assertEquals("disabled", ((NamedList) response.getResponse().get("status")).get("buffer"));
  }

  protected static QueryResponse invokeCdcrAction(CloudSolrClient client, CdcrParams.CdcrAction action) throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/cdcr");
    params.set(CommonParams.ACTION, action.toLower());
    return client.query(params);
  }

  protected static QueryResponse getCdcrQueue(CloudSolrClient client) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/cdcr");
    params.set(CommonParams.ACTION, CdcrParams.QUEUES);
    return client.query(params);
  }

  protected static Object getFingerPrintMaxVersion(CloudSolrClient client, String shardNames, int numDocs) throws SolrServerException, IOException, InterruptedException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/get");
    params.set("fingerprint", true);
    params.set("shards", shardNames);
    params.set("getVersions", numDocs);

    QueryResponse response = null;
    long start = System.nanoTime();
    while (System.nanoTime() - start <= TimeUnit.NANOSECONDS.convert(20, TimeUnit.SECONDS)) {
      response = client.query(params);
      if (response.getResponse() != null && response.getResponse().get("fingerprint") != null) {
        return (long) ((LinkedHashMap) response.getResponse().get("fingerprint")).get("maxVersionEncountered");
      }
      Thread.sleep(200);
    }
    log.error("maxVersionEncountered not found for client : {} in 20 attempts", client);
    return null;
  }

  protected static long waitForClusterToSync(long numDocs, CloudSolrClient clusterSolrClient) throws Exception {
    return waitForClusterToSync((int) numDocs, clusterSolrClient, "*:*");
  }

  protected static long waitForClusterToSync(int numDocs, CloudSolrClient clusterSolrClient) throws Exception {
    return waitForClusterToSync(numDocs, clusterSolrClient, "*:*");
  }

  protected static long waitForClusterToSync(int numDocs, CloudSolrClient clusterSolrClient, String query) throws Exception {
    long start = System.nanoTime();
    QueryResponse response = null;
    while (System.nanoTime() - start <= TimeUnit.NANOSECONDS.convert(120, TimeUnit.SECONDS)) {
      clusterSolrClient.commit();
      response = clusterSolrClient.query(new SolrQuery(query));
      if (response.getResults().getNumFound() == numDocs) {
        break;
      }
      Thread.sleep(1000);
    }
    return response != null ? response.getResults().getNumFound() : null;
  }

  protected static boolean assertShardInSync(String collection, String shard, CloudSolrClient client) throws IOException, SolrServerException {
    TimeOut waitTimeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    DocCollection docCollection = client.getZkStateReader().getClusterState().getCollection(collection);
    Slice correctSlice = null;
    for (Slice slice : docCollection.getSlices()) {
      if (shard.equals(slice.getName())) {
        correctSlice = slice;
        break;
      }
    }
    assertNotNull(correctSlice);

    long leaderDocCount;
    try (HttpSolrClient leaderClient = new HttpSolrClient.Builder(correctSlice.getLeader().getCoreUrl()).withHttpClient(client.getHttpClient()).build()) {
      leaderDocCount = leaderClient.query(new SolrQuery("*:*").setParam("distrib", "false")).getResults().getNumFound();
    }

    while (!waitTimeOut.hasTimedOut()) {
      int replicasInSync = 0;
      for (Replica replica : correctSlice.getReplicas()) {
        try (HttpSolrClient leaderClient = new HttpSolrClient.Builder(replica.getCoreUrl()).withHttpClient(client.getHttpClient()).build()) {
          long replicaDocCount = leaderClient.query(new SolrQuery("*:*").setParam("distrib", "false")).getResults().getNumFound();
          if (replicaDocCount == leaderDocCount) replicasInSync++;
        }
      }
      if (replicasInSync == correctSlice.getReplicas().size()) {
        return true;
      }
    }
    return false;
  }

  public static void indexRandomDocs(Integer start, Integer count, CloudSolrClient solrClient) throws Exception {
    // ADD operation on cluster 1
    int docs = 0;
    if (count == 0) {
      docs = (TEST_NIGHTLY ? 100 : 10);
    } else {
      docs = count;
    }
    for (int k = start; k < docs; k++) {
      UpdateRequest req = new UpdateRequest();
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", k);
      req.add(doc);

      req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
      req.process(solrClient);
    }
  }

  public static void indexRandomDocs(Integer count, CloudSolrClient solrClient) throws Exception {
    indexRandomDocs(0, count, solrClient);
  }

  public static void index(MiniSolrCloudCluster cluster, String collection, SolrInputDocument doc, boolean doCommit) throws IOException, SolrServerException {
    CloudSolrClient client = createCloudClient(cluster, collection);
    try {
      client.add(doc);
      if (doCommit) {
        client.commit(true, true);
      } else {
        client.commit(true, false);
      }
    } finally {
      client.close();
    }
  }

  public static void index(MiniSolrCloudCluster cluster, String collection, SolrInputDocument doc) throws IOException, SolrServerException {
    index(cluster, collection, doc, false);
  }

  public static CloudSolrClient createCloudClient(MiniSolrCloudCluster cluster, String defaultCollection) {
    CloudSolrClient server = getCloudSolrClient(cluster.getZkServer().getZkAddress(), random().nextBoolean());
    if (defaultCollection != null) server.setDefaultCollection(defaultCollection);
    return server;
  }


  public static void restartClusterNode(MiniSolrCloudCluster cluster, String collection, int index) throws Exception {
    System.setProperty("collection", collection);
    restartNode(cluster.getJettySolrRunner(index));
    System.clearProperty("collection");
  }

  public static void restartClusterNodes(MiniSolrCloudCluster cluster, String collection) throws Exception {
    System.setProperty("collection", collection);
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      restartNode(jetty);
    }
    System.clearProperty("collection");
  }

  public static void restartNode(JettySolrRunner jetty) throws Exception {
    jetty.stop();
    jetty.start();
    Thread.sleep(10000);
  }

  public static int numberOfFiles(String dir) {
    File file = new File(dir);
    if (!file.isDirectory()) {
      assertTrue("Path to tlog " + dir + " does not exists or it's not a directory.", false);
    }
    if (log.isDebugEnabled()) {
      log.debug("Update log dir {} contains: {}", dir, file.listFiles());
    }
    return file.listFiles().length;
  }

  public static int getNumberOfTlogFilesOnReplicas(MiniSolrCloudCluster cluster) throws Exception {
    int count = 0;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      for (SolrCore core : jetty.getCoreContainer().getCores()) {
        count += numberOfFiles(core.getUlogDir() + "/tlog");
      }
    }
    return count;
  }

  public static String getNonLeaderNode(MiniSolrCloudCluster cluster, String collection) throws Exception {
    String leaderNode = getLeaderNode(cluster, collection);
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (!jetty.getNodeName().equals(leaderNode)) {
        return jetty.getNodeName();
      }
    }
    return cluster.getJettySolrRunners().get(0).getNodeName();
  }

  public static String getLeaderNode(MiniSolrCloudCluster cluster, String collection) throws Exception {
    for (Replica replica : cluster.getSolrClient().getClusterStateProvider().getCollection(collection).getReplicas()) {
      if (cluster.getSolrClient().getClusterStateProvider().getCollection(collection).getLeader("shard1") == replica) {
        return replica.getNodeName();
      }
    }
    return "";
  }

}