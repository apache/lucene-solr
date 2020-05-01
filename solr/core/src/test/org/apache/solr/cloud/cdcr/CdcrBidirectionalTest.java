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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.CdcrParams;
import org.apache.solr.util.TimeOut;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcrBidirectionalTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-12524")
  public void testBiDir() throws Exception {
    MiniSolrCloudCluster cluster2 = new MiniSolrCloudCluster(1, createTempDir("cdcr-cluster2"), buildJettyConfig("/solr"));
    MiniSolrCloudCluster cluster1 = new MiniSolrCloudCluster(1, createTempDir("cdcr-cluster1"), buildJettyConfig("/solr"));
    try {
      if (log.isInfoEnabled()) {
        log.info("cluster2 zkHost = {}", cluster2.getZkServer().getZkAddress());
      }
      System.setProperty("cdcr.cluster2.zkHost", cluster2.getZkServer().getZkAddress());

      if (log.isInfoEnabled()) {
        log.info("cluster1 zkHost = {}", cluster1.getZkServer().getZkAddress());
      }
      System.setProperty("cdcr.cluster1.zkHost", cluster1.getZkServer().getZkAddress());


      cluster1.uploadConfigSet(configset("cdcr-cluster1"), "cdcr-cluster1");
      CollectionAdminRequest.createCollection("cdcr-cluster1", "cdcr-cluster1", 2, 1)
          .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
          .setMaxShardsPerNode(2)
          .process(cluster1.getSolrClient());
      CloudSolrClient cluster1SolrClient = cluster1.getSolrClient();
      cluster1SolrClient.setDefaultCollection("cdcr-cluster1");

      cluster2.uploadConfigSet(configset("cdcr-cluster2"), "cdcr-cluster2");
      CollectionAdminRequest.createCollection("cdcr-cluster2", "cdcr-cluster2", 2, 1)
          .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
          .setMaxShardsPerNode(2)
          .process(cluster2.getSolrClient());
      CloudSolrClient cluster2SolrClient = cluster2.getSolrClient();
      cluster2SolrClient.setDefaultCollection("cdcr-cluster2");

      UpdateRequest req = null;

      CdcrTestsUtil.cdcrStart(cluster1SolrClient);
      Thread.sleep(2000);

      // ADD operation on cluster 1
      int docs = (TEST_NIGHTLY ? 100 : 10);
      int numDocs_c1 = 0;
      for (int k = 0; k < docs; k++) {
        req = new UpdateRequest();
        for (; numDocs_c1 < (k + 1) * 100; numDocs_c1++) {
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", "cluster1_" + numDocs_c1);
          doc.addField("xyz", numDocs_c1);
          req.add(doc);
        }
        req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        log.info("Adding {} docs with commit=true, numDocs={}", docs, numDocs_c1);
        req.process(cluster1SolrClient);
      }

      QueryResponse response = cluster1SolrClient.query(new SolrQuery("*:*"));
      assertEquals("cluster 1 docs mismatch", numDocs_c1, response.getResults().getNumFound());

      assertEquals("cluster 2 docs mismatch", numDocs_c1, CdcrTestsUtil.waitForClusterToSync(numDocs_c1, cluster2SolrClient));

      CdcrTestsUtil.cdcrStart(cluster2SolrClient); // FULL BI-DIRECTIONAL CDCR FORWARDING ON
      Thread.sleep(2000);

      // ADD operation on cluster 2
      int numDocs_c2 = 0;
      for (int k = 0; k < docs; k++) {
        req = new UpdateRequest();
        for (; numDocs_c2 < (k + 1) * 100; numDocs_c2++) {
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", "cluster2_" + numDocs_c2);
          doc.addField("xyz", numDocs_c2);
          req.add(doc);
        }
        req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        log.info("Adding {} docs with commit=true, numDocs= {}", docs, numDocs_c2);
        req.process(cluster2SolrClient);
      }

      int numDocs = numDocs_c1 + numDocs_c2;

      response = cluster2SolrClient.query(new SolrQuery("*:*"));
      assertEquals("cluster 2 docs mismatch", numDocs, response.getResults().getNumFound());

      assertEquals("cluster 1 docs mismatch", numDocs, CdcrTestsUtil.waitForClusterToSync(numDocs, cluster1SolrClient));

      // logging cdcr clusters queue response
      response = CdcrTestsUtil.getCdcrQueue(cluster1SolrClient);
      if (log.isInfoEnabled()) {
        log.info("Cdcr cluster1 queue response: {}", response.getResponse());
      }
      response = CdcrTestsUtil.getCdcrQueue(cluster2SolrClient);
      if (log.isInfoEnabled()) {
        log.info("Cdcr cluster2 queue response: {}", response.getResponse());
      }

      // lets find and keep the maximum version assigned by cluster1 & cluster2 across all our updates

      long maxVersion_c1 = Math.min((long)CdcrTestsUtil.getFingerPrintMaxVersion(cluster1SolrClient, "shard1", numDocs),
          (long)CdcrTestsUtil.getFingerPrintMaxVersion(cluster1SolrClient, "shard2", numDocs));
      long maxVersion_c2 = Math.min((long)CdcrTestsUtil.getFingerPrintMaxVersion(cluster2SolrClient, "shard1", numDocs),
          (long)CdcrTestsUtil.getFingerPrintMaxVersion(cluster2SolrClient, "shard2", numDocs));

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.ACTION, CdcrParams.CdcrAction.COLLECTIONCHECKPOINT.toString());
      params.set(CommonParams.QT, "/cdcr");
      response = cluster2SolrClient.query(params);
      Long checkpoint_2 = (Long) response.getResponse().get(CdcrParams.CHECKPOINT);
      assertNotNull(checkpoint_2);

      params = new ModifiableSolrParams();
      params.set(CommonParams.ACTION, CdcrParams.CdcrAction.COLLECTIONCHECKPOINT.toString());
      params.set(CommonParams.QT, "/cdcr");
      response = cluster1SolrClient.query(params);
      Long checkpoint_1 = (Long) response.getResponse().get(CdcrParams.CHECKPOINT);
      assertNotNull(checkpoint_1);

      log.info("v1: {}\tv2: {}\tcheckpoint1: {}\tcheckpoint2: {}"
          , maxVersion_c1, maxVersion_c2, checkpoint_1, checkpoint_2);

      assertEquals("COLLECTIONCHECKPOINT from cluster2 should have returned the maximum " +
          "version across all updates made to cluster1", maxVersion_c1, checkpoint_2.longValue());
      assertEquals("COLLECTIONCHECKPOINT from cluster1 should have returned the maximum " +
          "version across all updates made to cluster2", maxVersion_c2, checkpoint_1.longValue());
      assertEquals("max versions of updates in both clusters should be same", maxVersion_c1, maxVersion_c2);

      // DELETE BY QUERY
      String deleteByQuery = "id:cluster1_" +String.valueOf(random().nextInt(numDocs_c1));
      response = cluster1SolrClient.query(new SolrQuery(deleteByQuery));
      assertEquals("should match exactly one doc", 1, response.getResults().getNumFound());
      cluster1SolrClient.deleteByQuery(deleteByQuery);
      cluster1SolrClient.commit();
      numDocs--;
      numDocs_c1--;

      response = cluster1SolrClient.query(new SolrQuery("*:*"));
      assertEquals("cluster 1 docs mismatch", numDocs, response.getResults().getNumFound());
      assertEquals("cluster 2 docs mismatch", numDocs, CdcrTestsUtil.waitForClusterToSync(numDocs, cluster2SolrClient));

      // DELETE BY ID
      SolrInputDocument doc;
      String delete_id_query = "cluster2_" + random().nextInt(numDocs_c2);
      cluster2SolrClient.deleteById(delete_id_query);
      cluster2SolrClient.commit();
      numDocs--;
      numDocs_c2--;
      response = cluster2SolrClient.query(new SolrQuery("*:*"));
      assertEquals("cluster 2 docs mismatch", numDocs, response.getResults().getNumFound());
      assertEquals("cluster 1 docs mismatch", numDocs, CdcrTestsUtil.waitForClusterToSync(numDocs, cluster1SolrClient));

      // ATOMIC UPDATES
      req = new UpdateRequest();
      doc = new SolrInputDocument();
      String atomicFieldName = "abc";
      ImmutableMap.of("", "");
      String atomicUpdateId = "cluster2_" + random().nextInt(numDocs_c2);
      doc.addField("id", atomicUpdateId);
      doc.addField("xyz", ImmutableMap.of("delete", ""));
      doc.addField(atomicFieldName, ImmutableMap.of("set", "ABC"));
      req.add(doc);
      req.process(cluster2SolrClient);
      cluster2SolrClient.commit();

      String atomicQuery = "id:" + atomicUpdateId;
      response = cluster2SolrClient.query(new SolrQuery(atomicQuery));
      assertEquals("cluster 2 wrong doc", "ABC", response.getResults().get(0).get(atomicFieldName));
      assertEquals("cluster 1 wrong doc", "ABC", getDocFieldValue(cluster1SolrClient, atomicQuery, "ABC", atomicFieldName ));


      // logging cdcr clusters queue response
      response = CdcrTestsUtil.getCdcrQueue(cluster1SolrClient);
      if (log.isInfoEnabled()) {
        log.info("Cdcr cluster1 queue response at end of testcase: {}", response.getResponse());
      }
      response = CdcrTestsUtil.getCdcrQueue(cluster2SolrClient);
      if (log.isInfoEnabled()) {
        log.info("Cdcr cluster2 queue response at end of testcase: {}", response.getResponse());
      }

      CdcrTestsUtil.cdcrStop(cluster1SolrClient);
      CdcrTestsUtil.cdcrStop(cluster2SolrClient);
    } finally {
      if (cluster1 != null) {
        cluster1.shutdown();
      }
      if (cluster2 != null) {
        cluster2.shutdown();
      }
    }
  }

  private String getDocFieldValue(CloudSolrClient clusterSolrClient, String query, String match, String field) throws Exception {
    TimeOut waitTimeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!waitTimeOut.hasTimedOut()) {
      clusterSolrClient.commit();
      QueryResponse response = clusterSolrClient.query(new SolrQuery(query));
      if (response.getResults().size() > 0 && match.equals(response.getResults().get(0).get(field))) {
        return (String) response.getResults().get(0).get(field);
      }
      Thread.sleep(1000);
    }
    return null;
  }
}
