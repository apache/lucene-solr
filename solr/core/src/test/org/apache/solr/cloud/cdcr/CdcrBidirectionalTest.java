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
import java.util.LinkedHashMap;
import java.util.concurrent.ThreadLocalRandom;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableMap;
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
import org.apache.solr.handler.CdcrParams;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcrBidirectionalTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testBiDir() throws Exception {
    MiniSolrCloudCluster cluster2 = new MiniSolrCloudCluster(1, createTempDir("cdcr-cluster2"), buildJettyConfig("/solr"));
    cluster2.waitForAllNodes(30);
    MiniSolrCloudCluster cluster1 = new MiniSolrCloudCluster(1, createTempDir("cdcr-cluster1"), buildJettyConfig("/solr"));
    cluster1.waitForAllNodes(30);
    try {
      log.info("cluster2 zkHost = " + cluster2.getZkServer().getZkAddress());
      System.setProperty("cdcr.cluster2.zkHost", cluster2.getZkServer().getZkAddress());

      log.info("cluster1 zkHost = " + cluster1.getZkServer().getZkAddress());
      System.setProperty("cdcr.cluster1.zkHost", cluster1.getZkServer().getZkAddress());


      cluster1.uploadConfigSet(configset("cdcr-cluster1"), "cdcr-cluster1");
      CollectionAdminRequest.createCollection("cdcr-cluster1", "cdcr-cluster1", 1, 1)
          .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
          .process(cluster1.getSolrClient());
      CloudSolrClient cluster1SolrClient = cluster1.getSolrClient();
      cluster1SolrClient.setDefaultCollection("cdcr-cluster1");

      cluster2.uploadConfigSet(configset("cdcr-cluster2"), "cdcr-cluster2");
      CollectionAdminRequest.createCollection("cdcr-cluster2", "cdcr-cluster2", 1, 1)
          .process(cluster2.getSolrClient());
      CloudSolrClient cluster2SolrClient = cluster2.getSolrClient();
      cluster2SolrClient.setDefaultCollection("cdcr-cluster2");

      UpdateRequest req;

      CdcrTestsUtil.cdcrStart(cluster1SolrClient);

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
        log.info("Adding " + docs + " docs with commit=true, numDocs=" + numDocs_c1);
        req.process(cluster1SolrClient);
      }

      QueryResponse response = cluster1SolrClient.query(new SolrQuery("*:*"));
      assertEquals("cluster 1 docs mismatch", numDocs_c1, response.getResults().getNumFound());

      assertEquals("cluster 2 docs mismatch", numDocs_c1, CdcrTestsUtil.waitForClusterToSync(numDocs_c1, cluster2SolrClient));

      CdcrTestsUtil.cdcrStart(cluster2SolrClient); // FULL BI-DIRECTIONAL CDCR FORWARDING ON

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
        log.info("Adding " + docs + " docs with commit=true, numDocs=" + numDocs_c2);
        req.process(cluster2SolrClient);
      }

      int numDocs = numDocs_c1 + numDocs_c2;

      response = cluster2SolrClient.query(new SolrQuery("*:*"));
      assertEquals("cluster 2 docs mismatch", numDocs, response.getResults().getNumFound());

      assertEquals("cluster 1 docs mismatch", numDocs, CdcrTestsUtil.waitForClusterToSync(numDocs, cluster1SolrClient));

      response = CdcrTestsUtil.getCdcrQueue(cluster1SolrClient);
      log.info("Cdcr cluster1 queue response: " + response.getResponse());
      response = CdcrTestsUtil.getCdcrQueue(cluster2SolrClient);
      log.info("Cdcr cluster2 queue response: " + response.getResponse());

      // lets find and keep the maximum version assigned by cluster1 & cluster2 across all our updates
      long maxVersion_c1;
      long maxVersion_c2;

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.QT, "/get");
      params.set("getVersions", numDocs);
      params.set("fingerprint", true);
      response = cluster1SolrClient.query(params);
      maxVersion_c1 = (long)(((LinkedHashMap)response.getResponse().get("fingerprint")).get("maxVersionEncountered"));
      response = cluster2SolrClient.query(params);
      maxVersion_c2 = (long)(((LinkedHashMap)response.getResponse().get("fingerprint")).get("maxVersionEncountered"));

      params = new ModifiableSolrParams();
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

      assertEquals("COLLECTIONCHECKPOINT from cluster2 should have returned the maximum " +
          "version across all updates made to cluster1", maxVersion_c1, checkpoint_2.longValue());
      assertEquals("COLLECTIONCHECKPOINT from cluster1 should have returned the maximum " +
          "version across all updates made to cluster2", maxVersion_c2, checkpoint_1.longValue());
      assertEquals("max versions of updates in both clusters should be same", maxVersion_c1, maxVersion_c2);

      // DELETE BY QUERY
      String randomQuery = String.valueOf(ThreadLocalRandom.current().nextInt(docs * 100));
      response = cluster1SolrClient.query(new SolrQuery(randomQuery));
      assertEquals("cluster 1 docs mismatch", 2, response.getResults().getNumFound());
      cluster1SolrClient.deleteByQuery(randomQuery);

      req = new UpdateRequest();
      req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
      req.process(cluster1SolrClient);
      response = cluster1SolrClient.query(new SolrQuery(randomQuery));
      assertEquals("cluster 1 docs mismatch", 0, response.getResults().getNumFound());
      assertEquals("cluster 2 docs mismatch", 0, CdcrTestsUtil.waitForClusterToSync((int)response.getResults().getNumFound(), cluster2SolrClient, randomQuery));

      // ADD the deleted query-doc again.
      req = new UpdateRequest();
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "cluster2_" + (docs * 100 + 1));
      doc.addField("xyz", randomQuery);
      req.add(doc);
      req.process(cluster2SolrClient);
      req = new UpdateRequest();
      req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
      req.process(cluster2SolrClient);
      response = cluster2SolrClient.query(new SolrQuery(randomQuery));
      assertEquals("cluster 2 docs mismatch", 1, response.getResults().getNumFound());
      assertEquals("cluster 1 docs mismatch", 1, CdcrTestsUtil.waitForClusterToSync((int)response.getResults().getNumFound(), cluster1SolrClient, randomQuery));

      // DELETE BY ID
      String delete_id_query = "cluster2_" + (docs * 100 + 1);
      cluster1SolrClient.deleteById(delete_id_query);
      req.process(cluster1SolrClient);
      response = cluster1SolrClient.query(new SolrQuery(delete_id_query));
      assertEquals("cluster 1 docs mismatch", 0, response.getResults().getNumFound());
      assertEquals("cluster 2 docs mismatch", 0, CdcrTestsUtil.waitForClusterToSync((int)response.getResults().getNumFound(), cluster2SolrClient, delete_id_query));

      // ATOMIC UPDATES
      req = new UpdateRequest();
      doc = new SolrInputDocument();
      ImmutableMap.of("", "");
      doc.addField("id", "cluster2_" + (docs * 100 + 1));
      doc.addField("xyz", ImmutableMap.of("delete", ""));
      doc.addField("abc", ImmutableMap.of("set", "ABC"));
      req.add(doc);
      req.process(cluster2SolrClient);
      req = new UpdateRequest();
      req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
      req.process(cluster2SolrClient);

      String atomic_query_1 = "xyz:" + randomQuery + " AND id:cluster2_" + (docs * 100 + 1);
      String atomic_query_2 = "abc:ABC AND id:cluster2_" + (docs * 100 + 1);

      response = cluster2SolrClient.query(new SolrQuery(atomic_query_1));
      assertEquals("cluster 2 docs mismatch", 0, response.getResults().getNumFound());
      response = cluster2SolrClient.query(new SolrQuery(atomic_query_2));
      assertEquals("cluster 2 docs mismatch", 1, response.getResults().getNumFound());
      assertEquals("cluster 1 docs mismatch", 0, CdcrTestsUtil.waitForClusterToSync((int)response.getResults().getNumFound(), cluster1SolrClient, atomic_query_1));
      assertEquals("cluster 1 docs mismatch", 1, CdcrTestsUtil.waitForClusterToSync((int)response.getResults().getNumFound(), cluster1SolrClient, atomic_query_2));

      response = CdcrTestsUtil.getCdcrQueue(cluster1SolrClient);
      log.info("Cdcr cluster1 queue response at end of testcase: " + response.getResponse());
      response = CdcrTestsUtil.getCdcrQueue(cluster2SolrClient);
      log.info("Cdcr cluster2 queue response at end of testcase: " + response.getResponse());

      CdcrTestsUtil.cdcrStop(cluster1SolrClient);
      CdcrTestsUtil.cdcrStop(cluster2SolrClient);

      cluster1SolrClient.close();
      cluster2SolrClient.close();

    } finally {
      if (cluster1 != null) {
        cluster1.shutdown();
      }
      if (cluster2 != null) {
        cluster2.shutdown();
      }
    }
  }

}
