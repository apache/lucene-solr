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

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.ShardParams._ROUTE_;

public class TestCloudBackupRestore extends SolrCloudTestCase {

  private static Logger log = LoggerFactory.getLogger(TestCloudBackupRestore.class);

  private static final int NUM_SHARDS = 2;

  @BeforeClass
  public static void createCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  public void test() throws Exception {
    String collectionName = "backuprestore";
    String restoreCollectionName = collectionName + "_restored";
    boolean isImplicit = random().nextBoolean();
    int numReplicas = TestUtil.nextInt(random(), 1, 2);
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, "conf1", NUM_SHARDS, numReplicas);
    create.setMaxShardsPerNode(NUM_SHARDS);
    if (isImplicit) { //implicit router
      create.setRouterName(ImplicitDocRouter.NAME);
      create.setNumShards(null);//erase it
      create.setShards("shard1,shard2"); // however still same number as NUM_SHARDS; we assume this later
      create.setRouterField("shard_s");
    }
//TODO nocommit test shard split & custom doc route?
    create.process(cluster.getSolrClient());
    waitForCollection(collectionName);
    indexDocs(collectionName);
    testBackupAndRestore(collectionName, restoreCollectionName, isImplicit);
  }

  private void indexDocs(String collectionName) throws Exception {
    int numDocs = TestUtil.nextInt(random(), 10, 100);
    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(collectionName);
    for (int i=0; i<numDocs; i++) {
      //We index the shard_s fields for whichever router gets chosen but only use it when implicit router was selected
      if (random().nextBoolean()) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", i);
        doc.addField("shard_s", "shard1");
        client.add(doc);
      } else {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", i);
        doc.addField("shard_s", "shard2");
        client.add(doc);
      }
    }
    client.commit();
  }

  private void testBackupAndRestore(String collectionName, String restoreCollectionName, boolean isImplicit) throws Exception {
    String backupName = "mytestbackup";
    CloudSolrClient client = cluster.getSolrClient();
    long totalDocs = client.query(collectionName, new SolrQuery("*:*")).getResults().getNumFound();
    long shard1Docs = 0, shard2Docs = 0;
    if (isImplicit) {
      shard1Docs = client.query(collectionName, new SolrQuery("*:*").setParam(_ROUTE_, "shard1")).getResults().getNumFound();
      shard2Docs = client.query(collectionName, new SolrQuery("*:*").setParam(_ROUTE_, "shard2")).getResults().getNumFound();
      assertTrue(totalDocs == shard1Docs + shard2Docs);
    }

    String location = createTempDir().toFile().getAbsolutePath();

    log.info("Triggering Backup command");

    CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(collectionName, backupName)
        .setLocation(location);
    NamedList<Object> rsp = cluster.getSolrClient().request(backup);
    assertEquals(0, ((NamedList)rsp.get("responseHeader")).get("status"));

    log.info("Triggering Restore command");

    CollectionAdminRequest.Restore restore = CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
        .setLocation(location);
    rsp = cluster.getSolrClient().request(restore);
    assertEquals(0, ((NamedList)rsp.get("responseHeader")).get("status"));
    waitForCollection(restoreCollectionName);

    //Check the number of results are the same
    long restoredNumDocs = client.query(restoreCollectionName, new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(totalDocs, restoredNumDocs);

    if (isImplicit) {
      long restoredShard1Docs = client.query(restoreCollectionName, new SolrQuery("*:*").setParam(_ROUTE_, "shard1")).getResults().getNumFound();
      long restoredShard2Docs = client.query(restoreCollectionName, new SolrQuery("*:*").setParam(_ROUTE_, "shard2")).getResults().getNumFound();

      assertEquals(shard2Docs, restoredShard2Docs);
      assertEquals(shard1Docs, restoredShard1Docs);
    }

    DocCollection backupCollection = client.getZkStateReader().getClusterState().getCollection(collectionName);
    DocCollection restoreCollection = client.getZkStateReader().getClusterState().getCollection(restoreCollectionName);

    assertEquals(backupCollection.getReplicationFactor(), restoreCollection.getReplicationFactor());

    assertEquals("restore.conf1", cluster.getSolrClient().getZkStateReader().readConfigName(restoreCollectionName));
  }

  public void waitForCollection(String collection) throws Exception {
    AbstractFullDistribZkTestBase.waitForCollection(cluster.getSolrClient().getZkStateReader(), collection, NUM_SHARDS);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(collection, cluster.getSolrClient().getZkStateReader(), log.isDebugEnabled(), true, 30);
    AbstractDistribZkTestBase.assertAllActive(collection, cluster.getSolrClient().getZkStateReader());
  }

}
