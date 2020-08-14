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
package org.apache.solr.security.hadoop;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.KerberosTestServices;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSolrCloudWithHadoopAuthPlugin extends SolrCloudAuthTestCase {
  protected static final int NUM_SERVERS = 1;
  protected static final int NUM_SHARDS = 1;
  protected static final int REPLICATION_FACTOR = 1;
  private static KerberosTestServices kerberosTestServices;

  @BeforeClass
  public static void setupClass() throws Exception {
    HdfsTestUtil.checkAssumptions();

    kerberosTestServices = KerberosUtils.setupMiniKdc(createTempDir());

    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(TEST_PATH().resolve("security").resolve("hadoop_kerberos_config.json"))
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withDefaultClusterProperty("useLegacyReplicaAssignment", "false")
        .configure();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    KerberosUtils.cleanupMiniKdc(kerberosTestServices);
    kerberosTestServices = null;
  }

  @Test
  public void testBasics() throws Exception {
    testCollectionCreateSearchDelete();
    // sometimes run a second test e.g. to test collection create-delete-create scenario
    if (random().nextBoolean()) testCollectionCreateSearchDelete();
  }

  protected void testCollectionCreateSearchDelete() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "testkerberoscollection";

    // create collection
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1",
        NUM_SHARDS, REPLICATION_FACTOR);
    create.process(solrClient);
    // The metrics counter for wrong credentials here really just means  
    assertAuthMetricsMinimums(4, 2, 0, 2, 0, 0);

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    solrClient.add(collectionName, doc);
    solrClient.commit(collectionName);
    assertAuthMetricsMinimums(8, 4, 0, 4, 0, 0);

    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    QueryResponse rsp = solrClient.query(collectionName, query);
    assertEquals(1, rsp.getResults().getNumFound());

    CollectionAdminRequest.Delete deleteReq = CollectionAdminRequest.deleteCollection(collectionName);
    deleteReq.process(solrClient);
    AbstractDistribZkTestBase.waitForCollectionToDisappear(collectionName,
        solrClient.getZkStateReader(), true, 330);
    // cookie was used to avoid re-authentication
    assertAuthMetricsMinimums(11, 7, 0, 4, 0, 0);  }
}
