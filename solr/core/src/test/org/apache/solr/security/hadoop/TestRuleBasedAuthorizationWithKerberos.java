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

import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.KerberosTestServices;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRuleBasedAuthorizationWithKerberos extends SolrCloudTestCase {
    protected static final int NUM_SERVERS = 1;
    protected static final int NUM_SHARDS = 1;
    protected static final int REPLICATION_FACTOR = 1;
    private static KerberosTestServices kerberosTestServices;

    @BeforeClass
    public static void setupClass() throws Exception {
        assumeFalse("Hadoop does not work on Windows", Constants.WINDOWS);

        kerberosTestServices = KerberosUtils.setupMiniKdc(createTempDir());

        configureCluster(NUM_SERVERS)// nodes
                .withSecurityJson(TEST_PATH().resolve("security").resolve("hadoop_kerberos_authz_config.json"))
                .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
                .configure();
    }

    @AfterClass
    public static void tearDownClass() {
        KerberosUtils.cleanupMiniKdc(kerberosTestServices);
        kerberosTestServices = null;
    }

    @Test
    public void testCollectionCreateSearchDelete() throws Exception {
        CloudSolrClient solrClient = cluster.getSolrClient();
        String collectionName = "testkerberoscollection_authz";

        // create collection
        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1",
                NUM_SHARDS, REPLICATION_FACTOR);
        create.process(solrClient);

        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("id", "1");
        solrClient.add(collectionName, doc);
        solrClient.commit(collectionName);

        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        QueryResponse rsp = solrClient.query(collectionName, query);
        assertEquals(1, rsp.getResults().getNumFound());

        CollectionAdminRequest.Delete deleteReq = CollectionAdminRequest.deleteCollection(collectionName);
        deleteReq.process(solrClient);
        AbstractDistribZkTestBase.waitForCollectionToDisappear(collectionName,
                solrClient.getZkStateReader(), true, 330);
    }
}