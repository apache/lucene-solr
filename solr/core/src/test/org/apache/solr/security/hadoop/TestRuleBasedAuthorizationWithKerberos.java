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
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientUtils;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.KerberosTestServices;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@LogLevel("org.apache.solr.security=DEBUG")
public class TestRuleBasedAuthorizationWithKerberos extends SolrCloudTestCase {
    protected static final int NUM_SERVERS = 2;
    protected static final int NUM_SHARDS = 1;
    protected static final int REPLICATION_FACTOR = 1;
    private static KerberosTestServices kerberosTestServices;

    private String collectionName;

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

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        collectionName = getSaferTestName();

        // create collection
        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1",
                NUM_SHARDS, REPLICATION_FACTOR);
        create.process(cluster.getSolrClient());
        cluster.waitForActiveCollection(collectionName, 1, 1);
    }

    @Test
    public void testReadsAltUser() throws Exception {
        String authorizedColl = "public";

        // create collection
        CollectionAdminRequest.createCollection(authorizedColl, "conf1", NUM_SHARDS, REPLICATION_FACTOR)
                              .process(cluster.getSolrClient());
        cluster.waitForActiveCollection(authorizedColl, 1, 1);

        final SolrQuery q = new SolrQuery("*:*");

        for (JettySolrRunner jsr : cluster.getJettySolrRunners()) {
            final String baseUrl = jsr.getBaseUrl().toString();
            try (Http2SolrClient client = new Http2SolrClient.Builder(baseUrl).build()) {
                Krb5HttpClientUtils.setup(client, "solr_alt");
                assertEquals(0, client.query(authorizedColl, q).getStatus());
                BaseHttpSolrClient.RemoteSolrException e = assertThrows(BaseHttpSolrClient.RemoteSolrException.class,
                        () -> client.query(collectionName, q));
                assertEquals(403, e.code());
            }
        }
    }
}
