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

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
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

public class TestSolrCloudWithHadoopAuthPlugin extends SolrCloudTestCase {
  protected static final int NUM_SERVERS = 1;
  protected static final int NUM_SHARDS = 1;
  protected static final int REPLICATION_FACTOR = 1;
  private static KerberosTestServices kerberosTestServices;

  @BeforeClass
  public static void setupClass() throws Exception {
    assumeFalse("Hadoop does not work on Windows", Constants.WINDOWS);
    assumeFalse("FIXME: SOLR-8182: This test fails under Java 9", Constants.JRE_IS_MINIMUM_JAVA9);

    setupMiniKdc();

    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(TEST_PATH().resolve("security").resolve("hadoop_kerberos_config.json"))
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    System.clearProperty("java.security.auth.login.config");
    System.clearProperty("solr.kerberos.principal");
    System.clearProperty("solr.kerberos.keytab");
    System.clearProperty("solr.kerberos.name.rules");
    System.clearProperty("solr.jaas.debug");
    if (kerberosTestServices != null) {
      kerberosTestServices.stop();
    }
    kerberosTestServices = null;
  }

  private static void setupMiniKdc() throws Exception {
    System.setProperty("solr.jaas.debug", "true");
    String kdcDir = createTempDir()+File.separator+"minikdc";
    String solrClientPrincipal = "solr";
    File keytabFile = new File(kdcDir, "keytabs");
    kerberosTestServices = KerberosTestServices.builder()
        .withKdc(new File(kdcDir))
        .withJaasConfiguration(solrClientPrincipal, keytabFile, "SolrClient")
        .build();
    String solrServerPrincipal = "HTTP/127.0.0.1";
    kerberosTestServices.start();
    kerberosTestServices.getKdc().createPrincipal(keytabFile, solrServerPrincipal, solrClientPrincipal);

    String jaas = "SolrClient {\n"
        + " com.sun.security.auth.module.Krb5LoginModule required\n"
        + " useKeyTab=true\n"
        + " keyTab=\"" + keytabFile.getAbsolutePath() + "\"\n"
        + " storeKey=true\n"
        + " useTicketCache=false\n"
        + " doNotPrompt=true\n"
        + " debug=true\n"
        + " principal=\"" + solrClientPrincipal + "\";\n"
        + "};";

    String jaasFilePath = kdcDir+File.separator+"jaas-client.conf";
    FileUtils.write(new File(jaasFilePath), jaas, StandardCharsets.UTF_8);
    System.setProperty("java.security.auth.login.config", jaasFilePath);
    System.setProperty("solr.kerberos.jaas.appname", "SolrClient"); // Get this app name from the jaas file

    System.setProperty("solr.kerberos.principal", solrServerPrincipal);
    System.setProperty("solr.kerberos.keytab", keytabFile.getAbsolutePath());
    // Extracts 127.0.0.1 from HTTP/127.0.0.1@EXAMPLE.COM
    System.setProperty("solr.kerberos.name.rules", "RULE:[1:$1@$0](.*EXAMPLE.COM)s/@.*//"
        + "\nRULE:[2:$2@$0](.*EXAMPLE.COM)s/@.*//"
        + "\nDEFAULT"
        );
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
        solrClient.getZkStateReader(), true, true, 330);
  }

}
