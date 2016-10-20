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

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.index.TieredMergePolicyFactory;
import org.apache.solr.util.BadZookeeperThreadsFilter;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

/**
 * Test 5 nodes Solr cluster with Kerberos plugin enabled.
 * This test is Ignored right now as Mini KDC has a known bug that
 * doesn't allow us to run multiple nodes on the same host.
 * https://issues.apache.org/jira/browse/HADOOP-9893
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadZookeeperThreadsFilter.class // Zookeeper login leaks TGT renewal threads
})

@LuceneTestCase.Slow
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class TestSolrCloudWithKerberosAlt extends LuceneTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final int NUM_SERVERS;
  protected final int NUM_SHARDS;
  protected final int REPLICATION_FACTOR;

  public TestSolrCloudWithKerberosAlt () {
    NUM_SERVERS = 1;
    NUM_SHARDS = 1;
    REPLICATION_FACTOR = 1;
  }

  private KerberosTestServices kerberosTestServices;

  @Rule
  public TestRule solrTestRules = RuleChain
      .outerRule(new SystemPropertiesRestoreRule());

  @ClassRule
  public static TestRule solrClassRules = RuleChain.outerRule(
      new SystemPropertiesRestoreRule()).around(
      new RevertDefaultThreadHandlerRule());

  @BeforeClass
  public static void betterNotBeJava9() {
    assumeFalse("FIXME: SOLR-8182: This test fails under Java 9", Constants.JRE_IS_MINIMUM_JAVA9);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    setupMiniKdc();
  }

  private void setupMiniKdc() throws Exception {
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
    System.setProperty("solr.kerberos.cookie.domain", "127.0.0.1");
    System.setProperty("solr.kerberos.principal", solrServerPrincipal);
    System.setProperty("solr.kerberos.keytab", keytabFile.getAbsolutePath());
    System.setProperty("authenticationPlugin", "org.apache.solr.security.KerberosPlugin");
    boolean enableDt = random().nextBoolean();
    log.info("Enable delegation token: " + enableDt);
    System.setProperty("solr.kerberos.delegation.token.enabled", new Boolean(enableDt).toString());
    // Extracts 127.0.0.1 from HTTP/127.0.0.1@EXAMPLE.COM
    System.setProperty("solr.kerberos.name.rules", "RULE:[1:$1@$0](.*EXAMPLE.COM)s/@.*//"
        + "\nRULE:[2:$2@$0](.*EXAMPLE.COM)s/@.*//"
        + "\nDEFAULT"
        );

    // more debugging, if needed
    /*System.setProperty("sun.security.jgss.debug", "true");
    System.setProperty("sun.security.krb5.debug", "true");
    System.setProperty("sun.security.jgss.debug", "true");
    System.setProperty("java.security.debug", "logincontext,policy,scl,gssloginconfig");*/
  }
  
  @Test
  public void testBasics() throws Exception {
    testCollectionCreateSearchDelete();
    // sometimes run a second test e.g. to test collection create-delete-create scenario
    if (random().nextBoolean()) testCollectionCreateSearchDelete();
  }

  protected void testCollectionCreateSearchDelete() throws Exception {
    String collectionName = "testkerberoscollection";

    MiniSolrCloudCluster miniCluster
        = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), JettyConfig.builder().setContext("/solr").build());
    CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();
    cloudSolrClient.setDefaultCollection(collectionName);
    
    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(NUM_SERVERS, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      // create collection
      String configName = "solrCloudCollectionConfig";
      miniCluster.uploadConfigSet(SolrTestCaseJ4.TEST_PATH().resolve("collection1/conf"), configName);

      CollectionAdminRequest.Create createRequest = new CollectionAdminRequest.Create();
      createRequest.setCollectionName(collectionName);
      createRequest.setNumShards(NUM_SHARDS);
      createRequest.setReplicationFactor(REPLICATION_FACTOR);
      Properties properties = new Properties();
      properties.put(CoreDescriptor.CORE_CONFIG, "solrconfig-tlog.xml");
      properties.put("solr.tests.maxBufferedDocs", "100000");
      properties.put("solr.tests.ramBufferSizeMB", "100");
      // use non-test classes so RandomizedRunner isn't necessary
      if (random().nextBoolean()) {
        properties.put(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICY, TieredMergePolicy.class.getName());
        properties.put(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "true");
        properties.put(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "false");
      } else {
        properties.put(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY, TieredMergePolicyFactory.class.getName());
        properties.put(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "true");
        properties.put(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "false");
      }
      properties.put("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
      properties.put("solr.directoryFactory", "solr.RAMDirectoryFactory");
      createRequest.setProperties(properties);
      
      createRequest.process(cloudSolrClient);
      
      try (SolrZkClient zkClient = new SolrZkClient
          (miniCluster.getZkServer().getZkAddress(), AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
           ZkStateReader zkStateReader = new ZkStateReader(zkClient)) {
        zkStateReader.createClusterStateWatchersAndUpdate();
        AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);

        // modify/query collection
        
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("id", "1");
        cloudSolrClient.add(doc);
        cloudSolrClient.commit();
        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        QueryResponse rsp = cloudSolrClient.query(query);
        assertEquals(1, rsp.getResults().getNumFound());
        
        // delete the collection we created earlier
        CollectionAdminRequest.Delete deleteRequest = new CollectionAdminRequest.Delete();
        deleteRequest.setCollectionName(collectionName);
        deleteRequest.process(cloudSolrClient);
        
        AbstractDistribZkTestBase.waitForCollectionToDisappear(collectionName, zkStateReader, true, true, 330);
      }
    }
    finally {
      cloudSolrClient.close();
      miniCluster.shutdown();
    }
  }

  @Override
  public void tearDown() throws Exception {
    System.clearProperty("java.security.auth.login.config");
    System.clearProperty("cookie.domain");
    System.clearProperty("kerberos.principal");
    System.clearProperty("kerberos.keytab");
    System.clearProperty("authenticationPlugin");
    System.clearProperty("solr.kerberos.name.rules");
    System.clearProperty("solr.jaas.debug");
    kerberosTestServices.stop();
    super.tearDown();
  }
}
