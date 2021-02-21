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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;

/**
 * Test 5 nodes Solr cluster with Kerberos plugin enabled.
 */
@LuceneTestCase.Slow
@Ignore // MRM TODO: debug
public class TestSolrCloudWithKerberosAlt extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int numShards = 1;
  private static final int numReplicas = 1;
  private static final int maxShardsPerNode = 1;
  private static final int nodeCount = (numShards*numReplicas + (maxShardsPerNode-1))/maxShardsPerNode;
  private static final String configName = "solrCloudCollectionConfig";
  private static final String collectionName = "testkerberoscollection";
  
  private KerberosTestServices kerberosTestServices;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    setupMiniKdc();
    configureCluster(nodeCount).addConfig(configName, SolrTestUtil.configset("cloud-minimal")).configure();
  }

  private void setupMiniKdc() throws Exception {
    System.setProperty("solr.jaas.debug", "true");
    String kdcDir = SolrTestUtil.createTempDir() +File.separator+"minikdc";
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
    log.info("Enable delegation token: {}", enableDt);
    System.setProperty("solr.kerberos.delegation.token.enabled", Boolean.toString(enableDt));
    // Extracts 127.0.0.1 from HTTP/127.0.0.1@EXAMPLE.COM
    System.setProperty("solr.kerberos.name.rules", "RULE:[1:$1@$0](.*EXAMPLE.COM)s/@.*//"
        + "\nRULE:[2:$2@$0](.*EXAMPLE.COM)s/@.*//"
        + "\nDEFAULT"
        );

    // more debugging, if needed
    // System.setProperty("sun.security.jgss.debug", "true");
    // System.setProperty("sun.security.krb5.debug", "true");
    // System.setProperty("sun.security.jgss.debug", "true");
    // System.setProperty("java.security.debug", "logincontext,policy,scl,gssloginconfig");
  }
  
  @Test
  public void testBasics() throws Exception {
    testCollectionCreateSearchDelete();
    // sometimes run a second test e.g. to test collection create-delete-create scenario
    if (random().nextBoolean()) testCollectionCreateSearchDelete();
  }

  private void testCollectionCreateSearchDelete() throws Exception {
    CloudHttp2SolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collectionName, configName, numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .process(client);

    // modify/query collection
    new UpdateRequest().add("id", "1").commit(client, collectionName);
    QueryResponse rsp = client.query(collectionName, new SolrQuery("*:*"));
    assertEquals(1, rsp.getResults().getNumFound());
        
    // delete the collection we created earlier
    CollectionAdminRequest.deleteCollection(collectionName).process(client);
  }

  @Override
  public void tearDown() throws Exception {
    System.clearProperty("solr.jaas.debug");
    System.clearProperty("java.security.auth.login.config");
    System.clearProperty("solr.kerberos.jaas.appname");
    System.clearProperty("solr.kerberos.cookie.domain");
    System.clearProperty("solr.kerberos.principal");
    System.clearProperty("solr.kerberos.keytab");
    System.clearProperty("authenticationPlugin");
    System.clearProperty("solr.kerberos.delegation.token.enabled");
    System.clearProperty("solr.kerberos.name.rules");
    
    // more debugging, if needed
    // System.clearProperty("sun.security.jgss.debug");
    // System.clearProperty("sun.security.krb5.debug");
    // System.clearProperty("sun.security.jgss.debug");
    // System.clearProperty("java.security.debug");

    kerberosTestServices.stop();
    super.tearDown();
  }
}
