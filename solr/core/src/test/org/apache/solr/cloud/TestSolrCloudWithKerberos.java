package org.apache.solr.cloud;

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

import javax.security.auth.login.Configuration;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.Slow
public class TestSolrCloudWithKerberos extends AbstractFullDistribZkTestBase {

  static final int TIMEOUT = 10000;
  private MiniKdc kdc;

  protected final static List<String> brokenLocales =
      Arrays.asList(
          "th_TH_TH_#u-nu-thai",
          "ja_JP_JP_#u-ca-japanese",
          "hi_IN");

  Configuration originalConfig = Configuration.getConfiguration();
  
  @Override
  public void distribSetUp() throws Exception {
    //SSLTestConfig.setSSLSystemProperties();
    if (brokenLocales.contains(Locale.getDefault().toString())) {
      Locale.setDefault(Locale.US);
    }
    // Use just one jetty
    this.sliceCount = 0;
    this.fixShardCount(1);

    setupMiniKdc();
    //useExternalKdc();
    
    super.distribSetUp();
    try (ZkStateReader zkStateReader = new ZkStateReader(zkServer.getZkAddress(), TIMEOUT, TIMEOUT)) {
      zkStateReader.getZkClient().create(ZkStateReader.SOLR_SECURITY_CONF_PATH,
          "{\"authentication\":{\"class\":\"org.apache.solr.security.KerberosPlugin\"}}".getBytes(Charsets.UTF_8),
          CreateMode.PERSISTENT, true);
    }
  }

  private void setupMiniKdc() throws Exception {
    System.setProperty("solr.jaas.debug", "true");
    String kdcDir = createTempDir()+File.separator+"minikdc";
    kdc = KerberosTestUtil.getKdc(new File(kdcDir));
    File keytabFile = new File(kdcDir, "keytabs");
    String solrServerPrincipal = "HTTP/127.0.0.1";
    String solrClientPrincipal = "solr";
    kdc.start();
    kdc.createPrincipal(keytabFile, solrServerPrincipal, solrClientPrincipal);

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

    Configuration conf = new KerberosTestUtil.JaasConfiguration(solrClientPrincipal, keytabFile, "SolrClient");
    Configuration.setConfiguration(conf);

    String jaasFilePath = kdcDir+File.separator+"jaas-client.conf";
    FileUtils.write(new File(jaasFilePath), jaas);
    System.setProperty("java.security.auth.login.config", jaasFilePath);
    System.setProperty("solr.kerberos.jaas.appname", "SolrClient"); // Get this app name from the jaas file
    System.setProperty("solr.kerberos.cookie.domain", "127.0.0.1");
    System.setProperty("solr.kerberos.principal", solrServerPrincipal);
    System.setProperty("solr.kerberos.keytab", keytabFile.getAbsolutePath());
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
  
  //This method can be used for debugging i.e. to use an external KDC for the test.
  public static void useExternalKdc() throws Exception {

    String jaas = "SolrClient {\n"
        +"  com.sun.security.auth.module.Krb5LoginModule required\n"
        +"  useKeyTab=true\n"
        +"  keyTab=\"/opt/keytabs/solr.keytab\"\n"
        +"  storeKey=true\n"
        + " doNotPrompt=true\n"
        +"  useTicketCache=false\n"
        +"  debug=true\n"
        +"  principal=\"HTTP/127.0.0.1\";\n"
        +"};\n";

    String tmpDir = createTempDir().toString();
    FileUtils.write(new File(tmpDir + File.separator + "jaas.conf"), jaas);
    
    Configuration conf = new KerberosTestUtil.JaasConfiguration("solr", new File("/opt/keytabs/solr.keytab"), "SolrClient");
    Configuration.setConfiguration(conf);

    System.setProperty("java.security.auth.login.config", tmpDir + File.separator + "jaas.conf");
    System.setProperty("solr.kerberos.jaas.appname", "SolrClient");
    System.setProperty("solr.kerberos.cookie.domain", "127.0.0.1");
    System.setProperty("solr.kerberos.principal", "HTTP/127.0.0.1@EXAMPLE.COM");
    System.setProperty("solr.kerberos.keytab", "/opt/keytabs/solr.keytab");
    System.setProperty("authenticationPlugin", "org.apache.solr.security.KerberosPlugin");
    // Extracts 127.0.0.1 from HTTP/127.0.0.1@EXAMPLE.COM
    //System.setProperty("solr.kerberos.name.rules", "RULE:[2:$2@$0](.*EXAMPLE.COM)s/@.*//");
  }
  
  @Test
  public void testKerberizedSolr() throws Exception {
    CloudSolrClient testClient = null;
    try {
      HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
      testClient = createCloudClient("testcollection");

      CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
      create.setCollectionName("testcollection");
      create.setConfigName("conf1");
      create.setNumShards(1);
      create.setReplicationFactor(1);
      create.process(testClient);

      waitForCollection(testClient.getZkStateReader(), "testcollection", 1);
      CollectionAdminRequest.List list = new CollectionAdminRequest.List();

      CollectionAdminResponse response = list.process(testClient);
      assertTrue("Expected to see testcollection but it doesn't exist",
          ((ArrayList) response.getResponse().get("collections")).contains("testcollection"));

      testClient.setDefaultCollection("testcollection");
      indexDoc(testClient, params("commit", "true"), getDoc("id", 1));

      QueryResponse queryResponse = testClient.query(new SolrQuery("*:*"));
      assertEquals("Expected #docs and actual isn't the same", 1, queryResponse.getResults().size());
    } finally {
      if(testClient != null)
        testClient.close();
    }
  }
  
  @Override
  public void distribTearDown() throws Exception {
    System.clearProperty("java.security.auth.login.config");
    System.clearProperty("solr.kerberos.jaas.appname");
    System.clearProperty("solr.cookie.domain");
    System.clearProperty("solr.kerberos.principal");
    System.clearProperty("solr.kerberos.keytab");
    System.clearProperty("solr.jaas.debug");
    System.clearProperty("solr.kerberos.name.rules");
    Configuration.setConfiguration(originalConfig);
    if (kdc != null) {
      kdc.stop();
    }
    //SSLTestConfig.clearSSLSystemProperties();
    super.distribTearDown();
  }
}
