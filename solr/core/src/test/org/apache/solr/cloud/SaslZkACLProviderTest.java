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
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.util.Locale;

import javax.security.auth.login.Configuration;

import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.lucene.util.Constants;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.DefaultZkACLProvider;
import org.apache.solr.common.cloud.SaslZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkACLProvider;
import org.apache.solr.util.BadZookeeperThreadsFilter;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@ThreadLeakFilters(defaultFilters = true, filters = {
    BadZookeeperThreadsFilter.class // hdfs currently leaks thread(s)
})
public class SaslZkACLProviderTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Charset DATA_ENCODING = Charset.forName("UTF-8");

  protected Locale savedLocale = null;

  protected ZkTestServer zkServer;

  @BeforeClass
  public static void beforeClass() {
    assumeFalse("FIXME: SOLR-7040: This test fails under IBM J9",
                Constants.JAVA_VENDOR.startsWith("IBM"));
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    System.clearProperty("solrcloud.skip.autorecovery");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    savedLocale = KerberosTestUtil.overrideLocaleIfNotSpportedByMiniKdc();
    log.info("####SETUP_START " + getTestName());
    createTempDir();

    String zkDir = createTempDir() + File.separator
        + "zookeeper/server1/data";
    log.info("ZooKeeper dataDir:" + zkDir);
    zkServer = new SaslZkTestServer(zkDir, createTempDir() + File.separator + "miniKdc");
    zkServer.run();

    System.setProperty("zkHost", zkServer.getZkAddress());

    SolrZkClient zkClient = new SolrZkClientWithACLs(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    try {
      zkClient.makePath("/solr", false, true);
    } finally {
      zkClient.close();
    }
    setupZNodes();

    log.info("####SETUP_END " + getTestName());
  }

  protected void setupZNodes() throws Exception {
    SolrZkClient zkClient = new SolrZkClientWithACLs(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      zkClient.create("/protectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
      zkClient.makePath("/protectedMakePathNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    } finally {
      zkClient.close();
    }

    zkClient = new SolrZkClientNoACLs(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      zkClient.create("/unprotectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
      zkClient.makePath("/unprotectedMakePathNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    } finally {
      zkClient.close();
    }
  }

  @Override
  public void tearDown() throws Exception {
    zkServer.shutdown();
    Locale.setDefault(savedLocale);
    super.tearDown();
  }

  @Test
  public void testSaslZkACLProvider() throws Exception {
    // Test with Sasl enabled
    SolrZkClient zkClient = new SolrZkClientWithACLs(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      VMParamsZkACLAndCredentialsProvidersTest.doTest(zkClient, true, true, true, true, true);
     } finally {
      zkClient.close();
    }

    // Test without Sasl enabled
    setupZNodes();
    System.setProperty("zookeeper.sasl.client", "false");
    zkClient = new SolrZkClientNoACLs(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      VMParamsZkACLAndCredentialsProvidersTest.doTest(zkClient, true, true, false, false, false);
    } finally {
      zkClient.close();
      System.clearProperty("zookeeper.sasl.client");
    }
  }

  /**
   * A SolrZKClient that adds Sasl ACLs
   */
  private static class SolrZkClientWithACLs extends SolrZkClient {

    public SolrZkClientWithACLs(String zkServerAddress, int zkClientTimeout) {
      super(zkServerAddress, zkClientTimeout);
    }

    @Override
    public ZkACLProvider createZkACLProvider() {
      return new SaslZkACLProvider();
    }
  }

  /**
   * A SolrZKClient that doesn't add ACLs
   */
  private static class SolrZkClientNoACLs extends SolrZkClient {

    public SolrZkClientNoACLs(String zkServerAddress, int zkClientTimeout) {
      super(zkServerAddress, zkClientTimeout);
    }

    @Override
    public ZkACLProvider createZkACLProvider() {
      return new DefaultZkACLProvider();
    }
  }

  /**
   * A ZkTestServer with Sasl support
   */
  public static class SaslZkTestServer extends ZkTestServer {
    private String kdcDir;
    private MiniKdc kdc;
    private Configuration conf;

    public SaslZkTestServer(String zkDir, String kdcDir) {
      super(zkDir);
      this.kdcDir = kdcDir;
    }

    public SaslZkTestServer(String zkDir, int port, String kdcDir) {
      super(zkDir, port);
      this.kdcDir = kdcDir;
      conf = Configuration.getConfiguration();
    }

    @Override
    public void run() throws InterruptedException {
      try {
        kdc = KerberosTestUtil.getKdc(new File(kdcDir));
        // Don't require that credentials match the entire principal string, e.g.
        // can match "solr" rather than "solr/host@DOMAIN"
        System.setProperty("zookeeper.kerberos.removeRealmFromPrincipal", "true");
        System.setProperty("zookeeper.kerberos.removeHostFromPrincipal", "true");
        File keytabFile = new File(kdcDir, "keytabs");
        String zkClientPrincipal = "solr";
        String zkServerPrincipal = "zookeeper/127.0.0.1";

        kdc.start();
        // Create ZK client and server principals and load them into the Configuration
        kdc.createPrincipal(keytabFile, zkClientPrincipal, zkServerPrincipal);
        KerberosTestUtil.JaasConfiguration jaas = new KerberosTestUtil.JaasConfiguration(
        zkClientPrincipal, keytabFile, zkServerPrincipal, keytabFile);
        Configuration.setConfiguration(jaas);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
      super.run();
    }

    @Override
    public void shutdown() throws IOException, InterruptedException {
      super.shutdown();
      System.clearProperty("zookeeper.authProvider.1");
      System.clearProperty("zookeeper.kerberos.removeRealmFromPrincipal");
      System.clearProperty("zookeeper.kerberos.removeHostFromPrincipal");
      Configuration.setConfiguration(conf);
      kdc.stop();
    }
  }
}
