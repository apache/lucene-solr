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

import javax.security.auth.login.Configuration;
import java.io.File;
import java.util.Locale;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.solr.util.BadZookeeperThreadsFilter;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Test 5 nodes Solr cluster with Kerberos plugin enabled.
 * This test is Ignored right now as Mini KDC has a known bug that
 * doesn't allow us to run multiple nodes on the same host.
 * https://issues.apache.org/jira/browse/HADOOP-9893
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadZookeeperThreadsFilter.class // Zookeeper login leaks TGT renewal threads
})

@Ignore
@LuceneTestCase.Slow
@SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class TestMiniSolrCloudClusterKerberos extends TestMiniSolrCloudCluster {

  private final Configuration originalConfig = Configuration.getConfiguration();

  public TestMiniSolrCloudClusterKerberos () {
    NUM_SERVERS = 5;
    NUM_SHARDS = 2;
    REPLICATION_FACTOR = 2;
  }
  
  private MiniKdc kdc;

  private Locale savedLocale; // in case locale is broken and we need to fill in a working locale
  @Rule
  public TestRule solrTestRules = RuleChain
      .outerRule(new SystemPropertiesRestoreRule());

  @ClassRule
  public static TestRule solrClassRules = RuleChain.outerRule(
      new SystemPropertiesRestoreRule()).around(
      new RevertDefaultThreadHandlerRule());

  @Override
  public void setUp() throws Exception {
    savedLocale = KerberosTestUtil.overrideLocaleIfNotSpportedByMiniKdc();
    super.setUp();
    setupMiniKdc();
  }
  
  private void setupMiniKdc() throws Exception {
    String kdcDir = createTempDir()+File.separator+"minikdc";
    kdc = KerberosTestUtil.getKdc(new File(kdcDir));
    File keytabFile = new File(kdcDir, "keytabs");
    String principal = "HTTP/127.0.0.1";
    String zkServerPrincipal = "zookeeper/127.0.0.1";

    kdc.start();
    kdc.createPrincipal(keytabFile, principal, zkServerPrincipal);

    String jaas = "Client {\n"
        + " com.sun.security.auth.module.Krb5LoginModule required\n"
        + " useKeyTab=true\n"
        + " keyTab=\""+keytabFile.getAbsolutePath()+"\"\n"
        + " storeKey=true\n"
        + " useTicketCache=false\n"
        + " doNotPrompt=true\n"
        + " debug=true\n"
        + " principal=\""+principal+"\";\n" 
        + "};\n"
        + "Server {\n"
        + " com.sun.security.auth.module.Krb5LoginModule required\n"
        + " useKeyTab=true\n"
        + " keyTab=\""+keytabFile.getAbsolutePath()+"\"\n"
        + " storeKey=true\n"
        + " doNotPrompt=true\n"
        + " useTicketCache=false\n"
        + " debug=true\n"
        + " principal=\""+zkServerPrincipal+"\";\n" 
        + "};\n";
    
    Configuration conf = new KerberosTestUtil.JaasConfiguration(principal, keytabFile, zkServerPrincipal, keytabFile);
    javax.security.auth.login.Configuration.setConfiguration(conf);
    
    String jaasFilePath = kdcDir+File.separator + "jaas-client.conf";
    FileUtils.write(new File(jaasFilePath), jaas);
    System.setProperty("java.security.auth.login.config", jaasFilePath);
    System.setProperty("solr.kerberos.cookie.domain", "127.0.0.1");
    System.setProperty("solr.kerberos.principal", principal);
    System.setProperty("solr.kerberos.keytab", keytabFile.getAbsolutePath());
    System.setProperty("authenticationPlugin", "org.apache.solr.security.KerberosPlugin");

    // more debugging, if needed
    /*System.setProperty("sun.security.jgss.debug", "true");
    System.setProperty("sun.security.krb5.debug", "true");
    System.setProperty("sun.security.jgss.debug", "true");
    System.setProperty("java.security.debug", "logincontext,policy,scl,gssloginconfig");*/
  }
  
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/HADOOP-9893")
  @Test
  @Override
  public void testCollectionCreateSearchDelete() throws Exception {
    super.testCollectionCreateSearchDelete();
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/HADOOP-9893")
  @Test
  @Override
  public void testErrorsInShutdown() throws Exception {
    super.testErrorsInShutdown();
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/HADOOP-9893")
  @Test
  @Override
  public void testErrorsInStartup() throws Exception {
    super.testErrorsInStartup();
  }
  
  @Override
  public void tearDown() throws Exception {
    System.clearProperty("java.security.auth.login.config");
    System.clearProperty("cookie.domain");
    System.clearProperty("kerberos.principal");
    System.clearProperty("kerberos.keytab");
    System.clearProperty("authenticationPlugin");
    Configuration.setConfiguration(this.originalConfig);
    if (kdc != null) {
      kdc.stop();
    }
    Locale.setDefault(savedLocale);
    super.tearDown();
  }
}
