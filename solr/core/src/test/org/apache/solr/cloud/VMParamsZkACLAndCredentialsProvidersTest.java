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

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider;
import org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VMParamsZkACLAndCredentialsProvidersTest extends SolrTestCaseJ4 {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Charset DATA_ENCODING = Charset.forName("UTF-8");
  
  protected ZkTestServer zkServer;
  
  protected Path zkDir;
  
  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    System.clearProperty("solrcloud.skip.autorecovery");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    log.info("####SETUP_START " + getTestName());
    createTempDir();
    
    zkDir = createTempDir().resolve("zookeeper/server1/data");
    log.info("ZooKeeper dataDir:" + zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run(false);
    
    System.setProperty("zkHost", zkServer.getZkAddress());
    
    setSecuritySystemProperties();

    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(),
        AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null, null, null);
    zkClient.makePath("/solr", false, true);
    zkClient.close();

    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    zkClient.create("/protectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.makePath("/protectedMakePathNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);

    zkClient.create(SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH, "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.close();
    
    clearSecuritySystemProperties();

    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    // Currently no credentials on ZK connection, because those same VM-params are used for adding ACLs, and here we want
    // no (or completely open) ACLs added. Therefore hack your way into being authorized for creating anyway
    zkClient.getSolrZooKeeper().addAuthInfo("digest", ("connectAndAllACLUsername:connectAndAllACLPassword")
        .getBytes(StandardCharsets.UTF_8));
    zkClient.create("/unprotectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.makePath("/unprotectedMakePathNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.close();

    log.info("####SETUP_END " + getTestName());
  }
  
  @Override
  public void tearDown() throws Exception {
    zkServer.shutdown();
    
    clearSecuritySystemProperties();
    
    super.tearDown();
  }
  
  @Test
  public void testNoCredentials() throws Exception {
    useNoCredentials();
    
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      doTest(zkClient,
          false, false, false, false, false,
          false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void testWrongCredentials() throws Exception {
    useWrongCredentials();
    
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      doTest(zkClient,
          false, false, false, false, false,
          false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void testAllCredentials() throws Exception {
    useAllCredentials();

    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      doTest(zkClient,
          true, true, true, true, true,
          true, true, true, true, true);
    } finally {
      zkClient.close();
    }
  }
  
  @Test
  public void testReadonlyCredentials() throws Exception {
    useReadonlyCredentials();

    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      doTest(zkClient,
          true, true, false, false, false,
          false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }
    
  protected static void doTest(
      SolrZkClient zkClient,
      boolean getData, boolean list, boolean create, boolean setData, boolean delete,
      boolean secureGet, boolean secureList, boolean secureCreate, boolean secureSet, boolean secureDelete) throws Exception {
    doTest(zkClient, "/protectedCreateNode", getData, list, create, setData, delete);
    doTest(zkClient, "/protectedMakePathNode", getData, list, create, setData, delete);
    doTest(zkClient, "/unprotectedCreateNode", true, true, true, true, delete);
    doTest(zkClient, "/unprotectedMakePathNode", true, true, true, true, delete);
    doTest(zkClient, SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH, secureGet, secureList, secureCreate, secureSet, secureDelete);
  }
  
  protected static void doTest(SolrZkClient zkClient, String path, boolean getData, boolean list, boolean create, boolean setData, boolean delete) throws Exception {
    try {
      zkClient.getData(path, null, null, false);
      if (!getData) fail("NoAuthException expected ");
    } catch (NoAuthException nae) {
      if (getData) fail("No NoAuthException expected");
      // expected
    }
    
    try {
      zkClient.getChildren(path, null, false);
      if (!list) fail("NoAuthException expected ");
    } catch (NoAuthException nae) {
      if (list) fail("No NoAuthException expected");
      // expected
    }
    
    try {
      zkClient.create(path + "/subnode", null, CreateMode.PERSISTENT, false);
      if (!create) fail("NoAuthException expected ");
      else {
        zkClient.delete(path + "/subnode", -1, false);
      }
    } catch (NoAuthException nae) {
      if (create) {
        nae.printStackTrace();
        fail("No NoAuthException expected");
      }
      // expected
    }
    
    try {
      zkClient.makePath(path + "/subnode/subsubnode", false);
      if (!create) fail("NoAuthException expected ");
      else {
        zkClient.delete(path + "/subnode/subsubnode", -1, false);
        zkClient.delete(path + "/subnode", -1, false);
      }
    } catch (NoAuthException nae) {
      if (create) fail("No NoAuthException expected");
      // expected
    }
    
    try {
      zkClient.setData(path, (byte[])null, false);
      if (!setData) fail("NoAuthException expected ");
    } catch (NoAuthException nae) {
      if (setData) fail("No NoAuthException expected");
      // expected
    }

    try {
      // Actually about the ACLs on /solr, but that is protected
      zkClient.delete(path, -1, false);
      if (!delete) fail("NoAuthException expected ");
    } catch (NoAuthException nae) {
      if (delete) fail("No NoAuthException expected");
      // expected
    }

  }
  
  private void useNoCredentials() {
    clearSecuritySystemProperties();
  }
  
  private void useWrongCredentials() {
    clearSecuritySystemProperties();
    
    System.setProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    System.setProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "connectAndAllACLUsername");
    System.setProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "connectAndAllACLPasswordWrong");
  }
  
  private void useAllCredentials() {
    clearSecuritySystemProperties();
    
    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    System.setProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "connectAndAllACLUsername");
    System.setProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "connectAndAllACLPassword");
  }
  
  private void useReadonlyCredentials() {
    clearSecuritySystemProperties();

    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    System.setProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "readonlyACLUsername");
    System.setProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "readonlyACLPassword");
  }
  
  private void setSecuritySystemProperties() {
    System.setProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsAllAndReadonlyDigestZkACLProvider.class.getName());
    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    System.setProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "connectAndAllACLUsername");
    System.setProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "connectAndAllACLPassword");
    System.setProperty(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME, "readonlyACLUsername");
    System.setProperty(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME, "readonlyACLPassword");
  }
  
  private void clearSecuritySystemProperties() {
    System.clearProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME);
    System.clearProperty(VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME);
    System.clearProperty(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME);
    System.clearProperty(VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME);
  }
  
}
