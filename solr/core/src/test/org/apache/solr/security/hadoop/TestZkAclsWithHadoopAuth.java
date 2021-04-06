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

import static org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider;
import org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZkAclsWithHadoopAuth extends SolrCloudTestCase {
  protected static final int NUM_SERVERS = 1;
  protected static final int NUM_SHARDS = 1;
  protected static final int REPLICATION_FACTOR = 1;
  private static final String SOLR_PASSWD = "solr";
  private static final String FOO_PASSWD = "foo";
  private static final Id SOLR_ZK_ID = new Id("digest", digest ("solr", SOLR_PASSWD));
  private static final Id FOO_ZK_ID = new Id("digest", digest ("foo", FOO_PASSWD));

  @BeforeClass
  public static void setupClass() throws Exception {
    HdfsTestUtil.checkAssumptions();

    System.setProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        VMParamsAllAndReadonlyDigestZkACLProvider.class.getName());
    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    System.setProperty(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "solr");
    System.setProperty(DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, SOLR_PASSWD);
    System.setProperty(DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME, "foo");
    System.setProperty(DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME, FOO_PASSWD);

    configureCluster(NUM_SERVERS)// nodes
        .withSolrXml(MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML)
        .withSecurityJson(TEST_PATH().resolve("security").resolve("hadoop_simple_auth_with_delegation.json"))
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void tearDownClass() {
    System.clearProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME);
    System.clearProperty(DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME);
    System.clearProperty(DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME);
    System.clearProperty(DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME);
  }

  @Test
  @SuppressWarnings({"try"})
  public void testZkAcls() throws Exception {
    try (ZooKeeper keeper = new ZooKeeper(cluster.getZkServer().getZkAddress(),
        (int) TimeUnit.MINUTES.toMillis(1), arg0 -> {/* Do nothing */})) {
      keeper.addAuthInfo("digest", ("solr:" + SOLR_PASSWD).getBytes(StandardCharsets.UTF_8));

      // Test well known paths.
      checkNonSecurityACLs(keeper, "/solr.xml");
      checkSecurityACLs(keeper, "/security/token");
      checkSecurityACLs(keeper, "/security");

      // Now test all ZK tree.
      String zkHost = cluster.getSolrClient().getZkHost();
      String zkChroot = zkHost.contains("/") ? zkHost.substring(zkHost.indexOf("/")) : null;
      walkZkTree(keeper, zkChroot, "/");
    }
  }

  private void walkZkTree (ZooKeeper keeper, String zkChroot, String path) throws Exception {
    if (isSecurityZNode(zkChroot, path)) {
      checkSecurityACLs(keeper, path);
    } else {
      checkNonSecurityACLs(keeper, path);
    }

    List<String> children = keeper.getChildren(path, false);
    for (String child : children) {
      String subpath = path.endsWith("/") ? path + child : path + "/" + child;
      walkZkTree(keeper, zkChroot, subpath);
    }
  }

  private boolean isSecurityZNode(String zkChroot, String path) {
    String temp = path;
    if (zkChroot != null) {
      temp = path.replace(zkChroot, "");
    }
    return temp.startsWith(SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH);
  }

  private void checkSecurityACLs(ZooKeeper keeper, String path) throws Exception {
    List<ACL> acls = keeper.getACL(path, new Stat());
    String message = String.format(Locale.ROOT, "Path %s ACLs found %s", path, acls);
    assertEquals(message, 1, acls.size());
    assertTrue(message, acls.contains(new ACL(ZooDefs.Perms.ALL, SOLR_ZK_ID)));
  }

  private void checkNonSecurityACLs(ZooKeeper keeper, String path)  throws Exception {
    List<ACL> acls = keeper.getACL(path, new Stat());
    String message = String.format(Locale.ROOT, "Path %s ACLs found %s", path, acls);
    assertEquals(message, 2, acls.size());
    assertTrue(message, acls.contains(new ACL(ZooDefs.Perms.ALL, SOLR_ZK_ID)));
    assertTrue(message, acls.contains(new ACL(ZooDefs.Perms.READ, FOO_ZK_ID)));
  }

  private static String digest (String userName, String passwd) {
    try {
      return DigestAuthenticationProvider.generateDigest(userName+":"+passwd);
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException(ex);
    }
  }
}
