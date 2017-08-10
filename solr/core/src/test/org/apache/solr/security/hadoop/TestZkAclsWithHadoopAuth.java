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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.Constants;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.ZkCredentialsProvider;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestZkAclsWithHadoopAuth extends SolrCloudTestCase {
  protected static final int NUM_SERVERS = 1;
  protected static final int NUM_SHARDS = 1;
  protected static final int REPLICATION_FACTOR = 1;

  @BeforeClass
  public static void setupClass() throws Exception {
    assumeFalse("Hadoop does not work on Windows", Constants.WINDOWS);
    assumeFalse("FIXME: SOLR-8182: This test fails under Java 9", Constants.JRE_IS_MINIMUM_JAVA9);

    System.setProperty("zookeeper.authProvider.1", DummyZKAuthProvider.class.getName());
    System.setProperty("zkCredentialsProvider", DummyZkCredentialsProvider.class.getName());
    System.setProperty("zkACLProvider", DummyZkAclProvider.class.getName());

    ProviderRegistry.initialize();

    configureCluster(NUM_SERVERS)// nodes
        .withSolrXml(MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML)
        .withSecurityJson(TEST_PATH().resolve("security").resolve("hadoop_simple_auth_with_delegation.json"))
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void tearDownClass() {
    System.clearProperty("zookeeper.authProvider.1");
    System.clearProperty("zkCredentialsProvider");
    System.clearProperty("zkACLProvider");
  }

  @Test
  public void testZkAcls() throws Exception {
    ZooKeeper keeper = null;
    try {
      keeper = new ZooKeeper(cluster.getZkServer().getZkAddress(), (int) TimeUnit.MINUTES.toMillis(1), new Watcher() {
        @Override
        public void process(WatchedEvent arg0) {
          // Do nothing
        }
      });

      keeper.addAuthInfo("dummyauth", "solr".getBytes(StandardCharsets.UTF_8));

      // Test well known paths.
      checkNonSecurityACLs(keeper, "/solr.xml");
      checkSecurityACLs(keeper, "/security/token");
      checkSecurityACLs(keeper, "/security");

      // Now test all ZK tree.
      String zkHost = cluster.getSolrClient().getZkHost();
      String zkChroot = zkHost.contains("/")? zkHost.substring(zkHost.indexOf("/")): null;
      walkZkTree(keeper, zkChroot, "/");

    } finally {
      if (keeper != null) {
        keeper.close();
      }
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
    return !ZkStateReader.SOLR_SECURITY_CONF_PATH.equals(path) &&
           temp.startsWith(SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH);
  }

  private void checkSecurityACLs(ZooKeeper keeper, String path) throws Exception {
    List<ACL> acls = keeper.getACL(path, new Stat());
    String message = String.format(Locale.ROOT, "Path %s ACLs found %s", path, acls);
    assertEquals(message, 1, acls.size());
    assertTrue(message, acls.contains(new ACL(ZooDefs.Perms.ALL, new Id("dummyauth", "solr"))));
  }

  private void checkNonSecurityACLs(ZooKeeper keeper, String path)  throws Exception {
    List<ACL> acls = keeper.getACL(path, new Stat());
    String message = String.format(Locale.ROOT, "Path %s ACLs found %s", path, acls);
    assertEquals(message, 2, acls.size());
    assertTrue(message, acls.contains(new ACL(ZooDefs.Perms.ALL, new Id("dummyauth", "solr"))));
    assertTrue(message, acls.contains(new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"))));
  }

  public static class DummyZKAuthProvider implements AuthenticationProvider {
    public static final String zkSuperUser = "zookeeper";
    public static final Collection<String> validUsers = Arrays.asList(zkSuperUser, "solr", "foo");

    @Override
    public String getScheme() {
      return "dummyauth";
    }

    @Override
    public Code handleAuthentication(ServerCnxn arg0, byte[] arg1) {
      String userName = new String(arg1, StandardCharsets.UTF_8);

      if (validUsers.contains(userName)) {
        if (zkSuperUser.equals(userName)) {
          arg0.addAuthInfo(new Id("super", ""));
        }
        arg0.addAuthInfo(new Id(getScheme(), userName));
        return KeeperException.Code.OK;
      }

      return KeeperException.Code.AUTHFAILED;
    }

    @Override
    public boolean isAuthenticated() {
      return true;
    }

    @Override
    public boolean isValid(String arg0) {
      return (arg0 != null) && validUsers.contains(arg0);
    }

    @Override
    public boolean matches(String arg0, String arg1) {
      return arg0.equals(arg1);
    }
  }

  public static class DummyZkCredentialsProvider implements ZkCredentialsProvider {
    public static final Collection<ZkCredentials> solrCreds =
        Arrays.asList(new ZkCredentials("dummyauth", "solr".getBytes(StandardCharsets.UTF_8)));

    @Override
    public Collection<ZkCredentials> getCredentials() {
      return solrCreds;
    }
  }

  public static class DummyZkAclProvider extends SecurityAwareZkACLProvider {

    @Override
    protected List<ACL> createNonSecurityACLsToAdd() {
      List<ACL> result = new ArrayList<>(2);
      result.add(new ACL(ZooDefs.Perms.ALL, new Id("dummyauth", "solr")));
      result.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

      return result;
    }

    @Override
    protected List<ACL> createSecurityACLsToAdd() {
      List<ACL> ret = new ArrayList<ACL>();
      ret.add(new ACL(ZooDefs.Perms.ALL, new Id("dummyauth", "solr")));
      return ret;
    }
  }

}
