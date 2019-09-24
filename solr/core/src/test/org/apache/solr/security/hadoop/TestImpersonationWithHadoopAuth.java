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

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.HadoopAuthPlugin;
import org.apache.solr.servlet.SolrRequestParsers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.security.HttpParamDelegationTokenPlugin.USER_PARAM;
import static org.apache.solr.security.hadoop.ImpersonationUtil.getExpectedGroupExMsg;
import static org.apache.solr.security.hadoop.ImpersonationUtil.getExpectedHostExMsg;
import static org.apache.solr.security.hadoop.ImpersonationUtil.getProxyRequest;

public class TestImpersonationWithHadoopAuth  extends SolrCloudTestCase {
  protected static final int NUM_SERVERS = 2;
  private static final boolean defaultAddRequestHeadersToContext =
      SolrRequestParsers.DEFAULT.isAddRequestHeadersToContext();

  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void setupClass() throws Exception {
    assumeFalse("Hadoop does not work on Windows", Constants.WINDOWS);

    InetAddress loopback = InetAddress.getLoopbackAddress();
    Path securityJsonPath = TEST_PATH().resolve("security").resolve("hadoop_simple_auth_with_delegation.json");
    String securityJson = new String(Files.readAllBytes(securityJsonPath), Charset.defaultCharset());

    Map<String, Object> securityConfig = (Map<String, Object>)Utils.fromJSONString(securityJson);
    Map<String, Object> authConfig = (Map<String, Object>)securityConfig.get("authentication");
    Map<String,String> proxyUserConfigs = (Map<String,String>) authConfig
        .getOrDefault(HadoopAuthPlugin.PROXY_USER_CONFIGS, new HashMap<>());
    proxyUserConfigs.put("proxyuser.noGroups.hosts", "*");
    proxyUserConfigs.put("proxyuser.anyHostAnyUser.hosts", "*");
    proxyUserConfigs.put("proxyuser.anyHostAnyUser.groups", "*");
    proxyUserConfigs.put("proxyuser.wrongHost.hosts", "1.1.1.1.1.1");
    proxyUserConfigs.put("proxyuser.wrongHost.groups", "*");
    proxyUserConfigs.put("proxyuser.noHosts.groups", "*");
    proxyUserConfigs.put("proxyuser.localHostAnyGroup.hosts",
        loopback.getCanonicalHostName() + "," + loopback.getHostName() + "," + loopback.getHostAddress());
    proxyUserConfigs.put("proxyuser.localHostAnyGroup.groups", "*");
    proxyUserConfigs.put("proxyuser.bogusGroup.hosts", "*");
    proxyUserConfigs.put("proxyuser.bogusGroup.groups", "__some_bogus_group");
    proxyUserConfigs.put("proxyuser.anyHostUsersGroup.groups", ImpersonationUtil.getUsersFirstGroup());
    proxyUserConfigs.put("proxyuser.anyHostUsersGroup.hosts", "*");

    authConfig.put(HadoopAuthPlugin.PROXY_USER_CONFIGS, proxyUserConfigs);

    SolrRequestParsers.DEFAULT.setAddRequestHeadersToContext(true);
    System.setProperty("collectionsHandler", ImpersonatorCollectionsHandler.class.getName());

    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(Utils.toJSONString(securityConfig))
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    SolrRequestParsers.DEFAULT.setAddRequestHeadersToContext(defaultAddRequestHeadersToContext);
    System.clearProperty("collectionsHandler");
  }

  private SolrClient newSolrClient() {
    return new HttpSolrClient.Builder(
        cluster.getJettySolrRunner(0).getBaseUrl().toString()).build();
  }

  @Test
  public void testProxyNoConfigGroups() throws Exception {
    try (SolrClient solrClient = newSolrClient()) {
      HttpSolrClient.RemoteSolrException ex = expectThrows(HttpSolrClient.RemoteSolrException.class,
          () -> solrClient.request(getProxyRequest("noGroups","bar")));
      assertTrue(ex.getLocalizedMessage(), ex.getMessage().contains(getExpectedGroupExMsg("noGroups", "bar")));
    }
  }

  @Test
  public void testProxyWrongHost() throws Exception {
    try (SolrClient solrClient = newSolrClient()) {
      HttpSolrClient.RemoteSolrException ex = expectThrows(HttpSolrClient.RemoteSolrException.class,
          () -> solrClient.request(getProxyRequest("wrongHost","bar")));
      assertTrue(ex.getMessage().contains(getExpectedHostExMsg("wrongHost")));
    }
  }

  @Test
  public void testProxyNoConfigHosts() throws Exception {
    try (SolrClient solrClient = newSolrClient()) {
      HttpSolrClient.RemoteSolrException ex = expectThrows(HttpSolrClient.RemoteSolrException.class,
          () -> solrClient.request(getProxyRequest("noHosts","bar")));
      assertTrue(ex.getMessage().contains(getExpectedHostExMsg("noHosts")));
    }
  }

  @Test
  public void testProxyValidateAnyHostAnyUser() throws Exception {
    try (SolrClient solrClient = newSolrClient()) {
      solrClient.request(getProxyRequest("anyHostAnyUser", "bar"));
      assertTrue(ImpersonatorCollectionsHandler.called.get());
    }
  }

  @Test
  public void testProxyInvalidProxyUser() throws Exception {
    try (SolrClient solrClient = newSolrClient()) {
      // wrong direction, should fail
      HttpSolrClient.RemoteSolrException ex = expectThrows(HttpSolrClient.RemoteSolrException.class,
          () -> solrClient.request(getProxyRequest("bar","anyHostAnyUser")));
      assertTrue(ex.getMessage().contains(getExpectedGroupExMsg("bar", "anyHostAnyUser")));
    }
  }

  @Test
  public void testProxyValidateHost() throws Exception {
    try (SolrClient solrClient = newSolrClient()) {
      solrClient.request(getProxyRequest("localHostAnyGroup", "bar"));
      assertTrue(ImpersonatorCollectionsHandler.called.get());
    }
  }

  @Test
  public void testProxyValidateGroup() throws Exception {
    try (SolrClient solrClient = newSolrClient()) {
      solrClient.request(getProxyRequest("anyHostUsersGroup", System.getProperty("user.name")));
      assertTrue(ImpersonatorCollectionsHandler.called.get());
    }
  }

  @Test
  public void testProxyInvalidGroup() throws Exception {
    try (SolrClient solrClient = newSolrClient()) {
      HttpSolrClient.RemoteSolrException ex = expectThrows(HttpSolrClient.RemoteSolrException.class,
          () -> solrClient.request(getProxyRequest("bogusGroup","bar")));
      assertTrue(ex.getMessage().contains(getExpectedGroupExMsg("bogusGroup", "bar")));
    }
  }

  @Test
  public void testProxyNullProxyUser() throws Exception {
    try (SolrClient solrClient = newSolrClient()) {
      expectThrows(HttpSolrClient.RemoteSolrException.class, () -> solrClient.request(getProxyRequest("","bar")));
    }
  }

  @Test
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/HADOOP-9893")
  public void testForwarding() throws Exception {
    String collectionName = "forwardingCollection";

    // create collection
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1",
        1, 1);
    try (SolrClient solrClient = newSolrClient()) {
      create.process(solrClient);
    }

    // try a command to each node, one of them must be forwarded
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      try (HttpSolrClient client =
               new HttpSolrClient.Builder(jetty.getBaseUrl().toString() + "/" + collectionName).build()) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("q", "*:*");
        params.set(USER_PARAM, "user");
        client.query(params);
      }
    }
  }

}
