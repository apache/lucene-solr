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

import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.HttpParamDelegationTokenPlugin;
import org.apache.solr.security.KerberosPlugin;
import org.apache.solr.servlet.SolrRequestParsers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.security.HttpParamDelegationTokenPlugin.REMOTE_ADDRESS_PARAM;
import static org.apache.solr.security.HttpParamDelegationTokenPlugin.REMOTE_HOST_PARAM;
import static org.apache.solr.security.HttpParamDelegationTokenPlugin.USER_PARAM;

public class TestSolrCloudWithSecureImpersonation extends SolrTestCaseJ4 {
  private static final int NUM_SERVERS = 2;
  private static MiniSolrCloudCluster miniCluster;
  private static SolrClient solrClient;

  private static String getUsersFirstGroup() throws Exception {
    String group = "*"; // accept any group if a group can't be found
    org.apache.hadoop.security.Groups hGroups =
        new org.apache.hadoop.security.Groups(new Configuration());
    try {
      List<String> g = hGroups.getGroups(System.getProperty("user.name"));
      if (g != null && g.size() > 0) {
        group = g.get(0);
      }
    } catch (NullPointerException npe) {
      // if user/group doesn't exist on test box
    }
    return group;
  }

  private static Map<String, String> getImpersonatorSettings() throws Exception {
    Map<String, String> filterProps = new TreeMap<>();
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "noGroups.hosts", "*");
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "anyHostAnyUser.groups", "*");
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "anyHostAnyUser.hosts", "*");
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "wrongHost.hosts", DEAD_HOST_1);
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "wrongHost.groups", "*");
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "noHosts.groups", "*");
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "localHostAnyGroup.groups", "*");
    InetAddress loopback = InetAddress.getLoopbackAddress();
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "localHostAnyGroup.hosts",
        loopback.getCanonicalHostName() + "," + loopback.getHostName() + "," + loopback.getHostAddress());
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "anyHostUsersGroup.groups", getUsersFirstGroup());
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "anyHostUsersGroup.hosts", "*");
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "bogusGroup.groups", "__some_bogus_group");
    filterProps.put(KerberosPlugin.IMPERSONATOR_PREFIX + "bogusGroup.hosts", "*");
    return filterProps;
  }

  @BeforeClass
  public static void startup() throws Exception {
    HdfsTestUtil.checkAssumptions();
    
    System.setProperty("authenticationPlugin", HttpParamDelegationTokenPlugin.class.getName());
    System.setProperty(KerberosPlugin.DELEGATION_TOKEN_ENABLED, "true");

    System.setProperty("solr.kerberos.cookie.domain", "127.0.0.1");
    Map<String, String> impSettings = getImpersonatorSettings();
    for (Map.Entry<String, String> entry : impSettings.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    SolrRequestParsers.DEFAULT.setAddRequestHeadersToContext(true);
    System.setProperty("collectionsHandler", ImpersonatorCollectionsHandler.class.getName());

    miniCluster = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), buildJettyConfig("/solr"));
    JettySolrRunner runner = miniCluster.getJettySolrRunners().get(0);
    solrClient = new HttpSolrClient.Builder(runner.getBaseUrl().toString()).build();
  }

  /**
   * Verify that impersonator info is preserved in the request
   */
  public static class ImpersonatorCollectionsHandler extends CollectionsHandler {
    public static AtomicBoolean called = new AtomicBoolean(false);

    public ImpersonatorCollectionsHandler() { super(); }

    public ImpersonatorCollectionsHandler(final CoreContainer coreContainer) {
      super(coreContainer);
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      called.set(true);
      super.handleRequestBody(req, rsp);
      String doAs = req.getParams().get(KerberosPlugin.IMPERSONATOR_DO_AS_HTTP_PARAM);
      if (doAs != null) {
        HttpServletRequest httpRequest = (HttpServletRequest)req.getContext().get("httpRequest");
        assertNotNull(httpRequest);
        String user = (String)httpRequest.getAttribute(USER_PARAM);
        assertNotNull(user);
        assertEquals(user, httpRequest.getAttribute(KerberosPlugin.IMPERSONATOR_USER_NAME));
      }
    }
  }

  @Before
  public void clearCalledIndicator() {
    ImpersonatorCollectionsHandler.called.set(false);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (solrClient != null) {
      IOUtils.closeQuietly(solrClient);
      solrClient = null;
    }
    try {
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    } finally {
      miniCluster = null;
      System.clearProperty("authenticationPlugin");
      System.clearProperty(KerberosPlugin.DELEGATION_TOKEN_ENABLED);
      System.clearProperty("solr.kerberos.cookie.domain");
      Map<String, String> impSettings = getImpersonatorSettings();
      for (Map.Entry<String, String> entry : impSettings.entrySet()) {
        System.clearProperty(entry.getKey());
      }
      System.clearProperty("solr.test.sys.prop1");
      System.clearProperty("solr.test.sys.prop2");
      System.clearProperty("collectionsHandler");

      SolrRequestParsers.DEFAULT.setAddRequestHeadersToContext(false);
    }
  }

  private void create1ShardCollection(String name, String config, MiniSolrCloudCluster solrCluster) throws Exception {
    CollectionAdminResponse response;
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create(name,config,1,1,0,0) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams msp = new ModifiableSolrParams(super.getParams());
        msp.set(USER_PARAM, "user");
        return msp;
      }
    };
    create.setMaxShardsPerNode(1);
    response = create.process(solrCluster.getSolrClient());

    miniCluster.waitForActiveCollection(name, 1, 1);
    
    if (response.getStatus() != 0 || response.getErrorMessages() != null) {
      fail("Could not create collection. Response" + response.toString());
    }
  }

  @SuppressWarnings({"rawtypes"})
  private SolrRequest getProxyRequest(String user, String doAs) {
    return getProxyRequest(user, doAs, null);
  }

  @SuppressWarnings({"rawtypes"})
  private SolrRequest getProxyRequest(String user, String doAs, String remoteHost) {
    return getProxyRequest(user, doAs, remoteHost, null);
  }

  @SuppressWarnings({"rawtypes"})
  private SolrRequest getProxyRequest(String user, String doAs, String remoteHost, String remoteAddress) {
    return new CollectionAdminRequest.List() {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
        params.set(USER_PARAM, user);
        params.set(KerberosPlugin.IMPERSONATOR_DO_AS_HTTP_PARAM, doAs);
        if (remoteHost != null) params.set(REMOTE_HOST_PARAM, remoteHost);
        if (remoteAddress != null) params.set(REMOTE_ADDRESS_PARAM, remoteAddress);
        return params;
      }
    };
  }

  private String getExpectedGroupExMsg(String user, String doAs) {
    return "User: " + user + " is not allowed to impersonate " + doAs;
  }

  private String getExpectedHostExMsg(String user) {
    return "Unauthorized connection for super-user: " + user;
  }

  @Test
  public void testProxyNoConfigGroups() throws Exception {
    HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class,
        () -> solrClient.request(getProxyRequest("noGroups","bar"))
    );
    assertTrue(e.getMessage().contains(getExpectedGroupExMsg("noGroups", "bar")));
  }

  @Test
  public void testProxyWrongHost() throws Exception {
    HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class,
        () -> solrClient.request(getProxyRequest("wrongHost","bar"))
    );
    assertTrue(e.getMessage().contains(getExpectedHostExMsg("wrongHost")));
  }

  @Test
  public void testProxyNoConfigHosts() throws Exception {
    HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class,
        () -> solrClient.request(getProxyRequest("noHosts","bar"))
    );
    // FixMe: this should return an exception about the host being invalid,
    // but a bug (HADOOP-11077) causes an NPE instead.
    // assertTrue(ex.getMessage().contains(getExpectedHostExMsg("noHosts")));
  }

  @Test
  public void testProxyValidateAnyHostAnyUser() throws Exception {
    solrClient.request(getProxyRequest("anyHostAnyUser", "bar", null));
    assertTrue(ImpersonatorCollectionsHandler.called.get());
  }

  @Test
  public void testProxyInvalidProxyUser() throws Exception {
    // wrong direction, should fail
    HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class,
        () -> solrClient.request(getProxyRequest("bar","anyHostAnyUser"))
    );
    assertTrue(e.getMessage().contains(getExpectedGroupExMsg("bar", "anyHostAnyUser")));
  }

  @Test
  public void testProxyValidateHost() throws Exception {
    solrClient.request(getProxyRequest("localHostAnyGroup", "bar"));
    assertTrue(ImpersonatorCollectionsHandler.called.get());
  }



  @Test
  public void testProxyValidateGroup() throws Exception {
    solrClient.request(getProxyRequest("anyHostUsersGroup", System.getProperty("user.name"), null));
    assertTrue(ImpersonatorCollectionsHandler.called.get());
  }

  @Test
  public void testProxyUnknownRemote() throws Exception {
    HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class,
        () -> {
          // Use a reserved ip address
          String nonProxyUserConfiguredIpAddress = "255.255.255.255";
          solrClient.request(getProxyRequest("localHostAnyGroup", "bar", "unknownhost.bar.foo", nonProxyUserConfiguredIpAddress));
    });
    assertTrue(e.getMessage().contains(getExpectedHostExMsg("localHostAnyGroup")));
  }

  @Test
  public void testProxyInvalidRemote() throws Exception {
    HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class,
        () -> {
          solrClient.request(getProxyRequest("localHostAnyGroup","bar", "[ff01::114]", DEAD_HOST_2));
    });
  }

  @Test
  public void testProxyInvalidGroup() throws Exception {
    HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class,
        () -> solrClient.request(getProxyRequest("bogusGroup","bar", null))
    );
    assertTrue(e.getMessage().contains(getExpectedGroupExMsg("bogusGroup", "bar")));
  }

  @Test
  public void testProxyNullProxyUser() throws Exception {
    expectThrows(HttpSolrClient.RemoteSolrException.class,
        () -> solrClient.request(getProxyRequest("","bar"))
    );
  }

  @Test
  public void testForwarding() throws Exception {
    String collectionName = "forwardingCollection";
    miniCluster.uploadConfigSet(TEST_PATH().resolve("collection1/conf"), "conf1");
    create1ShardCollection(collectionName, "conf1", miniCluster);

    // try a command to each node, one of them must be forwarded
    for (JettySolrRunner jetty : miniCluster.getJettySolrRunners()) {
      try (HttpSolrClient client = new HttpSolrClient.Builder(
          jetty.getBaseUrl().toString() + "/" + collectionName).build()) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("q", "*:*");
        params.set(USER_PARAM, "user");
        client.query(params);
      }
    }
  }
}
