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

import junit.framework.Assert;
import org.apache.hadoop.util.Time;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.DelegationTokenRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import static org.apache.solr.security.HttpParamDelegationTokenPlugin.USER_PARAM;

import org.apache.http.HttpStatus;
import org.apache.solr.security.HttpParamDelegationTokenPlugin;
import org.apache.solr.security.KerberosPlugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the delegation token support in the {@link org.apache.solr.security.KerberosPlugin}.
 */
@LuceneTestCase.Slow
public class TestSolrCloudWithDelegationTokens extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int NUM_SERVERS = 2;
  private static MiniSolrCloudCluster miniCluster;
  private static HttpSolrClient solrClientPrimary;
  private static HttpSolrClient solrClientSecondary;

  @BeforeClass
  public static void startup() throws Exception {
    System.setProperty("authenticationPlugin", HttpParamDelegationTokenPlugin.class.getName());
    System.setProperty(KerberosPlugin.DELEGATION_TOKEN_ENABLED, "true");
    System.setProperty("solr.kerberos.cookie.domain", "127.0.0.1");

    miniCluster = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), buildJettyConfig("/solr"));
    JettySolrRunner runnerPrimary = miniCluster.getJettySolrRunners().get(0);
    solrClientPrimary =
        new HttpSolrClient.Builder(runnerPrimary.getBaseUrl().toString())
            .build();
    JettySolrRunner runnerSecondary = miniCluster.getJettySolrRunners().get(1);
    solrClientSecondary =
        new HttpSolrClient.Builder(runnerSecondary.getBaseUrl().toString())
            .build();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
    solrClientPrimary.close();
    solrClientPrimary = null;
    solrClientSecondary.close();
    solrClientSecondary = null;
    System.clearProperty("authenticationPlugin");
    System.clearProperty(KerberosPlugin.DELEGATION_TOKEN_ENABLED);
    System.clearProperty("solr.kerberos.cookie.domain");
  }

  private String getDelegationToken(final String renewer, final String user, HttpSolrClient solrClient) throws Exception {
    DelegationTokenRequest.Get get = new DelegationTokenRequest.Get(renewer) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
        params.set(USER_PARAM, user);
        return params;
      }
    };
    DelegationTokenResponse.Get getResponse = get.process(solrClient);
    return getResponse.getDelegationToken();
  }

  private long renewDelegationToken(final String token, final int expectedStatusCode,
      final String user, HttpSolrClient client) throws Exception {
    DelegationTokenRequest.Renew renew = new DelegationTokenRequest.Renew(token) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
        params.set(USER_PARAM, user);
        return params;
      }

      @Override
      public Set<String> getQueryParams() {
        Set<String> queryParams = super.getQueryParams();
        queryParams.add(USER_PARAM);
        return queryParams;
      }
    };
    try {
      DelegationTokenResponse.Renew renewResponse = renew.process(client);
      assertEquals(HttpStatus.SC_OK, expectedStatusCode);
      return renewResponse.getExpirationTime();
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertEquals(expectedStatusCode, ex.code());
      return -1;
    }
  }

  private void cancelDelegationToken(String token, int expectedStatusCode, HttpSolrClient client)
  throws Exception {
    DelegationTokenRequest.Cancel cancel = new DelegationTokenRequest.Cancel(token);
    try {
      cancel.process(client);
      assertEquals(HttpStatus.SC_OK, expectedStatusCode);
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertEquals(expectedStatusCode, ex.code());
    }
  }

  private void doSolrRequest(String token, int expectedStatusCode, HttpSolrClient client)
  throws Exception {
    doSolrRequest(token, expectedStatusCode, client, 1);
  }

  private void doSolrRequest(String token, int expectedStatusCode, HttpSolrClient client, int trials)
  throws Exception {
    int lastStatusCode = 0;
    for (int i = 0; i < trials; ++i) {
      lastStatusCode = getStatusCode(token, null, null, client);
      if (lastStatusCode == expectedStatusCode) {
        return;
      }
      Thread.sleep(1000);
    }
    assertEquals("Did not receieve excepted status code", expectedStatusCode, lastStatusCode);
  }

  private SolrRequest getAdminRequest(final SolrParams params) {
    return new CollectionAdminRequest.List() {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams p = new ModifiableSolrParams(super.getParams());
        p.add(params);
        return p;
      }
    };
  }

  private int getStatusCode(String token, final String user, final String op, HttpSolrClient client)
  throws Exception {
    SolrClient delegationTokenClient;
    if (random().nextBoolean()) delegationTokenClient = new HttpSolrClient.Builder(client.getBaseURL().toString())
        .withKerberosDelegationToken(token)
        .withResponseParser(client.getParser())
        .build();
    else delegationTokenClient = new CloudSolrClient.Builder()
        .withZkHost((miniCluster.getZkServer().getZkAddress()))
        .withLBHttpSolrClientBuilder(new LBHttpSolrClient.Builder()
            .withResponseParser(client.getParser())
            .withHttpSolrClientBuilder(
                new HttpSolrClient.Builder()
                    .withKerberosDelegationToken(token)
            ))
        .build();
    try {
      ModifiableSolrParams p = new ModifiableSolrParams();
      if (user != null) p.set(USER_PARAM, user);
      if (op != null) p.set("op", op);
      SolrRequest req = getAdminRequest(p);
      if (user != null || op != null) {
        Set<String> queryParams = new HashSet<>();
        if (user != null) queryParams.add(USER_PARAM);
        if (op != null) queryParams.add("op");
        req.setQueryParams(queryParams);
      }
      try {
        delegationTokenClient.request(req, null);
        return HttpStatus.SC_OK;
      } catch (HttpSolrClient.RemoteSolrException re) {
        return re.code();
      }
    } finally {
      delegationTokenClient.close();
    }
  }

  private void doSolrRequest(HttpSolrClient client, SolrRequest request,
      int expectedStatusCode) throws Exception {
    try {
      client.request(request);
      assertEquals(HttpStatus.SC_OK, expectedStatusCode);
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertEquals(expectedStatusCode, ex.code());
    }
  }

  private void verifyTokenValid(String token) throws Exception {
     // pass with token
    doSolrRequest(token, HttpStatus.SC_OK, solrClientPrimary);

    // fail without token
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, solrClientPrimary);

    // pass with token on other server
    doSolrRequest(token, HttpStatus.SC_OK, solrClientSecondary);

    // fail without token on other server
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, solrClientSecondary);
  }

  /**
   * Test basic Delegation Token get/verify
   */
  @Test
  public void testDelegationTokenVerify() throws Exception {
    final String user = "bar";

    // Get token
    String token = getDelegationToken(null, user, solrClientPrimary);
    assertNotNull(token);
    verifyTokenValid(token);
  }

  private void verifyTokenCancelled(String token, HttpSolrClient client) throws Exception {
    // fail with token on both servers.  If cancelToOtherURL is true,
    // the request went to other url, so FORBIDDEN should be returned immediately.
    // The cancelled token may take awhile to propogate to the standard url (via ZK).
    // This is of course the opposite if cancelToOtherURL is false.
    doSolrRequest(token, ErrorCode.FORBIDDEN.code, client, 10);

    // fail without token on both servers
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, solrClientPrimary);
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, solrClientSecondary);
  }

  @Test
  public void testDelegationTokenCancel() throws Exception {
    {
      // Get token
      String token = getDelegationToken(null, "user", solrClientPrimary);
      assertNotNull(token);

      // cancel token, note don't need to be authenticated to cancel (no user specified)
      cancelDelegationToken(token, HttpStatus.SC_OK, solrClientPrimary);
      verifyTokenCancelled(token, solrClientPrimary);
    }

    {
      // cancel token on different server from where we got it
      String token = getDelegationToken(null, "user", solrClientPrimary);
      assertNotNull(token);

      cancelDelegationToken(token, HttpStatus.SC_OK, solrClientSecondary);
      verifyTokenCancelled(token, solrClientSecondary);
    }
  }

  @Test
  public void testDelegationTokenCancelFail() throws Exception {
    // cancel a bogus token
    cancelDelegationToken("BOGUS", ErrorCode.NOT_FOUND.code, solrClientPrimary);

    {
      // cancel twice, first on same server
      String token = getDelegationToken(null, "bar", solrClientPrimary);
      assertNotNull(token);
      cancelDelegationToken(token, HttpStatus.SC_OK, solrClientPrimary);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, solrClientSecondary);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, solrClientPrimary);
    }

    {
      // cancel twice, first on other server
      String token = getDelegationToken(null, "bar", solrClientPrimary);
      assertNotNull(token);
      cancelDelegationToken(token, HttpStatus.SC_OK, solrClientSecondary);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, solrClientSecondary);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, solrClientPrimary);
    }
  }

  private void verifyDelegationTokenRenew(String renewer, String user)
  throws Exception {
    {
      // renew on same server
      String token = getDelegationToken(renewer, user, solrClientPrimary);
      assertNotNull(token);
      long now = Time.now();
      assertTrue(renewDelegationToken(token, HttpStatus.SC_OK, user, solrClientPrimary) > now);
      verifyTokenValid(token);
    }

    {
      // renew on different server
      String token = getDelegationToken(renewer, user, solrClientPrimary);
      assertNotNull(token);
      long now = Time.now();
      assertTrue(renewDelegationToken(token, HttpStatus.SC_OK, user, solrClientSecondary) > now);
      verifyTokenValid(token);
    }
  }

  @Test
  public void testDelegationTokenRenew() throws Exception {
    // test with specifying renewer
    verifyDelegationTokenRenew("bar", "bar");

    // test without specifying renewer
    verifyDelegationTokenRenew(null, "bar");
  }

  @Test
  public void testDelegationTokenRenewFail() throws Exception {
    // don't set renewer and try to renew as an a different user
    String token = getDelegationToken(null, "bar", solrClientPrimary);
    assertNotNull(token);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "foo", solrClientPrimary);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "foo", solrClientSecondary);

    // set renewer and try to renew as different user
    token = getDelegationToken("renewUser", "bar", solrClientPrimary);
    assertNotNull(token);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "notRenewUser", solrClientPrimary);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "notRenewUser", solrClientSecondary);
  }

  /**
   * Test that a non-delegation-token "op" http param is handled correctly
   */
  @Test
  public void testDelegationOtherOp() throws Exception {
    assertEquals(HttpStatus.SC_OK, getStatusCode(null, "bar", "someSolrOperation", solrClientPrimary));
  }

  @Test
  public void testZNodePaths() throws Exception {
    getDelegationToken(null, "bar", solrClientPrimary);
    SolrZkClient zkClient = new SolrZkClient(miniCluster.getZkServer().getZkAddress(), 1000);
    try {
      assertTrue(zkClient.exists("/security/zkdtsm", true));
      assertTrue(zkClient.exists("/security/token", true));
    } finally {
      zkClient.close();
    }
  }

  /**
   * Test HttpSolrServer's delegation token support
   */
  @Test
  public void testDelegationTokenSolrClient() throws Exception {
    // Get token
    String token = getDelegationToken(null, "bar", solrClientPrimary);
    assertNotNull(token);

    SolrRequest request = getAdminRequest(new ModifiableSolrParams());

    // test without token
    HttpSolrClient ss =
        new HttpSolrClient.Builder(solrClientPrimary.getBaseURL().toString())
            .withResponseParser(solrClientPrimary.getParser())
            .build();
    try {
      doSolrRequest(ss, request, ErrorCode.UNAUTHORIZED.code);
    } finally {
      ss.close();
    }

    ss = new HttpSolrClient.Builder(solrClientPrimary.getBaseURL().toString())
        .withDelegationToken(token)
        .withResponseParser(solrClientPrimary.getParser())
        .build();
    try {
      // test with token via property
      doSolrRequest(ss, request, HttpStatus.SC_OK);

      // test with param -- should throw an exception
      ModifiableSolrParams tokenParam = new ModifiableSolrParams();
      tokenParam.set("delegation", "invalidToken");
      try {
        doSolrRequest(ss, getAdminRequest(tokenParam), ErrorCode.FORBIDDEN.code);
        Assert.fail("Expected exception");
      } catch (IllegalArgumentException ex) {}
    } finally {
      ss.close();
    }
  }
}
