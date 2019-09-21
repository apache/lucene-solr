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

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpStatus;
import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.DelegationTokenRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDelegationWithHadoopAuth extends SolrCloudTestCase {
  protected static final int NUM_SERVERS = 2;
  protected static final String USER_1 = "foo";
  protected static final String USER_2 = "bar";
  private static HttpSolrClient primarySolrClient, secondarySolrClient;

  @BeforeClass
  public static void setupClass() throws Exception {
    assumeFalse("Hadoop does not work on Windows", Constants.WINDOWS);

    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(TEST_PATH().resolve("security").resolve("hadoop_simple_auth_with_delegation.json"))
        .configure();

    JettySolrRunner runnerPrimary = cluster.getJettySolrRunners().get(0);
    primarySolrClient =
        new HttpSolrClient.Builder(runnerPrimary.getBaseUrl().toString())
            .build();
    JettySolrRunner runnerSecondary = cluster.getJettySolrRunners().get(1);
    secondarySolrClient =
        new HttpSolrClient.Builder(runnerSecondary.getBaseUrl().toString())
            .build();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (primarySolrClient != null) {
      primarySolrClient.close();
      primarySolrClient = null;
    }

    if (secondarySolrClient != null) {
      secondarySolrClient.close();
      secondarySolrClient = null;
    }
  }

  private String getDelegationToken(final String renewer, final String user, HttpSolrClient solrClient) throws Exception {
    DelegationTokenRequest.Get get = new DelegationTokenRequest.Get(renewer) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
        params.set(PseudoAuthenticator.USER_NAME, user);
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
        params.set(PseudoAuthenticator.USER_NAME, user);
        return params;
      }

      @Override
      public Set<String> getQueryParams() {
        Set<String> queryParams = super.getQueryParams();
        queryParams.add(PseudoAuthenticator.USER_NAME);
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
    else delegationTokenClient = new CloudSolrClient.Builder(Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
        .withLBHttpSolrClientBuilder(new LBHttpSolrClient.Builder()
            .withResponseParser(client.getParser())
            .withSocketTimeout(30000).withConnectionTimeout(15000)
            .withHttpSolrClientBuilder(
                new HttpSolrClient.Builder()
                    .withKerberosDelegationToken(token)
            ))
        .build();
    try {
      ModifiableSolrParams p = new ModifiableSolrParams();
      if (user != null) p.set(PseudoAuthenticator.USER_NAME, user);
      if (op != null) p.set("op", op);
      SolrRequest req = getAdminRequest(p);
      if (user != null || op != null) {
        Set<String> queryParams = new HashSet<>();
        if (user != null) queryParams.add(PseudoAuthenticator.USER_NAME);
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

  private void doSolrRequest(SolrClient client, SolrRequest request,
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
    doSolrRequest(token, HttpStatus.SC_OK, primarySolrClient);

    // fail without token
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, primarySolrClient);

    // pass with token on other server
    doSolrRequest(token, HttpStatus.SC_OK, secondarySolrClient);

    // fail without token on other server
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, secondarySolrClient);
  }

  /**
   * Test basic Delegation Token get/verify
   */
  @Test
  public void testDelegationTokenVerify() throws Exception {
    // Get token
    String token = getDelegationToken(null, USER_1, primarySolrClient);
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
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, primarySolrClient);
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, secondarySolrClient);
  }

  @Test
  public void testDelegationTokenCancel() throws Exception {
    {
      // Get token
      String token = getDelegationToken(null, USER_1, primarySolrClient);
      assertNotNull(token);

      // cancel token, note don't need to be authenticated to cancel (no user specified)
      cancelDelegationToken(token, HttpStatus.SC_OK, primarySolrClient);
      verifyTokenCancelled(token, primarySolrClient);
    }

    {
      // cancel token on different server from where we got it
      String token = getDelegationToken(null, USER_1, primarySolrClient);
      assertNotNull(token);

      cancelDelegationToken(token, HttpStatus.SC_OK, secondarySolrClient);
      verifyTokenCancelled(token, secondarySolrClient);
    }
  }

  @Test
  public void testDelegationTokenCancelFail() throws Exception {
    // cancel a bogus token
    cancelDelegationToken("BOGUS", ErrorCode.NOT_FOUND.code, primarySolrClient);

    {
      // cancel twice, first on same server
      String token = getDelegationToken(null, USER_1, primarySolrClient);
      assertNotNull(token);
      cancelDelegationToken(token, HttpStatus.SC_OK, primarySolrClient);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, secondarySolrClient);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, primarySolrClient);
    }

    {
      // cancel twice, first on other server
      String token = getDelegationToken(null, USER_1, primarySolrClient);
      assertNotNull(token);
      cancelDelegationToken(token, HttpStatus.SC_OK, secondarySolrClient);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, secondarySolrClient);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, primarySolrClient);
    }
  }

  private void verifyDelegationTokenRenew(String renewer, String user)
  throws Exception {
    {
      // renew on same server
      String token = getDelegationToken(renewer, user, primarySolrClient);
      assertNotNull(token);
      long now = Time.now();
      assertTrue(renewDelegationToken(token, HttpStatus.SC_OK, user, primarySolrClient) > now);
      verifyTokenValid(token);
    }

    {
      // renew on different server
      String token = getDelegationToken(renewer, user, primarySolrClient);
      assertNotNull(token);
      long now = Time.now();
      assertTrue(renewDelegationToken(token, HttpStatus.SC_OK, user, secondarySolrClient) > now);
      verifyTokenValid(token);
    }
  }

  @Test
// commented 4-Sep-2018   @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void testDelegationTokenRenew() throws Exception {
    // test with specifying renewer
    verifyDelegationTokenRenew(USER_1, USER_1);

    // test without specifying renewer
    verifyDelegationTokenRenew(null, USER_1);
  }

  @Test
  public void testDelegationTokenRenewFail() throws Exception {
    // don't set renewer and try to renew as an a different user
    String token = getDelegationToken(null, USER_1, primarySolrClient);
    assertNotNull(token);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, USER_2, primarySolrClient);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, USER_2, secondarySolrClient);

    // set renewer and try to renew as different user
    token = getDelegationToken("renewUser", USER_1, primarySolrClient);
    assertNotNull(token);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "notRenewUser", primarySolrClient);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "notRenewUser", secondarySolrClient);
  }

  /**
   * Test that a non-delegation-token "op" http param is handled correctly
   */
  @Test
  public void testDelegationOtherOp() throws Exception {
    assertEquals(HttpStatus.SC_OK, getStatusCode(null, USER_1, "someSolrOperation", primarySolrClient));
  }

  @Test
  public void testZNodePaths() throws Exception {
    getDelegationToken(null, USER_1, primarySolrClient);
    SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(), 1000);
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
    String token = getDelegationToken(null, USER_1, primarySolrClient);
    assertNotNull(token);

    SolrRequest request = getAdminRequest(new ModifiableSolrParams());

    // test without token
    HttpSolrClient ss =
        new HttpSolrClient.Builder(primarySolrClient.getBaseURL().toString())
            .withResponseParser(primarySolrClient.getParser())
            .build();
    try {
      doSolrRequest(ss, request, ErrorCode.UNAUTHORIZED.code);
    } finally {
      ss.close();
    }

    try (HttpSolrClient client = new HttpSolrClient.Builder(primarySolrClient.getBaseURL())
             .withKerberosDelegationToken(token)
             .withResponseParser(primarySolrClient.getParser())
             .build()) {
      // test with token via property
      doSolrRequest(client, request, HttpStatus.SC_OK);

      // test with param -- should throw an exception
      ModifiableSolrParams tokenParam = new ModifiableSolrParams();
      tokenParam.set("delegation", "invalidToken");
      expectThrows(IllegalArgumentException.class, () -> doSolrRequest(client, getAdminRequest(tokenParam), ErrorCode.FORBIDDEN.code));
    }
  }
}
