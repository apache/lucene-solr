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
package org.apache.solr.security;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHeader;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocalForTesting;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.cloud.SolrCloudAuthTestCase.NOT_NULL_PREDICATE;
import static org.apache.solr.security.BasicAuthIntegrationTest.verifySecurityStatus;
import static org.apache.solr.security.BasicAuthStandaloneTest.SolrInstance;
import static org.apache.solr.security.BasicAuthStandaloneTest.createAndStartJetty;
import static org.apache.solr.security.BasicAuthStandaloneTest.doHttpPost;
import static org.apache.solr.security.BasicAuthStandaloneTest.doHttpPostWithHeader;

public class MultiAuthPluginTest extends SolrTestCaseJ4 {

  private static final String authcPrefix = "/admin/authentication";
  private static final String authzPrefix = "/admin/authorization";

  final Predicate<Object> NULL_PREDICATE = Objects::isNull;
  SecurityConfHandlerLocalForTesting securityConfHandler;
  JettySolrRunner jetty;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    SolrInstance instance = new SolrInstance("inst", null);
    instance.setUp();
    jetty = createAndStartJetty(instance);
    securityConfHandler = new SecurityConfHandlerLocalForTesting(jetty.getCoreContainer());
    HttpClientUtil.clearRequestInterceptors(); // Clear out any old Authorization headers
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    super.tearDown();
  }

  @Test
  public void testMultiAuthEditAPI() throws Exception {
    final String user = "admin";
    final String pass = "SolrRocks";

    HttpClient cl = null;
    HttpSolrClient httpSolrClient = null;
    try {
      cl = HttpClientUtil.createClient(null);
      String baseUrl = buildUrl(jetty.getLocalPort(), "/solr");
      httpSolrClient = getHttpSolrClient(baseUrl);

      verifySecurityStatus(cl, baseUrl + authcPrefix, "/errorMessages", null, 5);

      // Initialize security.json with multiple auth plugins configured
      String multiAuthPluginSecurityJson =
          FileUtils.readFileToString(TEST_PATH().resolve("security").resolve("multi_auth_plugin_security.json").toFile(), StandardCharsets.UTF_8);
      securityConfHandler.persistConf(new SecurityConfHandler.SecurityConfig().setData(Utils.fromJSONString(multiAuthPluginSecurityJson)));
      securityConfHandler.securityConfEdited();
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/class", "solr.MultiAuthPlugin", 5, user, pass);
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/class", "solr.MultiAuthRuleBasedAuthorizationPlugin", 5, user, pass);

      // anonymous requests are blocked by all plugins
      int statusCode = doHttpGetAnonymous(cl, baseUrl + "/admin/info/system");
      assertEquals("anonymous get succeeded but should not have", 401, statusCode);
      // update blockUnknown to allow anonymous for the basic plugin
      String command = "{\n" +
          "'set-property': { 'basic': {'blockUnknown':false} }\n" +
          "}";
      doHttpPost(cl, baseUrl + authcPrefix, command, user, pass, 200);
      statusCode = doHttpGetAnonymous(cl, baseUrl + "/admin/info/system");
      assertEquals("anonymous get failed but should have succeeded", 200, statusCode);

      // For the multi-auth plugin, every command is wrapped with an object that identifies the "scheme"
      command = "{\n" +
          "'set-user': {'harry':'HarryIsCool'}\n" +
          "}";
      // no scheme identified!
      doHttpPost(cl, baseUrl + authcPrefix, command, user, pass, 400);

      command = "{\n" +
          "'set-user': { 'foo': {'harry':'HarryIsCool'} }\n" +
          "}";
      // no "foo" scheme configured
      doHttpPost(cl, baseUrl + authcPrefix, command, user, pass, 400);

      command = "{\n" +
          "'set-user': { 'basic': {'harry':'HarryIsCool'} }\n" +
          "}";

      // no creds, should fail ...
      doHttpPost(cl, baseUrl + authcPrefix, command, null, null, 401);
      // with basic creds, should pass ...
      doHttpPost(cl, baseUrl + authcPrefix, command, user, pass, 200);
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/schemes[0]/credentials/harry", NOT_NULL_PREDICATE, 5, user, pass);

      // authz command but missing the "scheme" wrapper
      command = "{\n" +
          "'set-user-role': {'harry':['users']}\n" +
          "}";
      doHttpPost(cl, baseUrl + authzPrefix, command, user, pass, 400);

      // add "harry" to the "users" role ...
      command = "{\n" +
          "'set-user-role': { 'basic': {'harry':['users']} }\n" +
          "}";
      doHttpPost(cl, baseUrl + authzPrefix, command, user, pass, 200);
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/schemes[0]/user-role/harry", NOT_NULL_PREDICATE, 5, user, pass);

      // give the users role a custom permission
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/permissions[6]", NULL_PREDICATE, 5, user, pass);
      command = "{\n" +
          "'set-permission': { 'name':'k8s-zk', 'role':'users', 'collection':null, 'path':'/admin/zookeeper/status' }\n" +
          "}";
      doHttpPost(cl, baseUrl + authzPrefix, command, user, pass, 200);
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/permissions[6]/path", new ExpectedValuePredicate("/admin/zookeeper/status"), 5, user, pass);

      command = "{\n" +
          "'update-permission': { 'index':'7', 'name':'k8s-zk', 'role':'users', 'collection':null, 'path':'/admin/zookeeper/status2' }\n" +
          "}";
      doHttpPost(cl, baseUrl + authzPrefix, command, user, pass, 200);
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/permissions[6]/path", new ExpectedValuePredicate("/admin/zookeeper/status2"), 5, user, pass);

      // delete the permission
      command = "{\n" +
          "'delete-permission': 7\n" +
          "}";
      doHttpPost(cl, baseUrl + authzPrefix, command, user, pass, 200);
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/permissions[6]", NULL_PREDICATE, 5, user, pass);

      // delete the user
      command = "{\n" +
          "'delete-user': { 'basic': 'harry' }\n" +
          "}";

      doHttpPost(cl, baseUrl + authcPrefix, command, user, pass, 200);
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/schemes[0]/credentials/harry", NULL_PREDICATE, 5, user, pass);

      // update the property on the mock (just to test routing to the mock plugin)
      command = "{\n" +
          "'set-property': { 'mock': { 'blockUnknown':false } }\n" +
          "}";

      doHttpPostWithHeader(cl, baseUrl + authcPrefix, command, new BasicHeader("Authorization", "mock foo"), 200);
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/schemes[1]/blockUnknown", new ExpectedValuePredicate(Boolean.FALSE), 5, user, pass);
    } finally {
      if (cl != null) {
        HttpClientUtil.close(cl);
      }
      if (httpSolrClient != null) {
        httpSolrClient.close();
      }
    }
  }

  private int doHttpGetAnonymous(HttpClient cl, String url) throws IOException {
    HttpGet httpPost = new HttpGet(url);
    HttpResponse r = cl.execute(httpPost);
    int statusCode = r.getStatusLine().getStatusCode();
    Utils.consumeFully(r.getEntity());
    return statusCode;
  }

  private static final class MockPrincipal implements Principal, Serializable {
    @Override
    public String getName() {
      return "mock";
    }
  }

  public static final class MockAuthPluginForTesting extends AuthenticationPlugin implements ConfigEditablePlugin {

    @Override
    public void init(Map<String, Object> pluginConfig) {

    }

    @Override
    public boolean doAuthenticate(ServletRequest request, ServletResponse response, FilterChain filterChain) throws Exception {
      Principal principal = new MockPrincipal();
      request = wrapWithPrincipal((HttpServletRequest) request, principal, "mock");
      filterChain.doFilter(request, response);
      return true;
    }

    HttpServletRequest wrapWithPrincipal(HttpServletRequest request, Principal principal, String username) {
      return new HttpServletRequestWrapper(request) {
        @Override
        public Principal getUserPrincipal() {
          return principal;
        }

        @Override
        public String getRemoteUser() {
          return username;
        }
      };
    }

    @Override
    public Map<String, Object> edit(Map<String, Object> latestConf, List<CommandOperation> commands) {
      for (CommandOperation op : commands) {
        if ("set-property".equals(op.name)) {
          for (Map.Entry<String, Object> e : op.getDataMap().entrySet()) {
            if ("blockUnknown".equals(e.getKey())) {
              latestConf.put(e.getKey(), e.getValue());
              return latestConf;
            } else {
              op.addError("Unknown property " + e.getKey());
            }
          }
        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unsupported command: " + op.name);
        }
      }
      return null;
    }
  }

  private static final class ExpectedValuePredicate implements Predicate<Object> {
    final Object expected;

    ExpectedValuePredicate(Object exp) {
      this.expected = exp;
    }

    @Override
    public boolean test(Object s) {
      return expected.equals(s);
    }
  }
}
