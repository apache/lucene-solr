package org.apache.solr.cloud;

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

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of the MiniSolrCloudCluster functionality with authentication enabled.
 */
@LuceneTestCase.Slow
@SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class TestAuthenticationFramework extends TestMiniSolrCloudCluster {
  
  public TestAuthenticationFramework () {
    NUM_SERVERS = 5;
    NUM_SHARDS = 2;
    REPLICATION_FACTOR = 2;
  }
  
  static String requestUsername = MockAuthenticationPlugin.expectedUsername;
  static String requestPassword = MockAuthenticationPlugin.expectedPassword;
  
  @Rule
  public TestRule solrTestRules = RuleChain
      .outerRule(new SystemPropertiesRestoreRule());

  @ClassRule
  public static TestRule solrClassRules = RuleChain.outerRule(
      new SystemPropertiesRestoreRule()).around(
      new RevertDefaultThreadHandlerRule());

  @Override
  public void setUp() throws Exception {
    setupAuthenticationPlugin();
    super.setUp();
  }
  
  private void setupAuthenticationPlugin() throws Exception {
    System.setProperty("authenticationPlugin", "org.apache.solr.cloud.TestAuthenticationFramework$MockAuthenticationPlugin");
  }
  
  @Test
  @Override
  public void testBasics() throws Exception {
    requestUsername = MockAuthenticationPlugin.expectedUsername;
    requestPassword = MockAuthenticationPlugin.expectedPassword;
    
    // Should pass
    testCollectionCreateSearchDelete();
    
    requestUsername = MockAuthenticationPlugin.expectedUsername;
    requestPassword = "junkpassword";
    
    // Should fail with 401
    try {
      testCollectionCreateSearchDelete();
      fail("Should've returned a 401 error");
    } catch (Exception ex) {
      if (!ex.getMessage().contains("Error 401")) {
        fail("Should've returned a 401 error");
      }
    }
  }

  @Override
  public void tearDown() throws Exception {
    System.clearProperty("authenticationPlugin");
    super.tearDown();
  }
  
  public static class MockAuthenticationPlugin extends AuthenticationPlugin {
    private static Logger log = LoggerFactory.getLogger(MockAuthenticationPlugin.class);

    public static String expectedUsername = "solr";
    public static String expectedPassword = "s0lrRocks";

    @Override
    public void init(Map<String,Object> pluginConfig) {}

    @Override
    public void doAuthenticate(ServletRequest request, ServletResponse response, FilterChain filterChain)
        throws Exception {
      HttpServletRequest httpRequest = (HttpServletRequest)request;
      String username = httpRequest.getHeader("username");
      String password = httpRequest.getHeader("password");
      
      log.info("Username: "+username+", password: "+password);
      if(MockAuthenticationPlugin.expectedUsername.equals(username) && MockAuthenticationPlugin.expectedPassword.equals(password))      
        filterChain.doFilter(request, response);
      else {
        ((HttpServletResponse)response).sendError(401, "Unauthorized request");
      }
    }

    @Override
    public HttpClientConfigurer getDefaultConfigurer() {
      return new MockClientConfigurer();
    }

    @Override
    public void close() {}
    
    private static class MockClientConfigurer extends HttpClientConfigurer {
      @Override
      public void configure(DefaultHttpClient httpClient, SolrParams config) {
        super.configure(httpClient, config);
        httpClient.addRequestInterceptor(new HttpRequestInterceptor() {
          @Override
          public void process(HttpRequest req, HttpContext rsp) throws HttpException, IOException {
            req.addHeader("username", requestUsername);
            req.addHeader("password", requestPassword);
          }
        });
      }
    }
  }
}
