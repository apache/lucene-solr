package org.apache.solr.security;

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


import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.security.TestAuthorizationFramework.verifySecurityStatus;

@SolrTestCaseJ4.SuppressSSL
public class PKIAuthenticationIntegrationTest extends AbstractFullDistribZkTestBase {
  final private Logger log = LoggerFactory.getLogger(PKIAuthenticationIntegrationTest.class);

  static final int TIMEOUT = 10000;

  @Test
  public void testPkiAuth() throws Exception {
    waitForThingsToLevelOut(10);

    byte[] bytes = Utils.toJSON(makeMap("authorization", singletonMap("class", MockAuthorizationPlugin.class.getName()),
        "authentication", singletonMap("class", MockAuthenticationPlugin.class.getName())));

    try (ZkStateReader zkStateReader = new ZkStateReader(zkServer.getZkAddress(),
        TIMEOUT, TIMEOUT)) {
      zkStateReader.getZkClient().setData(ZkStateReader.SOLR_SECURITY_CONF_PATH, bytes, true);
    }
    for (JettySolrRunner jetty : jettys) {
      String baseUrl = jetty.getBaseUrl().toString();
      verifySecurityStatus(cloudClient.getLbClient().getHttpClient(), baseUrl + "/admin/authorization", "authorization/class", MockAuthorizationPlugin.class.getName(), 20);
      verifySecurityStatus(cloudClient.getLbClient().getHttpClient(), baseUrl + "/admin/authentication", "authentication.enabled", "true", 20);
    }
    log.info("Starting test");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("__user", "solr");
    params.add("__pwd", "SolrRocks");
    // This should work fine.
    final AtomicInteger count = new AtomicInteger();


    MockAuthorizationPlugin.predicate = new Predicate<AuthorizationContext>() {
      @Override
      public boolean test(AuthorizationContext context) {
        if ("/select".equals(context.getResource())) {
          Principal principal = context.getUserPrincipal();
          log.info("principalIs : {}", principal);
          if (principal != null && principal.getName().equals("solr")) {
            count.incrementAndGet();
          }
        }
        return true;
      }
    };

    MockAuthenticationPlugin.predicate = new Predicate<ServletRequest>() {
      @Override
      public boolean test(ServletRequest servletRequest) {
        String s = ((HttpServletRequest) servletRequest).getQueryString();
        if (s != null && s.contains("__user=solr") && s.contains("__pwd=SolrRocks")) {
          servletRequest.setAttribute(Principal.class.getName(), "solr");
        }
        return true;
      }
    };
    QueryRequest query = new QueryRequest(params);
    query.process(cloudClient);
    assertTrue("all nodes must get the user solr , no:of nodes got solr : " + count.get(),count.get() > 2);
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    MockAuthenticationPlugin.predicate = null;
    MockAuthorizationPlugin.predicate = null;
  }

}
