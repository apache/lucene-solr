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

import javax.servlet.http.HttpServletRequest;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.util.Utils.makeMap;

public class PKIAuthenticationIntegrationTest extends SolrCloudAuthTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION = "pkiCollection";
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    final String SECURITY_CONF = Utils.toJSONString
      (makeMap("authorization", singletonMap("class", MockAuthorizationPlugin.class.getName()),
               "authentication", singletonMap("class", MockAuthenticationPlugin.class.getName())));
    
    configureCluster(2)
      .addConfig("conf", configset("cloud-minimal"))
      .withSecurityJson(SECURITY_CONF)
      .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1).process(cluster.getSolrClient());

    cluster.waitForActiveCollection(COLLECTION, 2, 2);
  }

  @Test
  public void testPkiAuth() throws Exception {
    HttpClient httpClient = cluster.getSolrClient().getHttpClient();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      String baseUrl = jetty.getBaseUrl().toString();
      verifySecurityStatus(httpClient, baseUrl + "/admin/authorization", "authorization/class", MockAuthorizationPlugin.class.getName(), 20);
      verifySecurityStatus(httpClient, baseUrl + "/admin/authentication", "authentication.enabled", "true", 20);
    }
    log.info("Starting test");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("__user", "solr");
    params.add("__pwd", "SolrRocks");
    // This should work fine.
    final AtomicInteger count = new AtomicInteger();


    MockAuthorizationPlugin.predicate = context -> {
        if ("/select".equals(context.getResource())) {
          Principal principal = context.getUserPrincipal();
          log.info("principalIs : {}", principal);
          if (principal != null && principal.getName().equals("solr")) {
            count.incrementAndGet();
          }
        }
        return true;
    };

    MockAuthenticationPlugin.predicate = servletRequest -> {
        String s = ((HttpServletRequest) servletRequest).getQueryString();
        if (s != null && s.contains("__user=solr") && s.contains("__pwd=SolrRocks")) {
          servletRequest.setAttribute(Principal.class.getName(), "solr");
        }
        return true;
    };
    QueryRequest query = new QueryRequest(params);
    query.process(cluster.getSolrClient(), COLLECTION);
    assertTrue("all nodes must get the user solr , no:of nodes got solr : " + count.get(), count.get() > 2);
    assertPkiAuthMetricsMinimums(2, 2, 0, 0, 0, 0);
  }

  @After
  public void distribTearDown() throws Exception {
    MockAuthenticationPlugin.predicate = null;
    MockAuthorizationPlugin.predicate = null;
  }

}
