package org.apache.solr.client.solrj.impl;

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

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL
public class ExternalHttpClientTest extends SolrJettyTestBase {
  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
    jetty.getDispatchFilter().getServletHandler()
        .addServletWithMapping(BasicHttpSolrServerTest.SlowServlet.class, "/slow/*");
  }

  /**
   * The internal client created by HttpSolrServer is a SystemDefaultHttpClient
   * which takes care of merging request level params (such as timeout) with the
   * configured defaults.
   *
   * However, if an external HttpClient is passed to HttpSolrServer,
   * the logic in InternalHttpClient.executeMethod replaces the configured defaults
   * by request level params if they exist. That is why we must test a setting such
   * as timeout with an external client to assert that the defaults are indeed being
   * used
   *
   * See SOLR-6245 for more details
   */
  @Test
  public void testTimeoutWithExternalClient() throws Exception {
    HttpClientBuilder builder = HttpClientBuilder.create();
    RequestConfig config = RequestConfig.custom().setSocketTimeout(2000).build();
    builder.setDefaultRequestConfig(config);
    HttpSolrServer server = null;
    try (CloseableHttpClient httpClient = builder.build()) {
      server = new HttpSolrServer(jetty.getBaseUrl().toString() +
          "/slow/foo", httpClient);

      SolrQuery q = new SolrQuery("*:*");
      try {
        QueryResponse response = server.query(q, SolrRequest.METHOD.GET);
        fail("No exception thrown.");
      } catch (SolrServerException e) {
        assertTrue(e.getMessage().contains("Timeout"));
      }
    } finally {
      if (server != null) {
        server.shutdown();
      }
    }
  }
}
