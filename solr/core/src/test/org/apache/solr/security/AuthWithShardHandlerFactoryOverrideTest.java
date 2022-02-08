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

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Similar to BasicAuthOnSingleNodeTest, but using a different configset, to test SOLR-14569
 */
public class AuthWithShardHandlerFactoryOverrideTest extends SolrCloudAuthTestCase {

  private static final String COLLECTION = "authCollection";
  private static final String ALIAS = "alias";
  
  private static final String SECURITY_CONF = "{\n" +
      "  \"authentication\":{\n" +
      "   \"blockUnknown\": true,\n" +
      "   \"class\":\"solr.BasicAuthPlugin\",\n" +
      "   \"credentials\":{\"solr\":\"EEKn7ywYk5jY8vG9TyqlG2jvYuvh1Q7kCCor6Hqm320= 6zkmjMjkMKyJX6/f0VarEWQujju5BzxZXub6WOrEKCw=\"}\n" +
      "  },\n" +
      "  \"authorization\":{\n" +
      "   \"class\":\"solr.RuleBasedAuthorizationPlugin\",\n" +
      "   \"permissions\":[\n" +
      " {\"name\":\"all\", \"role\":\"admin\"}\n" +
      "   ],\n" +
      "   \"user-role\":{\"solr\":\"admin\"}\n" +
      "  }\n" +
      "}";

  @Before
  public void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal_override-shardHandler"))
        .withSecurityJson(SECURITY_CONF)
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 4, 1)
        .setMaxShardsPerNode(100)
        .setBasicAuthCredentials("solr", "solr")
        .process(cluster.getSolrClient());
    
    CollectionAdminRequest.createAlias(ALIAS, COLLECTION)
        .setBasicAuthCredentials("solr", "solr")
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION, 4, 4);

    JettySolrRunner jetty = cluster.getJettySolrRunner(0);
    jetty.stop();
    cluster.waitForJettyToStop(jetty);
    jetty.start();
    cluster.waitForAllNodes(30);
    cluster.waitForActiveCollection(COLLECTION, 4, 4);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    super.tearDown();
  }

  @Test
  public void collectionTest() throws Exception {
    try (Http2SolrClient client = new Http2SolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
        .build()){

      for (int i = 0; i < 30; i++) {
        SolrResponse response = new QueryRequest(params("q", "*:*"))
            .setBasicAuthCredentials("solr", "solr").process(client, COLLECTION);
        // likely to be non-null, even if an error occurred
        assertNotNull(response);
        assertNotNull(response.getResponse());
        // now we know we have results
        assertNotNull(response.getResponse().get("response"));
      }
    }
  }
  
  @Test
  public void aliasTest() throws Exception {
    try (Http2SolrClient client = new Http2SolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
        .build()){

      for (int i = 0; i < 30; i++) {
        SolrResponse response = new QueryRequest(params("q", "*:*"))
            .setBasicAuthCredentials("solr", "solr").process(client, ALIAS);
        // likely to be non-null, even if an error occurred
        assertNotNull(response);
        assertNotNull(response.getResponse());
        // now we know we have results
        assertNotNull(response.getResponse().get("response"));
      }
    }
  }
}
