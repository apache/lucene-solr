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
package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

public class HttpClusterStateProviderTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int NODE_COUNT = 1;

  private CloudSolrClient cloudSolrClient;
  private CountingHttpClusterStateProvider httpClusterStateProvider;

  @Before
  public void setupCluster() throws Exception {
    configureCluster(NODE_COUNT)
      .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
      .configure();

    final List<String> solrUrls = new ArrayList<>();
    solrUrls.add(cluster.getJettySolrRunner(0).getBaseUrl().toString());

    httpClusterStateProvider = new CountingHttpClusterStateProvider(solrUrls, null);
    cloudSolrClient = new CloudSolrClient(new CloudSolrClient.Builder(httpClusterStateProvider));
    CollectionAdminRequest.Create create = CollectionAdminRequest.Create.createCollection("x", 1, 1);
    CollectionAdminResponse adminResponse = create.process(cloudSolrClient);
    assertTrue(adminResponse.isSuccess());
    cloudSolrClient.setDefaultCollection("x");
  }

  @After
  public void tearDown() throws Exception {
    cloudSolrClient.close();
    httpClusterStateProvider.close();
    super.tearDown();
  }

  @Test
  public void test() throws Exception {
    // the constructor of HttpClusterStateProvider fetches live nodes
    // and creating a collection fetches the cluster state
    // so we can expect exactly 2 http calls to have been made already at this point
    assertEquals(2, httpClusterStateProvider.getRequestCount());

    QueryResponse queryResponse = cloudSolrClient.query(new SolrQuery("*:*"));
    assertEquals(0, queryResponse.getResults().getNumFound());
    // we can expect 1 extra call to fetch and cache the collection state
    assertEquals(3, httpClusterStateProvider.getRequestCount());
    queryResponse = cloudSolrClient.query(new SolrQuery("*:*"));
    assertEquals(0, queryResponse.getResults().getNumFound());
    // the collection state should already be in the cache so we do not expect another call
    assertEquals(3, httpClusterStateProvider.getRequestCount());

    cloudSolrClient.add(new SolrInputDocument("id", "a"));
    // we can expect another call to check if the collection is a routed alias
    assertEquals(4, httpClusterStateProvider.getRequestCount());
    cloudSolrClient.add(List.of(new SolrInputDocument("id", "b"), new SolrInputDocument("id", "c")));
    assertEquals(4, httpClusterStateProvider.getRequestCount());
    cloudSolrClient.commit();
    assertEquals(4, httpClusterStateProvider.getRequestCount());

    queryResponse = cloudSolrClient.query(new SolrQuery("*:*"));
    assertEquals(3, queryResponse.getResults().getNumFound());
    // the collection state should already be in the cache so we do not expect another call
    assertEquals(4, httpClusterStateProvider.getRequestCount());
  }
}
