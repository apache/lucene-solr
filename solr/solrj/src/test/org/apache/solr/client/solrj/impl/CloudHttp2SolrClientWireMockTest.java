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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.routing.ShufflingReplicaListTransformer;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

public class CloudHttp2SolrClientWireMockTest extends BaseSolrClientWireMockTest {

  @Test
  public void testQueryRequest() throws Exception {
    stubFor(get(urlPathEqualTo("/solr/wireMock/select"))
        .willReturn(ok()
            .withLogNormalRandomDelay(10, 0.1)
            .withHeader("Content-Type", RESPONSE_CONTENT_TYPE)
            .withBody(queryResponseOk())));

    QueryResponse response = testClient.query(BUILT_IN_MOCK_COLLECTION, new SolrQuery("*:*"));
    assertEquals(1L, response.getResults().getNumFound());
  }

  @Test(expected = SolrServerException.class)
  public void testQueryRequestErrorHandling() throws Exception {
    stubFor(get(urlPathEqualTo("/solr/wireMock/select"))
        .willReturn(aResponse().withStatus(500)));

    testClient.query(BUILT_IN_MOCK_COLLECTION, new SolrQuery("*:*"));
  }

  @Test
  public void testUpdateRequestErrorHandling() throws Exception {
    stubFor(post(urlPathEqualTo(SHARD1_PATH+"/update"))
        .willReturn(aResponse().withStatus(404)));

    stubFor(post(urlPathEqualTo(SHARD2_PATH+"/update"))
        .willReturn(ok()
            .withHeader("Content-Type", RESPONSE_CONTENT_TYPE)
            .withBody(updateRequestOk())));

    UpdateRequest req = buildUpdateRequest(10);
    try {
      req.process(testClient, BUILT_IN_MOCK_COLLECTION);
    } catch (BaseCloudSolrClient.RouteException re) {
      assertEquals(404, re.code());
    }
  }

  // Basic regression test for route logic, should expand to encompass
  // ReplicaListTransformer logic and more complex collection layouts
  @Test
  public void testUpdateRequestRouteLogic() {
    final String shard1Route = mockSolr.baseUrl()+SHARD1_PATH+"/";
    final String shard2Route = mockSolr.baseUrl()+SHARD2_PATH+"/";

    final int numDocs = 20;
    UpdateRequest ur = buildUpdateRequest(numDocs);
    Map<String,List<String>> urlMap = testClient.buildUrlMap(mockDocCollection, new ShufflingReplicaListTransformer(random()));
    assertEquals(2, urlMap.size());
    List<String> shard1 = urlMap.get("s1");
    assertEquals(1, shard1.size());
    assertEquals(shard1Route, shard1.get(0));
    List<String> shard2 = urlMap.get("s2");
    assertEquals(1, shard2.size());
    assertEquals(shard2Route, shard2.get(0));

    Map<String, LBSolrClient.Req> routes =
        ur.getRoutesToCollection(mockDocCollection.getRouter(), mockDocCollection, urlMap, ur.getParams(), "id");
    assertEquals(2, routes.size());
    assertNotNull(routes.get(shard1Route));
    assertNotNull(routes.get(shard1Route).getRequest());
    assertNotNull(routes.get(shard2Route));
    assertNotNull(routes.get(shard2Route).getRequest());

    final String threadName = Thread.currentThread().getName();
    ur = new UpdateRequest();
    for (int i=0; i < numDocs; i++) {
      ur.deleteById(threadName+(1000+i));
    }
    routes = ur.getRoutesToCollection(mockDocCollection.getRouter(), mockDocCollection, urlMap, ur.getParams(), "id");
    assertEquals(2, routes.size());
    assertNotNull(routes.get(shard1Route));
    assertNotNull(routes.get(shard1Route).getRequest());
    assertNotNull(routes.get(shard2Route));
    assertNotNull(routes.get(shard2Route).getRequest());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testParallelUpdates() throws Exception {
    // expect update requests go to both shards
    stubFor(post(urlPathEqualTo(SHARD1_PATH+"/update"))
        .willReturn(ok()
            .withLogNormalRandomDelay(140, 0.1)
            .withHeader("Content-Type", RESPONSE_CONTENT_TYPE)
            .withBody(updateRequestOk())));

    stubFor(post(urlPathEqualTo(SHARD2_PATH+"/update"))
        .willReturn(ok()
            .withLogNormalRandomDelay(70, 0.1)
            .withHeader("Content-Type", RESPONSE_CONTENT_TYPE)
            .withBody(updateRequestOk())));

    assertTrue(testClient.isParallelUpdates());
    assertNotNull(testClient.getClusterStateProvider().getCollection(BUILT_IN_MOCK_COLLECTION));

    UpdateRequest req = buildUpdateRequest(80);
    UpdateResponse response = req.process(testClient, BUILT_IN_MOCK_COLLECTION);
    assertEquals(0, response.getStatus());
    BaseCloudSolrClient.RouteResponse rr = (BaseCloudSolrClient.RouteResponse) response.getResponse();
    assertNotNull(rr);
    NamedList<Object> routeResponses = rr.getRouteResponses();
    assertNotNull(routeResponses);
    assertEquals(2, routeResponses.size());
    assertEquals(2, rr.getRoutes().size());
    NamedList<Object> shard1Response = (NamedList<Object>) routeResponses.get(mockSolr.baseUrl()+SHARD1_PATH+"/");
    assertNotNull(shard1Response);
    NamedList<Object> shard2Response = (NamedList<Object>) routeResponses.get(mockSolr.baseUrl()+SHARD2_PATH+"/");
    assertNotNull(shard2Response);
  }

  @Test(expected = BaseCloudSolrClient.RouteException.class)
  public void testParallelUpdatesWithFailingRoute() throws Exception {
    // update requests sent to shard2 will fail with a 503
    stubFor(post(urlPathEqualTo(SHARD2_PATH+"/update"))
        .willReturn(aResponse().withStatus(503)));

    stubFor(post(urlPathEqualTo(SHARD1_PATH+"/update"))
        .willReturn(ok()
            .withHeader("Content-Type", RESPONSE_CONTENT_TYPE)
            .withBody(updateRequestOk())));

    // should fail with a RouteException
    buildUpdateRequest(20).process(testClient, BUILT_IN_MOCK_COLLECTION);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testConcurrentParallelUpdates() throws Exception {
    // expect update requests go to both shards
    stubFor(post(urlPathEqualTo(SHARD1_PATH+"/update"))
        .willReturn(ok()
            .withLogNormalRandomDelay(140, 0.1)
            .withHeader("Content-Type", RESPONSE_CONTENT_TYPE)
            .withBody(updateRequestOk())));

    stubFor(post(urlPathEqualTo(SHARD2_PATH+"/update"))
        .willReturn(ok()
            .withLogNormalRandomDelay(70, 0.1)
            .withHeader("Content-Type", RESPONSE_CONTENT_TYPE)
            .withBody(updateRequestOk())));

    List<Future<UpdateResponse>> list = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    for (int t=0; t < 10; t++) {
      Future<UpdateResponse> responseFuture = executorService.submit(() -> {
        UpdateRequest req = buildUpdateRequest(20);
        UpdateResponse resp;
        try {
          resp = req.process(testClient, BUILT_IN_MOCK_COLLECTION);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return resp;
      });
      list.add(responseFuture);
    }
    ExecutorUtil.shutdownAndAwaitTermination(executorService);

    List<ServeEvent> events = mockSolr.getAllServeEvents();
    // code should have sent 10 requests to each shard leader
    assertEquals(20, events.size());

    // verify every response has 2 route responses!
    for (int i=0; i < list.size(); i++) {
      UpdateResponse response = list.get(i).get();
      assertEquals(0, response.getStatus());
      BaseCloudSolrClient.RouteResponse rr = (BaseCloudSolrClient.RouteResponse) response.getResponse();
      assertNotNull(rr);
      NamedList<Object> routeResponses = rr.getRouteResponses();
      assertNotNull(routeResponses);
      assertEquals(2, routeResponses.size());
      assertEquals(2, rr.getRoutes().size());
    }
  }
}
