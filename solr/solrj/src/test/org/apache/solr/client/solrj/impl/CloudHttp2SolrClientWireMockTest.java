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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.util.NamedList;
import org.junit.Ignore;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

public class CloudHttp2SolrClientWireMockTest extends BaseSolrClientWireMockTest {

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
    buildUpdateRequest(40).process(testClient, BUILT_IN_MOCK_COLLECTION);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  @Ignore // nocommit ~ TJP WIP fails b/c some responses don't have both route responses
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
    executorService.shutdown();
    executorService.awaitTermination(3, TimeUnit.SECONDS);

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
