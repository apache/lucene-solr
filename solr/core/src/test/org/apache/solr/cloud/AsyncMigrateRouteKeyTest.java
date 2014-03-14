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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;

import java.io.IOException;

public class AsyncMigrateRouteKeyTest extends MigrateRouteKeyTest {

  public AsyncMigrateRouteKeyTest() {
    schemaString = "schema15.xml";      // we need a string id
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  public void doTest() throws Exception {
    waitForThingsToLevelOut(15);

    multipleShardMigrateTest();
    printLayout();
  }

  protected void checkAsyncRequestForCompletion(String asyncId) throws SolrServerException, IOException {
    ModifiableSolrParams params;
    String message;
    params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.REQUESTSTATUS.toString());
    params.set(OverseerCollectionProcessor.REQUESTID, asyncId);
    message = sendStatusRequestWithRetry(params, 10);
    assertEquals("Task " + asyncId + " not found in completed tasks.",
        "found " + asyncId + " in completed tasks", message);
  }

  @Override
  protected void invokeMigrateApi(String sourceCollection, String splitKey, String targetCollection) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    String asyncId = "20140128";
    params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.MIGRATE.toString());
    params.set("collection", sourceCollection);
    params.set("target.collection", targetCollection);
    params.set("split.key", splitKey);
    params.set("forward.timeout", 45);
    params.set("async", asyncId);

    invoke(params);

    checkAsyncRequestForCompletion(asyncId);
  }

  /**
   * Helper method to send a status request with specific retry limit and return
   * the message/null from the success response.
   */
  private String sendStatusRequestWithRetry(ModifiableSolrParams params, int maxCounter)
      throws SolrServerException, IOException {
    NamedList status = null;
    String state = null;
    String message = null;
    NamedList r;
    while (maxCounter-- > 0) {
      r = sendRequest(params);
      status = (NamedList) r.get("status");
      state = (String) status.get("state");
      message = (String) status.get("msg");

      if (state.equals("completed") || state.equals("failed"))
        return (String) status.get("msg");

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {

      }

    }
    // Return last state?
    return message;
  }

  protected NamedList sendRequest(ModifiableSolrParams params) throws SolrServerException, IOException {
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrServer) shardToJetty.get(SHARD1).get(0).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    HttpSolrServer baseServer = null;

    try {
      baseServer = new HttpSolrServer(baseUrl);
      baseServer.setConnectionTimeout(15000);
      return baseServer.request(request);
    } finally {
      baseServer.shutdown();
    }
  }
}
