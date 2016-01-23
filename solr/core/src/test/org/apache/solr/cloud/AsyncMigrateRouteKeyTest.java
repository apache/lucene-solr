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
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import java.io.IOException;

public class AsyncMigrateRouteKeyTest extends MigrateRouteKeyTest {

  public AsyncMigrateRouteKeyTest() {
    schemaString = "schema15.xml";      // we need a string id
  }

  private static final int MAX_WAIT_SECONDS = 2 * 60;

  @Test
  public void test() throws Exception {
    waitForThingsToLevelOut(15);

    multipleShardMigrateTest();
    printLayout();
  }

  protected void checkAsyncRequestForCompletion(String asyncId) throws SolrServerException, IOException {
    ModifiableSolrParams params;
    String message;
    params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.REQUESTSTATUS.toString());
    params.set(OverseerCollectionMessageHandler.REQUESTID, asyncId);
    // This task takes long enough to run. Also check for the current state of the task to be running.
    message = sendStatusRequestWithRetry(params, 5);
    assertEquals("found [" + asyncId + "] in running tasks", message);
    // Now wait until the task actually completes successfully/fails.
    message = sendStatusRequestWithRetry(params, MAX_WAIT_SECONDS);
    assertEquals("Task " + asyncId + " not found in completed tasks.", 
        "found [" + asyncId + "] in completed tasks", message);
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
    params.set(CommonAdminParams.ASYNC, asyncId);

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
    RequestStatusState state = null;
    String message = null;
    NamedList r;
    while (maxCounter-- > 0) {
      r = sendRequest(params);
      status = (NamedList) r.get("status");
      state = RequestStatusState.fromKey((String) status.get("state"));
      message = (String) status.get("msg");

      if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
        return (String) status.get("msg");
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {

      }

    }
    // Return last state?
    return message;
  }

  protected NamedList sendRequest(ModifiableSolrParams params) throws SolrServerException, IOException {
    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrClient) shardToJetty.get(SHARD1).get(0).client.solrClient).getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    try (HttpSolrClient baseServer = new HttpSolrClient(baseUrl)) {
      baseServer.setConnectionTimeout(15000);
      return baseServer.request(request);
    }
  }
}
