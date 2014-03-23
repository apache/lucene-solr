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
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class OverseerStatusTest extends BasicDistributedZkTest {

  public OverseerStatusTest() {
    schemaString = "schema15.xml";      // we need a string id
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (VERBOSE || printLayoutOnTearDown) {
      super.printLayout();
    }
    if (controlClient != null) {
      controlClient.shutdown();
    }
    if (cloudClient != null) {
      cloudClient.shutdown();
    }
    if (controlClientCloud != null) {
      controlClientCloud.shutdown();
    }
    super.tearDown();
  }

  @Override
  public void doTest() throws Exception {
    waitForThingsToLevelOut(15);

    String collectionName = "overseer_status_test";
    CollectionAdminResponse response = createCollection(collectionName, 1, 1, 1);
    NamedList<Object> resp = invokeCollectionApi("action",
        CollectionParams.CollectionAction.OVERSEERSTATUS.toLower());
    NamedList<Object> collection_operations = (NamedList<Object>) resp.get("collection_operations");
    NamedList<Object> overseer_operations = (NamedList<Object>) resp.get("overseer_operations");
    SimpleOrderedMap<Object> createcollection = (SimpleOrderedMap<Object>) collection_operations.get(OverseerCollectionProcessor.CREATECOLLECTION);
    assertEquals("No stats for createcollection in OverseerCollectionProcessor", 1, createcollection.get("requests"));
    createcollection = (SimpleOrderedMap<Object>) overseer_operations.get("createcollection");
    assertEquals("No stats for createcollection in Overseer", 1, createcollection.get("requests"));

    invokeCollectionApi("action", CollectionParams.CollectionAction.RELOAD.toLower(), "name", collectionName);
    resp = invokeCollectionApi("action",
        CollectionParams.CollectionAction.OVERSEERSTATUS.toLower());
    collection_operations = (NamedList<Object>) resp.get("collection_operations");
    SimpleOrderedMap<Object> reload = (SimpleOrderedMap<Object>) collection_operations.get(OverseerCollectionProcessor.RELOADCOLLECTION);
    assertEquals("No stats for reload in OverseerCollectionProcessor", 1, reload.get("requests"));

    try {
      invokeCollectionApi("action", CollectionParams.CollectionAction.SPLITSHARD.toLower(),
          "collection", "non_existent_collection",
          "shard", "non_existent_shard");
    } catch (Exception e) {
      // expected because we did not correctly specify required params for split
    }
    resp = invokeCollectionApi("action",
        CollectionParams.CollectionAction.OVERSEERSTATUS.toLower());
    collection_operations = (NamedList<Object>) resp.get("collection_operations");
    SimpleOrderedMap<Object> split = (SimpleOrderedMap<Object>) collection_operations.get(OverseerCollectionProcessor.SPLITSHARD);
    assertEquals("No stats for split in OverseerCollectionProcessor", 1, split.get("errors"));
    assertNotNull(split.get("recent_failures"));

    waitForThingsToLevelOut(15);
  }

  private NamedList<Object> invokeCollectionApi(String... args) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrRequest request = new QueryRequest(params);
    for (int i = 0; i < args.length - 1; i+=2) {
      params.add(args[i], args[i+1]);
    }
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrServer) shardToJetty.get(SHARD1).get(0).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
    baseServer.setConnectionTimeout(15000);
    baseServer.setSoTimeout(60000 * 5);
    return baseServer.request(request);
  }
}
