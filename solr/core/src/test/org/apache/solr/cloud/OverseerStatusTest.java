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

    // find existing command counts because collection may be created by base test class too
    int numCollectionCreates = 0, numOverseerCreates = 0;
    NamedList<Object> resp = invokeCollectionApi("action",
        CollectionParams.CollectionAction.OVERSEERSTATUS.toLower());
    if (resp != null) {
      NamedList<Object> collection_operations = (NamedList<Object>) resp.get("collection_operations");
      if (collection_operations != null)  {
        SimpleOrderedMap<Object> createcollection = (SimpleOrderedMap<Object>) collection_operations.get(CollectionParams.CollectionAction.CREATE.toLower());
        if (createcollection != null && createcollection.get("requests") != null) {
          numCollectionCreates = (Integer) createcollection.get("requests");
        }
        NamedList<Object> overseer_operations = (NamedList<Object>) resp.get("overseer_operations");
        if (overseer_operations != null)  {
          createcollection = (SimpleOrderedMap<Object>) overseer_operations.get(CollectionParams.CollectionAction.CREATE.toLower());
          if (createcollection != null && createcollection.get("requests") != null) {
            numOverseerCreates = (Integer) createcollection.get("requests");
          }
        }
      }
    }

    String collectionName = "overseer_status_test";
    CollectionAdminResponse response = createCollection(collectionName, 1, 1, 1);
    resp = invokeCollectionApi("action",
        CollectionParams.CollectionAction.OVERSEERSTATUS.toLower());
    NamedList<Object> collection_operations = (NamedList<Object>) resp.get("collection_operations");
    NamedList<Object> overseer_operations = (NamedList<Object>) resp.get("overseer_operations");
    SimpleOrderedMap<Object> createcollection = (SimpleOrderedMap<Object>) collection_operations.get(CollectionParams.CollectionAction.CREATE.toLower());
    assertEquals("No stats for create in OverseerCollectionProcessor", numCollectionCreates + 1, createcollection.get("requests"));
    createcollection = (SimpleOrderedMap<Object>) overseer_operations.get(CollectionParams.CollectionAction.CREATE.toLower());
    assertEquals("No stats for create in Overseer", numOverseerCreates + 1, createcollection.get("requests"));

    invokeCollectionApi("action", CollectionParams.CollectionAction.RELOAD.toLower(), "name", collectionName);
    resp = invokeCollectionApi("action",
        CollectionParams.CollectionAction.OVERSEERSTATUS.toLower());
    collection_operations = (NamedList<Object>) resp.get("collection_operations");
    SimpleOrderedMap<Object> reload = (SimpleOrderedMap<Object>) collection_operations.get(CollectionParams.CollectionAction.RELOAD.toLower());
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
    SimpleOrderedMap<Object> split = (SimpleOrderedMap<Object>) collection_operations.get(CollectionParams.CollectionAction.SPLITSHARD.toLower());
    assertEquals("No stats for split in OverseerCollectionProcessor", 1, split.get("errors"));
    assertNotNull(split.get("recent_failures"));

    SimpleOrderedMap<Object> amIleader = (SimpleOrderedMap<Object>) collection_operations.get("am_i_leader");
    assertNotNull("OverseerCollectionProcessor amILeader stats should not be null", amIleader);
    assertNotNull(amIleader.get("requests"));
    assertTrue(Integer.parseInt(amIleader.get("requests").toString()) > 0);
    assertNotNull(amIleader.get("errors"));
    assertNotNull(amIleader.get("avgTimePerRequest"));

    amIleader = (SimpleOrderedMap<Object>) overseer_operations.get("am_i_leader");
    assertNotNull("Overseer amILeader stats should not be null", amIleader);
    assertNotNull(amIleader.get("requests"));
    assertTrue(Integer.parseInt(amIleader.get("requests").toString()) > 0);
    assertNotNull(amIleader.get("errors"));
    assertNotNull(amIleader.get("avgTimePerRequest"));

    SimpleOrderedMap<Object> updateState = (SimpleOrderedMap<Object>) overseer_operations.get("update_state");
    assertNotNull("Overseer update_state stats should not be null", updateState);
    assertNotNull(updateState.get("requests"));
    assertTrue(Integer.parseInt(updateState.get("requests").toString()) > 0);
    assertNotNull(updateState.get("errors"));
    assertNotNull(updateState.get("avgTimePerRequest"));

    waitForThingsToLevelOut(15);
  }
}
