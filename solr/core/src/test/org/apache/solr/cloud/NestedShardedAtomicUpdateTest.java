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

package org.apache.solr.cloud;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

public class NestedShardedAtomicUpdateTest extends AbstractFullDistribZkTestBase {

  public NestedShardedAtomicUpdateTest() {
    stress = 0;
    sliceCount = 4;
    schemaString = "schema-nest.xml";
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-tlog.xml";
  }

  @Override
  protected String getCloudSchemaFile() {
    return "schema-nest.xml";
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    boolean testFinished = false;
    try {
      sendWrongRouteParam();
      doNestedInplaceUpdateTest();
      doRootShardRoutingTest();
      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }
    }
  }

  public void doRootShardRoutingTest() throws Exception {
    assertEquals(4, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    final String[] ids = {"3", "4", "5", "6"};

    assertEquals("size of ids to index should be the same as the number of clients", clients.size(), ids.length);
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.

    SolrInputDocument doc = sdoc("id", "1", "level_s", "root");

    final SolrParams params = params("wt", "json", "_route_", "1");

    int which = (params.get("_route_").hashCode() & 0x7fffffff) % clients.size();
    SolrClient aClient = clients.get(which);

    indexDoc(aClient, params, doc);

    doc = sdoc("id", "1", "children", map("add", sdocs(sdoc("id", "2", "level_s", "child"))));

    indexDoc(aClient, params, doc);

    for(int idIndex = 0; idIndex < ids.length; ++idIndex) {

      doc = sdoc("id", "2", "grandChildren", map("add", sdocs(sdoc("id", ids[idIndex], "level_s", "grand_child"))));

      indexDocAndRandomlyCommit(getRandomSolrClient(), params, doc);

      doc = sdoc("id", "3", "inplace_updatable_int", map("inc", "1"));

      indexDocAndRandomlyCommit(getRandomSolrClient(), params, doc);

      // assert RTG request respects _route_ param
      QueryResponse routeRsp = getRandomSolrClient().query(params("qt","/get", "id","2", "_route_", "1"));
      SolrDocument results = (SolrDocument) routeRsp.getResponse().get("doc");
      assertNotNull("RTG should find doc because _route_ was set to the root documents' ID", results);
      assertEquals("2", results.getFieldValue("id"));

      // assert all docs are indexed under the same root
      getRandomSolrClient().commit();
      assertEquals(0, getRandomSolrClient().query(params("q", "-_root_:1")).getResults().size());

      // assert all docs are indexed inside the same block
      QueryResponse rsp = getRandomSolrClient().query(params("qt","/get", "id","1", "fl", "*, [child]"));
      SolrDocument val = (SolrDocument) rsp.getResponse().get("doc");
      assertEquals("1", val.getFieldValue("id"));
      List<SolrDocument> children = (List) val.getFieldValues("children");
      assertEquals(1, children.size());
      SolrDocument childDoc = children.get(0);
      assertEquals("2", childDoc.getFieldValue("id"));
      List<SolrDocument> grandChildren = (List) childDoc.getFieldValues("grandChildren");
      assertEquals(idIndex + 1, grandChildren.size());
      SolrDocument grandChild = grandChildren.get(0);
      assertEquals(idIndex + 1, grandChild.getFirstValue("inplace_updatable_int"));
      assertEquals("3", grandChild.getFieldValue("id"));
    }
  }

  public void doNestedInplaceUpdateTest() throws Exception {
    assertEquals(4, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    final String[] ids = {"3", "4", "5", "6"};

    assertEquals("size of ids to index should be the same as the number of clients", clients.size(), ids.length);
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.

    SolrInputDocument doc = sdoc("id", "1", "level_s", "root");

    final SolrParams params = params("wt", "json", "_route_", "1");

    int which = (params.get("_route_").hashCode() & 0x7fffffff) % clients.size();
    SolrClient aClient = clients.get(which);

    indexDocAndRandomlyCommit(aClient, params, doc);

    doc = sdoc("id", "1", "children", map("add", sdocs(sdoc("id", "2", "level_s", "child"))));

    indexDocAndRandomlyCommit(aClient, params, doc);

    doc = sdoc("id", "2", "grandChildren", map("add", sdocs(sdoc("id", ids[0], "level_s", "grand_child"))));

    indexDocAndRandomlyCommit(aClient, params, doc);

    for (int fieldValue = 1; fieldValue < 5; ++fieldValue) {
      doc = sdoc("id", "3", "inplace_updatable_int", map("inc", "1"));

      indexDocAndRandomlyCommit(getRandomSolrClient(), params, doc);

      // assert RTG request respects _route_ param
      QueryResponse routeRsp = getRandomSolrClient().query(params("qt","/get", "id","2", "_route_", "1"));
      SolrDocument results = (SolrDocument) routeRsp.getResponse().get("doc");
      assertNotNull("RTG should find doc because _route_ was set to the root documents' ID", results);
      assertEquals("2", results.getFieldValue("id"));

      // assert all docs are indexed under the same root
      getRandomSolrClient().commit();
      assertEquals(0, getRandomSolrClient().query(params("q", "-_root_:1")).getResults().size());

      // assert all docs are indexed inside the same block
      QueryResponse rsp = getRandomSolrClient().query(params("qt","/get", "id","1", "fl", "*, [child]"));
      SolrDocument val = (SolrDocument) rsp.getResponse().get("doc");
      assertEquals("1", val.getFieldValue("id"));
      List<SolrDocument> children = (List) val.getFieldValues("children");
      assertEquals(1, children.size());
      SolrDocument childDoc = children.get(0);
      assertEquals("2", childDoc.getFieldValue("id"));
      List<SolrDocument> grandChildren = (List) childDoc.getFieldValues("grandChildren");
      assertEquals(1, grandChildren.size());
      SolrDocument grandChild = grandChildren.get(0);
      assertEquals(fieldValue, grandChild.getFirstValue("inplace_updatable_int"));
      assertEquals("3", grandChild.getFieldValue("id"));
    }
  }

  public void sendWrongRouteParam() throws Exception {
    assertEquals(4, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    final String rootId = "1";

    SolrInputDocument doc = sdoc("id", rootId, "level_s", "root");

    final SolrParams wrongRootParams = params("wt", "json", "_route_", "c");
    final SolrParams rightParams = params("wt", "json", "_route_", rootId);

    int which = (rootId.hashCode() & 0x7fffffff) % clients.size();
    SolrClient aClient = clients.get(which);

    indexDocAndRandomlyCommit(aClient, params("wt", "json", "_route_", rootId), doc, false);

    final SolrInputDocument childDoc = sdoc("id", rootId, "children", map("add", sdocs(sdoc("id", "2", "level_s", "child"))));

    indexDocAndRandomlyCommit(aClient, rightParams, childDoc, false);

    final SolrInputDocument grandChildDoc = sdoc("id", "2", "grandChildren",
        map("add", sdocs(
            sdoc("id", "3", "level_s", "grandChild")
            )
        )
    );

    SolrException e = expectThrows(SolrException.class,
        "wrong \"_route_\" param should throw an exception",
        () -> indexDocAndRandomlyCommit(aClient, wrongRootParams, grandChildDoc)
    );

    assertTrue("message should suggest the wrong \"_route_\" param was supplied",
        e.getMessage().contains("perhaps the wrong \"_route_\" param was supplied"));
  }

  private void indexDocAndRandomlyCommit(SolrClient client, SolrParams params, SolrInputDocument sdoc) throws IOException, SolrServerException {
    indexDocAndRandomlyCommit(client, params, sdoc, true);
  }

  private void indexDocAndRandomlyCommit(SolrClient client, SolrParams params, SolrInputDocument sdoc, boolean compareToControlCollection) throws IOException, SolrServerException {
    if (compareToControlCollection) {
      indexDoc(client, params, sdoc);
    } else {
      add(client, params, sdoc);
    }
    // randomly commit docs
    if (random().nextBoolean()) {
      client.commit();
    }
  }

  private SolrClient getRandomSolrClient() {
    return clients.get(random().nextInt(clients.size()));
  }

}
