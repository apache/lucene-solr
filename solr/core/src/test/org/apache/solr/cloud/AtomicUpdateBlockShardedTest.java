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

import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

public class AtomicUpdateBlockShardedTest extends AbstractFullDistribZkTestBase {

  public AtomicUpdateBlockShardedTest() {
    stress = 0;
    sliceCount = 4;
    schemaString = "sharded-block-schema.xml";
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-block-atomic-update.xml";
  }

  @Override
  protected String getCloudSchemaFile() {
    return "sharded-block-schema.xml";
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    boolean testFinished = false;
    try {
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
    final String[] ids = {"c", "d", "e", "f"};

    assertEquals("size of ids to index should be the same as the number of clients", clients.size(), ids.length);
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.

    SolrInputDocument doc = sdoc("id", "a", "level_s", "root");

    final SolrParams params = new ModifiableSolrParams().set("update.chain", "nested-rtg").set("wt", "json").set("_route_", "a");

    int which = (params.get("_route_").hashCode() & 0x7fffffff) % clients.size();
    SolrClient aClient = clients.get(which);

    indexDoc(aClient, params, doc);

    aClient.commit();

    doc = sdoc("id", "a", "children", map("add", sdocs(sdoc("id", "b", "level_s", "child"))));

    indexDoc(aClient, params, doc);
    aClient.commit();

    int i = 0;
    for (SolrClient client : clients) {

      doc = sdoc("id", "b", "grandChildren", map("add", sdocs(sdoc("id", ids[i], "level_s", "grand_child"))));

      indexDoc(client, params, doc);
      client.commit();

      doc = sdoc("id", "c", "inplace_updatable_int", map("inc", "1"));

      indexDoc(client, params, doc);
      client.commit();

      // assert RTG request respects _route_ param
      QueryResponse routeRsp = client.query(params("qt","/get", "id","b", "_route_", "a"));
      SolrDocument results = (SolrDocument) routeRsp.getResponse().get("doc");
      assertNotNull("RTG should find doc because _route_ was set to the root documents' ID", results);
      assertEquals("b", results.getFieldValue("id"));

      // assert all docs are indexed under the same root
      assertEquals(0, client.query(new ModifiableSolrParams().set("q", "-_root_:a")).getResults().size());

      // assert all docs are indexed inside the same block
      QueryResponse rsp = client.query(params("qt","/get", "id","a", "fl", "*, [child]"));
      SolrDocument val = (SolrDocument) rsp.getResponse().get("doc");
      assertEquals("a", val.getFieldValue("id"));
      List<SolrDocument> children = (List) val.getFieldValues("children");
      assertEquals(1, children.size());
      SolrDocument childDoc = children.get(0);
      assertEquals("b", childDoc.getFieldValue("id"));
      List<SolrDocument> grandChildren = (List) childDoc.getFieldValues("grandChildren");
      assertEquals(++i, grandChildren.size());
      SolrDocument grandChild = grandChildren.get(0);
      assertEquals(i, grandChild.getFirstValue("inplace_updatable_int"));
      assertEquals("c", grandChild.getFieldValue("id"));
    }
  }

  public void doNestedInplaceUpdateTest() throws Exception {
    assertEquals(4, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    final String[] ids = {"c", "d", "e", "f"};

    assertEquals("size of ids to index should be the same as the number of clients", clients.size(), ids.length);
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.

    SolrInputDocument doc = sdoc("id", "a", "level_s", "root");

    final SolrParams params = new ModifiableSolrParams().set("update.chain", "nested-rtg").set("wt", "json").set("_route_", "a");

    int which = (params.get("_route_").hashCode() & 0x7fffffff) % clients.size();
    SolrClient aClient = clients.get(which);

    indexDoc(aClient, params, doc);

    aClient.commit();

    doc = sdoc("id", "a", "children", map("add", sdocs(sdoc("id", "b", "level_s", "child"))));

    indexDoc(aClient, params, doc);
    aClient.commit();

    doc = sdoc("id", "b", "grandChildren", map("add", sdocs(sdoc("id", ids[0], "level_s", "grand_child"))));

    indexDoc(aClient, params, doc);
    aClient.commit();

    int i = 0;
    for (SolrClient client : clients) {

      doc = sdoc("id", "c", "inplace_updatable_int", map("inc", "1"));

      indexDoc(client, params, doc);
      client.commit();

      // assert RTG request respects _route_ param
      QueryResponse routeRsp = client.query(params("qt","/get", "id","b", "_route_", "a"));
      SolrDocument results = (SolrDocument) routeRsp.getResponse().get("doc");
      assertNotNull("RTG should find doc because _route_ was set to the root documents' ID", results);
      assertEquals("b", results.getFieldValue("id"));

      // assert all docs are indexed under the same root
      assertEquals(0, client.query(new ModifiableSolrParams().set("q", "-_root_:a")).getResults().size());

      // assert all docs are indexed inside the same block
      QueryResponse rsp = client.query(params("qt","/get", "id","a", "fl", "*, [child]"));
      SolrDocument val = (SolrDocument) rsp.getResponse().get("doc");
      assertEquals("a", val.getFieldValue("id"));
      List<SolrDocument> children = (List) val.getFieldValues("children");
      assertEquals(1, children.size());
      SolrDocument childDoc = children.get(0);
      assertEquals("b", childDoc.getFieldValue("id"));
      List<SolrDocument> grandChildren = (List) childDoc.getFieldValues("grandChildren");
      assertEquals(1, grandChildren.size());
      SolrDocument grandChild = grandChildren.get(0);
      assertEquals(++i, grandChild.getFirstValue("inplace_updatable_int"));
      assertEquals("c", grandChild.getFieldValue("id"));
    }
  }

}
