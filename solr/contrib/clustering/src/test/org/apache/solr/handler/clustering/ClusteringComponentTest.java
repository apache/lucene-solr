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
package org.apache.solr.handler.clustering;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.QueryResult;
import org.junit.Before;
import org.junit.Test;

/**
 *
 *
 **/
public class ClusteringComponentTest extends AbstractClusteringTestCase {

  @Before
  public void doBefore() {
    clearIndex();
  }

  @Test
  public void testComponent() throws Exception {
    SolrCore core = h.getCore();

    SearchComponent sc = core.getSearchComponent("clustering");
    assertTrue("sc is null and it shouldn't be", sc != null);
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add(ClusteringComponent.COMPONENT_NAME, "true");
    params.add(CommonParams.Q, "*:*");

    params.add(ClusteringParams.USE_SEARCH_RESULTS, "true");


    SolrRequestHandler handler = core.getRequestHandler("/select");
    SolrQueryResponse rsp;
    rsp = new SolrQueryResponse();
    rsp.addResponseHeader(new SimpleOrderedMap<>());
    SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
    handler.handleRequest(req, rsp);
    NamedList<?> values = rsp.getValues();
    Object clusters = values.get("clusters");
    //System.out.println("Clusters: " + clusters);
    assertTrue("clusters is null and it shouldn't be", clusters != null);
    req.close();

    params = new ModifiableSolrParams();
    params.add(ClusteringComponent.COMPONENT_NAME, "true");
    params.add(ClusteringParams.ENGINE_NAME, "mock");
    params.add(ClusteringParams.USE_COLLECTION, "true");
    params.add(QueryComponent.COMPONENT_NAME, "false");

    handler = core.getRequestHandler("docClustering");

    rsp = new SolrQueryResponse();
    rsp.addResponseHeader(new SimpleOrderedMap<>());
    req = new LocalSolrQueryRequest(core, params);
    handler.handleRequest(req, rsp);
    values = rsp.getValues();
    clusters = values.get("clusters");
    //System.out.println("Clusters: " + clusters);
    assertTrue("clusters is null and it shouldn't be", clusters != null);
    req.close();
  }


  // tests ClusteringComponent.docListToSolrDocumentList
  @Test
  public void testDocListConversion() throws Exception {
    assertU("", adoc("id", "3234", "url", "ignoreme", "val_i", "1",
        "val_dynamic", "quick red fox"));
    assertU("", adoc("id", "3235", "url", "ignoreme", "val_i", "1",
        "val_dynamic", "quick green fox"));
    assertU("", adoc("id", "3236", "url", "ignoreme", "val_i", "1",
        "val_dynamic", "quick brown fox"));
    assertU("", commit());

    h.getCore().withSearcher(srchr -> {
      QueryResult qr = new QueryResult();
      QueryCommand cmd = new QueryCommand();
      cmd.setQuery(new MatchAllDocsQuery());
      cmd.setLen(10);
      qr = srchr.search(qr, cmd);

      DocList docs = qr.getDocList();
      assertEquals("wrong docs size", 3, docs.size());
      Set<String> fields = new HashSet<>();
      fields.add("val_dynamic");
      fields.add("dynamic_val");
      fields.add("range_facet_l"); // copied from id

      SolrDocumentList list = ClusteringComponent.docListToSolrDocumentList(docs, srchr, fields, null);
      assertEquals("wrong list Size", docs.size(), list.size());
      for (SolrDocument document : list) {

        assertTrue("unexpected field", ! document.containsKey("val_i"));
        assertTrue("unexpected id field", ! document.containsKey("id"));

        assertTrue("original field", document.containsKey("val_dynamic"));
        assertTrue("dyn copy field", document.containsKey("dynamic_val"));
        assertTrue("copy field", document.containsKey("range_facet_l"));

        assertNotNull("original field null", document.get("val_dynamic"));
        assertNotNull("dyn copy field null", document.get("dynamic_val"));
        assertNotNull("copy field null", document.get("range_facet_l"));
      }
      return null;
    });
  }

}
