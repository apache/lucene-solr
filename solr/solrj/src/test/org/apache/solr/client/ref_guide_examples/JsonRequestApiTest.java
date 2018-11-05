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

package org.apache.solr.client.ref_guide_examples;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.json.JsonQueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example SolrJ usage of the JSON Request API.
 *
 * Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference Guide.
 */
public class JsonRequestApiTest extends SolrCloudTestCase {
  private static final String COLLECTION_NAME = "techproducts";
  private static final String CONFIG_NAME = "techproducts_config";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(CONFIG_NAME, new File(ExternalPaths.TECHPRODUCTS_CONFIGSET).toPath())
        .configure();

    final List<String> solrUrls = new ArrayList<>();
    solrUrls.add(cluster.getJettySolrRunner(0).getBaseUrl().toString());

    CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1).process(cluster.getSolrClient());

    ContentStreamUpdateRequest up = new ContentStreamUpdateRequest("/update");
    up.setParam("collection", COLLECTION_NAME);
    up.addFile(getFile("solrj/docs2.xml"), "application/xml"); // A subset of the 'techproducts' documents
    up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    UpdateResponse updateResponse = up.process(cluster.getSolrClient());
    assertEquals(0, updateResponse.getStatus());
  }

  @Test
  public void testSimpleJsonQuery() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();
    final int expectedResults = 3;

    // tag::solrj-json-query-simple[]
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("memory")
        .withFilter("inStock:true");
    QueryResponse queryResponse = simpleQuery.process(solrClient, COLLECTION_NAME);
    // end::solrj-json-query-simple[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(expectedResults, queryResponse.getResults().size());
  }

  @Test
  public void testJsonQueryUsingParamsBlock() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-query-params-block[]
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("fl", "name", "price");
    final JsonQueryRequest simpleQuery = new JsonQueryRequest(params)
        .withParam("q", "memory")
        .withParam("rows", 1);
    QueryResponse queryResponse = simpleQuery.process(solrClient, COLLECTION_NAME);
    // end::solrj-json-query-params-block[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(1, queryResponse.getResults().size());
    final SolrDocument doc = queryResponse.getResults().get(0);
    final Collection<String> returnedFields = doc.getFieldNames();
    assertEquals(2, doc.getFieldNames().size());
    assertTrue("Expected returned field list to include 'name'", returnedFields.contains("name"));
    assertTrue("Expected returned field list to include 'price'", returnedFields.contains("price"));
  }

  @Test
  public void testJsonQueryMacroExpansion() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-query-macro-expansion[]
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("FIELD", "text");
    params.set("TERM", "memory");
    final JsonQueryRequest simpleQuery = new JsonQueryRequest(params)
        .setQuery("${FIELD}:${TERM}");
    QueryResponse queryResponse = simpleQuery.process(solrClient, COLLECTION_NAME);
    // end::solrj-json-query-macro-expansion[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(3, queryResponse.getResults().size());
  }

}
