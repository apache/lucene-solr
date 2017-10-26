/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.solr.client.ref_guide_examples;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example SolrJ usage.
 *
 * Snippets surrounded by "tag" and "end" comments are extracted and used in the Solr Reference Guide.
 */
public class UsingSolrJRefGuideExamplesTest extends SolrCloudTestCase {

  private static final int NUM_INDEXED_DOCUMENTS = 3;
  private static final int NUM_LIVE_NODES = 1;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    configureCluster(NUM_LIVE_NODES)
        .addConfig("conf", new File(ExternalPaths.TECHPRODUCTS_CONFIGSET).toPath())
        .configure();

    CollectionAdminResponse response = CollectionAdminRequest.createCollection("techproducts", "conf", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final SolrClient client = getSolrClient();

    final List<TechProduct> products = new ArrayList<TechProduct>();
    products.add(new TechProduct("1","Fitbit Alta"));
    products.add(new TechProduct("2", "Sony Walkman"));
    products.add(new TechProduct("3", "Garmin GPS"));

    client.addBeans("techproducts", products);
    client.commit("techproducts");
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();

    final SolrClient client = getSolrClient();
    client.deleteByQuery("techproducts", "*:*");
    client.commit("techproducts");
  }

  @Test
  public void queryWithRawSolrParamsExample() throws Exception {
    // tag::solrj-query-with-raw-solrparams[]
    final SolrClient client = getSolrClient();

    final Map<String, String> queryParamMap = new HashMap<String, String>();
    queryParamMap.put("q", "*:*");
    queryParamMap.put("fl", "id, name");
    MapSolrParams queryParams = new MapSolrParams(queryParamMap);

    final QueryResponse response = client.query("techproducts", queryParams);
    final SolrDocumentList documents = response.getResults();

    assertEquals(NUM_INDEXED_DOCUMENTS, documents.getNumFound());
    for(SolrDocument document : documents) {
      assertTrue(document.getFieldNames().contains("id"));
      assertTrue(document.getFieldNames().contains("name"));
    }
    // end::solrj-query-with-raw-solrparams[]
  }

  @Test
  public void queryWithSolrQueryExample() throws Exception {
    final int numResultsToReturn = 1;
    final SolrClient client = getSolrClient();

    // tag::solrj-query-with-solrquery[]
    final SolrQuery query = new SolrQuery("*:*");
    query.addField("id");
    query.addField("name");
    query.setRows(numResultsToReturn);
    // end::solrj-query-with-solrquery[]

    final QueryResponse response = client.query("techproducts", query);
    final SolrDocumentList documents = response.getResults();

    assertEquals(NUM_INDEXED_DOCUMENTS, documents.getNumFound());
    assertEquals(numResultsToReturn, documents.size());
    for(SolrDocument document : documents) {
      assertTrue(document.getFieldNames().contains("id"));
      assertTrue(document.getFieldNames().contains("name"));
    }
  }

  @Test
  public void indexWithSolrInputDocumentExample() throws Exception {
    // tag::solrj-index-with-raw-solrinputdoc[]
    final SolrClient client = getSolrClient();

    final SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", UUID.randomUUID().toString());
    doc.addField("name", "Amazon Kindle Paperwhite");

    final UpdateResponse updateResponse = client.add("techproducts", doc);
    // Indexed documents must be committed
    client.commit("techproducts");
    // end::solrj-index-with-raw-solrinputdoc[]

    assertNumDocuments(NUM_INDEXED_DOCUMENTS + 1);
  }

  @Test
  public void indexBeanValueTypeExample() throws Exception {
    // tag::solrj-index-bean-value-type[]
    final SolrClient client = getSolrClient();

    final TechProduct kindle = new TechProduct("kindle-id-4", "Amazon Kindle Paperwhite");
    final UpdateResponse response = client.addBean("techproducts", kindle);

    client.commit("techproducts");
    // end::solrj-index-bean-value-type[]

    assertNumDocuments(NUM_INDEXED_DOCUMENTS + 1);
  }

  @Test
  public void queryBeanValueTypeExample() throws Exception {
    // tag::solrj-query-bean-value-type[]
    final SolrClient client = getSolrClient();

    final SolrQuery query = new SolrQuery("*:*");
    query.addField("id");
    query.addField("name");

    final QueryResponse response = client.query("techproducts", query);
    final List<TechProduct> products = response.getBeans(TechProduct.class);
    // end::solrj-query-bean-value-type[]

    assertEquals(NUM_INDEXED_DOCUMENTS, products.size());
    for (TechProduct product : products) {
      assertFalse(product.id.isEmpty());
      assertFalse(product.name.isEmpty());
    }
  }

  @Test
  public void otherSolrApisExample() throws Exception {
    // tag::solrj-other-apis[]
    final SolrClient client = getSolrClient();

    final SolrRequest request = new CollectionAdminRequest.ClusterStatus();

    final NamedList<Object> response = client.request(request);
    final NamedList<Object> cluster = (NamedList<Object>) response.get("cluster");
    final List<String> liveNodes = (List<String>) cluster.get("live_nodes");

    assertEquals(NUM_LIVE_NODES, liveNodes.size());
    // end::solrj-other-apis[]
  }

  private SolrClient getSolrClient() {
    return cluster.getSolrClient();
  }

  private SolrClient getTechProductSolrClient() {
    // tag::solrj-solrclient-timeouts[]
    final String solrUrl = "http://localhost:8983/solr";
    return new HttpSolrClient.Builder(solrUrl)
        .withConnectionTimeout(10000)
        .withSocketTimeout(60000)
        .build();
    // end::solrj-solrclient-timeouts[]
  }

  private void assertNumDocuments(int expectedNumResults) throws Exception {
    final QueryResponse queryResponse = getSolrClient().query("techproducts", new SolrQuery("*:*"));
    assertEquals(expectedNumResults, queryResponse.getResults().getNumFound());
  }

  // tag::solrj-techproduct-value-type[]
  public static class TechProduct {
    @Field public String id;
    @Field public String name;

    public TechProduct(String id, String name) {
      this.id = id;  this.name = name;
    }

    public TechProduct() {}
  }
  // end::solrj-techproduct-value-type[]

}
