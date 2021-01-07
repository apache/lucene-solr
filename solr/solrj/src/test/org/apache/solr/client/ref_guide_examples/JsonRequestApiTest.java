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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.json.DomainMap;
import org.apache.solr.client.solrj.request.json.JsonQueryRequest;
import org.apache.solr.client.solrj.request.json.QueryFacetMap;
import org.apache.solr.client.solrj.request.json.RangeFacetMap;
import org.apache.solr.client.solrj.request.json.TermsFacetMap;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.json.BucketJsonFacet;
import org.apache.solr.client.solrj.response.json.NestableJsonFacet;
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

    CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1).setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());

    ContentStreamUpdateRequest up = new ContentStreamUpdateRequest("/update");
    up.setParam("collection", COLLECTION_NAME);
    up.addFile(getFile("solrj/techproducts.xml"), "application/xml");
    up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    UpdateResponse updateResponse = up.process(cluster.getSolrClient());
    assertEquals(0, updateResponse.getStatus());
  }

  @Test
  public void testSimpleJsonQuery() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();
    final int expectedResults = 4;

    // tag::solrj-json-query-simple[]
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("memory")
        .withFilter("inStock:true");
    QueryResponse queryResponse = simpleQuery.process(solrClient, COLLECTION_NAME);
    // end::solrj-json-query-simple[]

    assertResponseFoundNumDocs(queryResponse, expectedResults);
  }

  @Test
  public void testJsonQueryWithJsonQueryParamOverrides() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();
    final int expectedResults = 4;

    // This subtest has its own scope so that it and its twin below can can have identical variable declarations (as they appear as separate snippets in the ref-guide)
    {
      // tag::solrj-json-query-param-overrides[]
      final ModifiableSolrParams overrideParams = new ModifiableSolrParams();
      final JsonQueryRequest queryWithParamOverrides = new JsonQueryRequest(overrideParams)
          .setQuery("memory")
          .setLimit(10)
          .withFilter("inStock:true");
      overrideParams.set("json.limit", 5);
      overrideParams.add("json.filter", "\"cat:electronics\"");
      QueryResponse queryResponse = queryWithParamOverrides.process(solrClient, COLLECTION_NAME);
      // end::solrj-json-query-param-overrides[]

      assertResponseFoundNumDocs(queryResponse, expectedResults);
    }

    // tag::solrj-json-query-param-overrides-equivalent[]
    final JsonQueryRequest query = new JsonQueryRequest()
        .setQuery("memory")
        .setLimit(5)
        .withFilter("inStock:true")
        .withFilter("cat:electronics");
    QueryResponse queryResponse = query.process(solrClient, COLLECTION_NAME);
    // end::solrj-json-query-param-overrides-equivalent[]

    assertResponseFoundNumDocs(queryResponse, expectedResults);
  }

  @Test
  public void testJsonFacetWithAllQueryParams() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    // This subtest has its own scope so that it and its twin below can can have identical variable declarations (as they appear as separate snippets in the ref-guide)
    {
      //tag::solrj-json-facet-all-query-params[]
      final ModifiableSolrParams params = new ModifiableSolrParams();
      final SolrQuery query = new SolrQuery("*:*");
      query.setRows(1);
      query.setParam("json.facet.avg_price", "\"avg(price)\"");
      query.setParam("json.facet.top_cats", "{type:terms,field:\"cat\",limit:3}");
      QueryResponse queryResponse = solrClient.query(COLLECTION_NAME, query);
      //end::solrj-json-facet-all-query-params[]

      NestableJsonFacet topLevelFacet = queryResponse.getJsonFacetingResponse();
      assertResponseFoundNumDocs(queryResponse, 1);
      assertHasFacetWithBucketValues(topLevelFacet, "top_cats",
          new FacetBucket("electronics", 12),
          new FacetBucket("currency", 4),
          new FacetBucket("memory", 3));
    }

    {
      //tag::solrj-json-facet-all-query-params-equivalent[]
      final JsonQueryRequest jsonQueryRequest = new JsonQueryRequest()
          .setQuery("*:*")
          .setLimit(1)
          .withStatFacet("avg_price", "avg(price)")
          .withFacet("top_cats", new TermsFacetMap("cat").setLimit(3));
      QueryResponse queryResponse = jsonQueryRequest.process(solrClient, COLLECTION_NAME);
      //end::solrj-json-facet-all-query-params-equivalent[]

      NestableJsonFacet topLevelFacet = queryResponse.getJsonFacetingResponse();
      assertResponseFoundNumDocs(queryResponse, 1);
      assertHasFacetWithBucketValues(topLevelFacet, "top_cats",
          new FacetBucket("electronics", 12),
          new FacetBucket("currency", 4),
          new FacetBucket("memory", 3));
    }
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

    assertResponseFoundNumDocs(queryResponse, 5);
  }

  @Test
  public void testJsonQueryDslBasicEquivalents() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    {
      //tag::solrj-ipod-query-basic[]
      final SolrQuery query = new SolrQuery("name:iPod");
      final QueryResponse response = solrClient.query(COLLECTION_NAME, query);
      //end::solrj-ipod-query-basic[]

      assertResponseFoundNumDocs(response, 3);
    }

    {
      //tag::solrj-ipod-query-dsl-1[]
      final JsonQueryRequest query = new JsonQueryRequest()
          .setQuery("name:iPod");
      final QueryResponse response = query.process(solrClient, COLLECTION_NAME);
      //end::solrj-ipod-query-dsl-1[]

      assertResponseFoundNumDocs(response, 3);
    }

    {
      //tag::solrj-ipod-query-dsl-2[]
      final JsonQueryRequest query = new JsonQueryRequest()
          .setQuery("{!lucene df=name}iPod");
      final QueryResponse response = query.process(solrClient, COLLECTION_NAME);
      //end::solrj-ipod-query-dsl-2[]

      assertResponseFoundNumDocs(response, 3);
    }

    {
      //tag::solrj-ipod-query-dsl-3[]
      final Map<String, Object> queryTopLevel = new HashMap<>();
      final Map<String, Object> luceneQueryProperties = new HashMap<>();
      queryTopLevel.put("lucene", luceneQueryProperties);
      luceneQueryProperties.put("df", "name");
      luceneQueryProperties.put("query", "iPod");
      final JsonQueryRequest query = new JsonQueryRequest()
          .setQuery(queryTopLevel);
      final QueryResponse response = query.process(solrClient, COLLECTION_NAME);
      //end::solrj-ipod-query-dsl-3[]

      assertResponseFoundNumDocs(response, 3);
    }
  }

  @Test
  public void testJsonQueryDslBoostEquivalents() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();
    QueryResponse[] responses = new QueryResponse[3];

    {
      //tag::solrj-ipod-query-boosted-basic[]
      final SolrQuery query = new SolrQuery("{!boost b=log(popularity) v=\'{!lucene df=name}iPod\'}");
      final QueryResponse response = solrClient.query(COLLECTION_NAME, query);
      //end::solrj-ipod-query-boosted-basic[]

      responses[0] = response;
    }

    {
      //tag::solrj-ipod-query-boosted-dsl-1[]
      final Map<String, Object> queryTopLevel = new HashMap<>();
      final Map<String, Object> boostQuery = new HashMap<>();
      queryTopLevel.put("boost", boostQuery);
      boostQuery.put("b", "log(popularity)");
      boostQuery.put("query", "{!lucene df=name}iPod");
      final JsonQueryRequest query = new JsonQueryRequest()
          .setQuery(queryTopLevel);
      final QueryResponse response = query.process(solrClient, COLLECTION_NAME);
      //end::solrj-ipod-query-boosted-dsl-1[]

      responses[1] = response;
    }

    {
      //tag::solrj-ipod-query-boosted-dsl-2[]
      final Map<String, Object> queryTopLevel = new HashMap<>();
      final Map<String, Object> boostProperties = new HashMap<>();
      final Map<String, Object> luceneTopLevel = new HashMap<>();
      final Map<String, Object> luceneProperties = new HashMap<>();
      queryTopLevel.put("boost", boostProperties);
      boostProperties.put("b", "log(popularity)");
      boostProperties.put("query", luceneTopLevel);
      luceneTopLevel.put("lucene", luceneProperties);
      luceneProperties.put("df", "name");
      luceneProperties.put("query", "iPod");
      final JsonQueryRequest query = new JsonQueryRequest()
          .setQuery(queryTopLevel);
      final QueryResponse response = query.process(solrClient, COLLECTION_NAME);
      //end::solrj-ipod-query-boosted-dsl-2[]

      responses[2] = response;
    }

    for (QueryResponse response : responses) {
      assertResponseFoundNumDocs(response, 3);
      assertEquals("MA147LL/A", response.getResults().get(0).get("id"));
      assertEquals("F8V7067-APL-KIT", response.getResults().get(1).get("id"));
      assertEquals("IW-02", response.getResults().get(2).get("id"));
    }
  }

  @Test
  public void testJsonBooleanQuery() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();
    QueryResponse[] responses = new QueryResponse[3];

    {
      //tag::solrj-ipod-query-bool[]
      final Map<String, Object> queryTopLevel = new HashMap<>();
      final Map<String, Object> boolProperties = new HashMap<>();
      final List<Object> mustClauses = new ArrayList<>();
      final List<Object> mustNotClauses = new ArrayList<>();
      final Map<String, Object> frangeTopLevel = new HashMap<>();
      final Map<String, Object> frangeProperties = new HashMap<>();

      queryTopLevel.put("bool", boolProperties);
      boolProperties.put("must", mustClauses);
      mustClauses.add("name:iPod");

      boolProperties.put("must_not", mustNotClauses);
      frangeTopLevel.put("frange", frangeProperties);
      frangeProperties.put("l", 0);
      frangeProperties.put("u", 5);
      frangeProperties.put("query", "popularity");
      mustNotClauses.add(frangeTopLevel);

      final JsonQueryRequest query = new JsonQueryRequest()
          .setQuery(queryTopLevel);
      final QueryResponse response = query.process(solrClient, COLLECTION_NAME);
      //end::solrj-ipod-query-bool[]

      responses[0] = response;
    }

    {
      //tag::solrj-ipod-query-bool-condensed[]
      final Map<String, Object> queryTopLevel = new HashMap<>();
      final Map<String, Object> boolProperties = new HashMap<>();
      final List<Object> mustClauses = new ArrayList<>();
      final List<Object> mustNotClauses = new ArrayList<>();
      queryTopLevel.put("bool", boolProperties);
      boolProperties.put("must", "name:iPod");
      boolProperties.put("must_not", "{!frange l=0 u=5}popularity");

      final JsonQueryRequest query = new JsonQueryRequest()
          .setQuery(queryTopLevel);
      final QueryResponse response = query.process(solrClient, COLLECTION_NAME);
      //end::solrj-ipod-query-bool-condensed[]

      responses[1] = response;
    }

    {
      //tag::solrj-ipod-query-bool-filter[]
      final Map<String, Object> queryTopLevel = new HashMap<>();
      final Map<String, Object> boolProperties = new HashMap<>();
      queryTopLevel.put("bool", boolProperties);
      boolProperties.put("must_not","{!frange l=0 u=5}popularity");

      final JsonQueryRequest query = new JsonQueryRequest()
          .setQuery(queryTopLevel)
          .withFilter("name:iPod");
      final QueryResponse response = query.process(solrClient, COLLECTION_NAME);
      //end::solrj-ipod-query-bool-filter[]

      responses[2] = response;
    }

    for (QueryResponse response : responses) {
      assertResponseFoundNumDocs(response, 1);
      assertEquals("MA147LL/A", response.getResults().get(0).get("id"));
    }
  }

  @Test
  public void testJsonTaggedQuery() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-tagged-query[]
    final Map<String, Object> titleTaggedQuery = new HashMap<>();
    titleTaggedQuery.put("#titleTag", "name:Solr");
    final Map<String, Object> inStockTaggedQuery = new HashMap<>();
    inStockTaggedQuery.put("#inStockTag", "inStock:true");
    final JsonQueryRequest query = new JsonQueryRequest()
        .setQuery("*:*")
        .withFilter(titleTaggedQuery)
        .withFilter(inStockTaggedQuery);
    final QueryResponse response = query.process(solrClient, COLLECTION_NAME);
    //end::solrj-tagged-query[]

    assertResponseFoundNumDocs(response, 1);
    assertEquals("SOLR1000", response.getResults().get(0).get("id"));
  }

  @Test
  public void testSimpleJsonTermsFacet() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-simple-terms-facet[]
    final TermsFacetMap categoryFacet = new TermsFacetMap("cat").setLimit(3);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("categories", categoryFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-simple-terms-facet[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(32, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetingData,"categories",
        new FacetBucket("electronics",12),
        new FacetBucket("currency", 4),
        new FacetBucket("memory", 3));
  }

  @Test
  public void testTermsFacet2() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-terms-facet-2[]
    final TermsFacetMap categoryFacet = new TermsFacetMap("cat").setLimit(5);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("categories", categoryFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-terms-facet-2[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(32, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetingData,"categories",
        new FacetBucket("electronics",12),
        new FacetBucket("currency", 4),
        new FacetBucket("memory", 3),
        new FacetBucket("connector", 2),
        new FacetBucket("graphics card", 2));
  }

  @Test
  public void testStatFacet1() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-metrics-facet-1[]
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("memory")
        .withFilter("inStock:true")
        .withStatFacet("avg_price", "avg(price)")
        .withStatFacet("min_manufacturedate_dt", "min(manufacturedate_dt)")
        .withStatFacet("num_suppliers", "unique(manu_exact)")
        .withStatFacet("median_weight", "percentile(weight,50)");
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-metrics-facet-1[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(4, queryResponse.getResults().getNumFound());
    assertEquals(4, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertEquals(146.66, (double) topLevelFacetingData.getStatValue("avg_price"), 0.5);
    assertEquals(3, topLevelFacetingData.getStatValue("num_suppliers"));
    assertEquals(352.0, (double) topLevelFacetingData.getStatValue("median_weight"), 0.5);

    Object val = topLevelFacetingData.getStatValue("min_manufacturedate_dt");
    assertTrue(val instanceof Date);
    assertEquals("2006-02-13T15:26:37Z", ((Date)val).toInstant().toString());
  }

  @Test
  public void testStatFacetSimple() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-metrics-facet-simple[]
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFilter("price:[1.0 TO *]")
        .withFilter("popularity:[0 TO 10]")
        .withStatFacet("min_manu_id_s", "min(manu_id_s)")
        .withStatFacet("avg_value", "avg(div(popularity,price))");
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-metrics-facet-simple[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(13, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertEquals(0.036, (double) topLevelFacetingData.getStatValue("avg_value"), 0.1);
    Object val = topLevelFacetingData.getStatValue("min_manu_id_s");
    assertTrue(val instanceof String);
    assertEquals("apple", val.toString());
  }

  @Test
  public void testStatFacetExpanded() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-metrics-facet-expanded[]
    final  Map<String, Object> expandedStatFacet = new HashMap<>();
    expandedStatFacet.put("type", "func");
    expandedStatFacet.put("func", "avg(div($numer,$denom))");
    expandedStatFacet.put("numer", "mul(popularity,3.0)");
    expandedStatFacet.put("denom", "price");
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFilter("price:[1.0 TO *]")
        .withFilter("popularity:[0 TO 10]")
        .withFacet("avg_value", expandedStatFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-metrics-facet-expanded[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(13, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertEquals(0.108, (double) topLevelFacetingData.getStatValue("avg_value"), 0.1);
  }

  @Test
  public void testQueryFacetSimple() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-query-facet-simple[]
    QueryFacetMap queryFacet = new QueryFacetMap("popularity:[8 TO 10]");
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("high_popularity", queryFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-query-facet-simple[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(32, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertEquals(2, topLevelFacetingData.getQueryFacet("high_popularity").getCount());
  }

  @Test
  public void testQueryFacetExpanded() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-query-facet-expanded[]
    QueryFacetMap queryFacet = new QueryFacetMap("popularity:[8 TO 10]")
        .withStatSubFacet("average_price", "avg(price)");
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("high_popularity", queryFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-query-facet-expanded[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(32, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertEquals(2, topLevelFacetingData.getQueryFacet("high_popularity").getCount());
    assertEquals(199.5, topLevelFacetingData.getQueryFacet("high_popularity").getStatValue("average_price"));
  }

  @Test
  public void testRangeFacetSimple() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-range-facet-simple[]
    RangeFacetMap rangeFacet = new RangeFacetMap("price", 0.0, 100.0, 20.0);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("prices", rangeFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-range-facet-simple[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(32, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetingData,"prices",
        new FacetBucket(0.0f,5),
        new FacetBucket(20.0f, 0),
        new FacetBucket(40.0f, 0),
        new FacetBucket(60.0f, 1),
        new FacetBucket(80.0f, 1));
  }

  @Test
  public void testNestedFacetSimple() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-nested-cat-facet[]
    final TermsFacetMap topCategoriesFacet = new TermsFacetMap("cat").setLimit(3);
    final TermsFacetMap topManufacturerFacet = new TermsFacetMap("manu_id_s").setLimit(1);
    topCategoriesFacet.withSubFacet("top_manufacturers", topManufacturerFacet);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("categories", topCategoriesFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-nested-cat-facet[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(32, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();

    assertHasFacetWithBucketValues(topLevelFacetingData, "categories",
        new FacetBucket("electronics", 12),
        new FacetBucket("currency", 4),
        new FacetBucket("memory", 3));

    // Check the top manufacturer for each category
    List<BucketJsonFacet> catBuckets = topLevelFacetingData.getBucketBasedFacets("categories").getBuckets();
    assertHasFacetWithBucketValues(catBuckets.get(0), "top_manufacturers",
        new FacetBucket("corsair", 3));
    assertHasFacetWithBucketValues(catBuckets.get(1), "top_manufacturers",
        new FacetBucket("boa", 1));
    assertHasFacetWithBucketValues(catBuckets.get(2), "top_manufacturers",
        new FacetBucket("corsair", 3));
  }

  @Test
  public void testFacetSortedByNestedMetric() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-nested-cat-facet-sorted[]
    final TermsFacetMap topCategoriesFacet = new TermsFacetMap("cat")
        .setLimit(3)
        .withStatSubFacet("avg_price", "avg(price)")
        .setSort("avg_price desc");
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("categories", topCategoriesFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-nested-cat-facet-sorted[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(32, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetingData, "categories",
        new FacetBucket("electronics and computer1", 1),
        new FacetBucket("graphics card", 2),
        new FacetBucket("music", 1));
  }

  @Test
  public void testFacetFilteredDomain() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-facet-filtered-domain[]
    final TermsFacetMap categoryFacet = new TermsFacetMap("cat")
        .setLimit(3)
        .withDomain(new DomainMap().withFilter("popularity:[5 TO 10]"));
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("categories", categoryFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-facet-filtered-domain[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(32, queryResponse.getResults().getNumFound());
    assertEquals(10, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetingData, "categories",
        new FacetBucket("electronics", 9),
        new FacetBucket("graphics card", 2),
        new FacetBucket("hard drive", 2));
  }

  @Test
  public void testFacetWidenedExcludeTagsDomain() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-facet-excludetags-domain[]
    final TermsFacetMap inStockFacet = new TermsFacetMap("inStock").setLimit(2);
    final TermsFacetMap allManufacturersFacet = new TermsFacetMap("manu_id_s")
        .setLimit(2)
        .withDomain(new DomainMap().withTagsToExclude("MANU"));
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("cat:electronics")
        .withFilter("{!tag=MANU}manu_id_s:apple")
        .withFacet("stock", inStockFacet)
        .withFacet("manufacturers", allManufacturersFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-facet-excludetags-domain[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(1, queryResponse.getResults().getNumFound());
    assertEquals(1, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetingData, "stock",
        new FacetBucket(true, 1));
    assertHasFacetWithBucketValues(topLevelFacetingData, "manufacturers",
        new FacetBucket("corsair", 3),
        new FacetBucket("belkin", 2));
  }

  @Test
  public void testFacetWidenedUsingQueryDomain() throws Exception {
    SolrClient solrClient = cluster.getSolrClient();

    //tag::solrj-json-facet-query-domain[]
    final TermsFacetMap inStockFacet = new TermsFacetMap("inStock").setLimit(2);
    final TermsFacetMap popularCategoriesFacet = new TermsFacetMap("cat")
        .withDomain(new DomainMap().withQuery("popularity:[8 TO 10]"))
        .setLimit(3);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("apple")
        .withFacet("popular_categories", popularCategoriesFacet);
    QueryResponse queryResponse = request.process(solrClient, COLLECTION_NAME);
    //end::solrj-json-facet-query-domain[]

    assertEquals(0, queryResponse.getStatus());
    assertEquals(1, queryResponse.getResults().getNumFound());
    assertEquals(1, queryResponse.getResults().size());
    final NestableJsonFacet topLevelFacetingData = queryResponse.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetingData, "popular_categories",
        new FacetBucket("electronics", 1),
        new FacetBucket("music", 1),
        new FacetBucket("search", 1));
  }

  private class FacetBucket {
    private final Object val;
    private final int count;
    FacetBucket(Object val, int count) {
      this.val = val;
      this.count = count;
    }

    public Object getVal() { return val; }
    public int getCount() { return count; }
  }

  private void assertHasFacetWithBucketValues(NestableJsonFacet response, String expectedFacetName, FacetBucket... expectedBuckets) {
    assertTrue("Expected response to have facet with name " + expectedFacetName,
        response.getBucketBasedFacets(expectedFacetName) != null);
    final List<BucketJsonFacet> buckets = response.getBucketBasedFacets(expectedFacetName).getBuckets();
    assertEquals(expectedBuckets.length, buckets.size());
    for (int i = 0; i < expectedBuckets.length; i++) {
      final FacetBucket expectedBucket = expectedBuckets[i];
      final BucketJsonFacet actualBucket = buckets.get(i);
      assertEquals(expectedBucket.getVal(), actualBucket.getVal());
      assertEquals(expectedBucket.getCount(), actualBucket.getCount());
    }
  }

  private void assertResponseFoundNumDocs(QueryResponse response, int expectedNumDocs) {
    assertEquals(0, response.getStatus());
    assertEquals(expectedNumDocs, response.getResults().size());
  }
}
