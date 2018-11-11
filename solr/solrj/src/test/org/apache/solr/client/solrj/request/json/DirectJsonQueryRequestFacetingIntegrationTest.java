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

package org.apache.solr.client.solrj.request.json;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

public class DirectJsonQueryRequestFacetingIntegrationTest extends SolrCloudTestCase {

  private static final String COLLECTION_NAME = "techproducts";
  private static final String CONFIG_NAME = "techproducts_config";
  private static final int NUM_TECHPRODUCTS_DOCS = 32;
  private static final int NUM_IN_STOCK = 17;
  private static final int NUM_ELECTRONICS = 12;
  private static final int NUM_CURRENCY = 4;
  private static final int NUM_MEMORY = 3;
  private static final int NUM_CORSAIR = 3;
  private static final int NUM_BELKIN = 2;
  private static final int NUM_CANON = 2;



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
    up.addFile(getFile("solrj/techproducts.xml"), "application/xml");
    up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    UpdateResponse updateResponse = up.process(cluster.getSolrClient());
    assertEquals(0, updateResponse.getStatus());
  }
  @Test
  public void testSingleTermsFacet() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'top_cats': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 3",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();
    assertHasFacetWithBucketValues(rawResponse,"top_cats", new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY), new FacetBucket("memory", NUM_MEMORY));
  }

  @Test
  public void testMultiTermsFacet() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'top_cats': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 3",
        "    },",
        "    'top_manufacturers': {",
        "      'type': 'terms',",
        "      'field': 'manu_id_s',",
        "      'limit': 3",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    assertHasFacetWithBucketValues(rawResponse,"top_cats", new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY), new FacetBucket("memory", NUM_MEMORY));
    assertHasFacetWithBucketValues(rawResponse,"top_manufacturers", new FacetBucket("corsair",NUM_CORSAIR),
        new FacetBucket("belkin", NUM_BELKIN), new FacetBucket("canon", NUM_CANON));
  }

  @Test
  public void testSingleRangeFacet() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'prices': {",
        "      'type': 'range',",
        "      'field': 'price',",
        "      'start': 0,",
        "      'end': 100,",
        "      'gap': 20",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();
    assertHasFacetWithBucketValues(rawResponse,"prices",
        new FacetBucket(0.0f, 5),
        new FacetBucket(20.0f, 0),
        new FacetBucket(40.0f, 0),
        new FacetBucket(60.0f, 1),
        new FacetBucket(80.0f, 1));
  }

  @Test
  public void testMultiRangeFacet() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'prices': {",
        "      'type': 'range',",
        "      'field': 'price',",
        "      'start': 0,",
        "      'end': 100,",
        "      'gap': 20",
        "    },",
        "    'shipping_weights': {",
        "      'type': 'range',",
        "      'field': 'weight',",
        "      'start': 0,",
        "      'end': 200,",
        "      'gap': 50",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();
    assertHasFacetWithBucketValues(rawResponse,"prices",
        new FacetBucket(0.0f, 5),
        new FacetBucket(20.0f, 0),
        new FacetBucket(40.0f, 0),
        new FacetBucket(60.0f, 1),
        new FacetBucket(80.0f, 1));
    assertHasFacetWithBucketValues(rawResponse, "shipping_weights",
        new FacetBucket(0.0f, 6),
        new FacetBucket(50.0f, 0),
        new FacetBucket(100.0f, 0),
        new FacetBucket(150.0f,1));
  }

  @Test
  public void testSingleStatFacet() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'sum_price': 'sum(price)'",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();
    assertHasStatFacetWithValue(rawResponse,"sum_price", 5251.270030975342);
  }

  @Test
  public void testMultiStatFacet() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'sum_price': 'sum(price)',",
        "    'avg_price': 'avg(price)'",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();
    assertHasStatFacetWithValue(rawResponse,"sum_price", 5251.270030975342);
    assertHasStatFacetWithValue(rawResponse,"avg_price", 328.20437693595886);
  }

  @Test
  public void testMultiFacetsMixedTypes() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'avg_price': 'avg(price)',",
        "    'top_cats': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 3",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();
    assertHasStatFacetWithValue(rawResponse,"avg_price", 328.20437693595886);
    assertHasFacetWithBucketValues(rawResponse,"top_cats", new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY), new FacetBucket("memory", NUM_MEMORY));
  }

  @Test
  public void testNestedTermsFacet() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'top_cats': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 3",
        "      'facet': {",
        "        'top_manufacturers_for_cat': {",
        "          'type': 'terms',",
        "          'field': 'manu_id_s',",
        "          'limit': 1",
        "        }",
        "      }",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    // Test top level facets
    assertHasFacetWithBucketValues(rawResponse,"top_cats", new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY), new FacetBucket("memory", NUM_MEMORY));

    // Test subfacet values for each top-level facet bucket
    final List<NamedList<Object>> topLevelFacetResponse = (List<NamedList<Object>>) rawResponse.findRecursive("facets", "top_cats", "buckets");
    final NamedList<Object> electronicsSubFacet = topLevelFacetResponse.get(0);
    assertFacetResponseHasFacetWithBuckets(electronicsSubFacet, "top_manufacturers_for_cat", new FacetBucket("corsair", 3));
    final NamedList<Object> currencySubfacet = topLevelFacetResponse.get(1);
    assertFacetResponseHasFacetWithBuckets(currencySubfacet, "top_manufacturers_for_cat", new FacetBucket("boa", 1));
    final NamedList<Object> memorySubfacet = topLevelFacetResponse.get(2);
    assertFacetResponseHasFacetWithBuckets(memorySubfacet, "top_manufacturers_for_cat", new FacetBucket("corsair", 3));
  }

  @Test
  public void testNestedFacetsOfMixedTypes() throws Exception {
    final String subfacetName = "avg_price_for_cat";
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'top_cats': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 3",
        "      'facet': {",
        "        'avg_price_for_cat': 'avg(price)'",
        "      }",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    // Test top level facets
    assertHasFacetWithBucketValues(rawResponse,"top_cats", new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY), new FacetBucket("memory", NUM_MEMORY));

    // Test subfacet values for each top-level facet bucket
    final List<NamedList<Object>> topLevelFacetResponse = (List<NamedList<Object>>) rawResponse.findRecursive("facets", "top_cats", "buckets");
    final NamedList<Object> electronicsSubFacet = topLevelFacetResponse.get(0);
    assertFacetResponseHasStatFacetWithValue(electronicsSubFacet, subfacetName, 252.02909261530095);
    final NamedList<Object> currencySubfacet = topLevelFacetResponse.get(1);
    assertFacetResponseHasStatFacetWithValue(currencySubfacet, subfacetName, 0.0);
    final NamedList<Object> memorySubfacet = topLevelFacetResponse.get(2);
    assertFacetResponseHasStatFacetWithValue(memorySubfacet, subfacetName, 129.99499893188477);
  }

  @Test
  public void testFacetWithDomainFilteredBySimpleQueryString() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'top_popular_cats': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 3",
        "      'domain': {",
        "        'filter': 'popularity:[5 TO 10]'",
        "      }",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    // Test top level facets
    assertHasFacetWithBucketValues(rawResponse,"top_popular_cats", new FacetBucket("electronics",9),
        new FacetBucket("graphics card", 2), new FacetBucket("hard drive", 2));
  }

  @Test
  public void testFacetWithDomainFilteredByLocalParamsQueryString() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'facet': {",
        "    'top_popular_cats': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 3",
        "      'domain': {",
        "        'filter': '{!lucene df=\"popularity\" v=\"[5 TO 10]\"}'",
        "      }",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_TECHPRODUCTS_DOCS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    // Test top level facets
    assertHasFacetWithBucketValues(rawResponse,"top_popular_cats", new FacetBucket("electronics",9),
        new FacetBucket("graphics card", 2), new FacetBucket("hard drive", 2));
  }

  @Test
  public void testFacetWithArbitraryDomainFromQueryString() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': 'cat:electronics',",
        "  'facet': {",
        "    'top_cats': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 3",
        "      'domain': {",
        "        'query': '*:*'",
        "      }",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_ELECTRONICS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    // Test top level facets
    assertHasFacetWithBucketValues(rawResponse,"top_cats", new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY), new FacetBucket("memory", NUM_MEMORY));
  }

  @Test
  public void testFacetWithArbitraryDomainFromLocalParamsQuery() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': 'cat:electronics',",
        "  'facet': {",
        "    'largest_search_cats': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'domain': {",
        "        'query': '{!lucene df=\"cat\" v=\"search\"}'",
        "      }",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_ELECTRONICS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    assertHasFacetWithBucketValues(rawResponse,"largest_search_cats",
        new FacetBucket("search",2),
        new FacetBucket("software", 2));
  }

  /*
   * Multiple query clauses are effectively AND'd together
   */
  public void testFacetWithMultipleSimpleQueryClausesInArbitraryDomain() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': 'cat:electronics',",
        "  'facet': {",
        "    'cats_matching_solr': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'domain': {",
        "        'query': ['cat:search', 'name:Solr']",
        "      }",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_ELECTRONICS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    assertHasFacetWithBucketValues(rawResponse,"cats_matching_solr",
        new FacetBucket("search",1),
        new FacetBucket("software", 1));
  }

  public void testFacetWithMultipleLocalParamsQueryClausesInArbitraryDomain() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': 'cat:electronics',",
        "  'facet': {",
        "    'cats_matching_solr': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'domain': {",
        "        'query': ['{!lucene df=\"cat\" v=\"search\"}', '{!lucene df=\"name\" v=\"Solr\"}']",
        "      }",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_ELECTRONICS, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    // Test top level facets
    assertHasFacetWithBucketValues(rawResponse,"cats_matching_solr",
        new FacetBucket("search",1),
        new FacetBucket("software", 1));
  }

  @Test
  public void testFacetWithDomainWidenedUsingExcludeTagsToIgnoreFilters() throws Exception {
    final String jsonBody = String.join("\n","{",
        "  'query': '*:*',",
        "  'filter': {'#on_shelf': 'inStock:true'},",
        "  'facet': {",
        "    'in_stock_only': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 2",
        "    }",
        "    'all': {",
        "      'type': 'terms',",
        "      'field': 'cat',",
        "      'limit': 2,",
        "      'domain': {",
        "        'excludeTags': 'on_shelf'",
        "      }",
        "    }",
        "  }",
        "}");
    final DirectJsonQueryRequest request = new DirectJsonQueryRequest(jsonBody);
    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, response.getStatus());
    final SolrDocumentList returnedDocs = response.getResults();
    assertEquals(NUM_IN_STOCK, returnedDocs.getNumFound());
    assertEquals(10, returnedDocs.size());
    final NamedList<Object> rawResponse = response.getResponse();

    assertHasFacetWithBucketValues(rawResponse,"in_stock_only",
        new FacetBucket("electronics",8),
        new FacetBucket("currency", 4));
    assertHasFacetWithBucketValues(rawResponse,"all",
        new FacetBucket("electronics",12),
        new FacetBucket("currency", 4));
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

  private void assertHasFacetWithBucketValues(NamedList<Object> rawResponse, String expectedFacetName, FacetBucket... expectedBuckets) {
    final NamedList<Object> facetsTopLevel = assertHasFacetResponse(rawResponse);
    assertFacetResponseHasFacetWithBuckets(facetsTopLevel, expectedFacetName, expectedBuckets);
  }

  private void assertHasStatFacetWithValue(NamedList<Object> rawResponse, String expectedFacetName, Double expectedStatValue) {
    final NamedList<Object> facetsTopLevel = assertHasFacetResponse(rawResponse);
    assertFacetResponseHasStatFacetWithValue(facetsTopLevel, expectedFacetName, expectedStatValue);
  }

  private NamedList<Object> assertHasFacetResponse(NamedList<Object> topLevelResponse) {
    Object o = topLevelResponse.get("facets");
    if (o == null) fail("Response has no top-level 'facets' property as expected");
    if (!(o instanceof NamedList)) fail("Response has a top-level 'facets' property, but it is not a NamedList");

    return (NamedList<Object>) o;
  }

  private void assertFacetResponseHasFacetWithBuckets(NamedList<Object> facetResponse, String expectedFacetName, FacetBucket... expectedBuckets) {
    Object o = facetResponse.get(expectedFacetName);
    if (o == null) fail("Response has no top-level facet named '" + expectedFacetName + "'");
    if (!(o instanceof NamedList)) fail("Response has a property for the expected facet '" + expectedFacetName + "' property, but it is not a NamedList");

    final NamedList<Object> expectedFacetTopLevel = (NamedList<Object>) o;
    o = expectedFacetTopLevel.get("buckets");
    if (o == null) fail("Response has no 'buckets' property under 'facets'");
    if (!(o instanceof List)) fail("Response has no 'buckets' property containing actual facet information.");

    final List<NamedList> bucketList = (List<NamedList>) o;
    assertEquals("Expected " + expectedBuckets.length + " buckets, but found " + bucketList.size(),
        expectedBuckets.length, bucketList.size());
    for (int i = 0; i < expectedBuckets.length; i++) {
      final FacetBucket expectedBucket = expectedBuckets[i];
      final NamedList<Object> actualBucket = bucketList.get(i);
      assertEquals(expectedBucket.getVal(), actualBucket.get("val"));
      assertEquals(expectedBucket.getCount(), actualBucket.get("count"));
    }
  }

  private void assertFacetResponseHasStatFacetWithValue(NamedList<Object> facetResponse, String expectedFacetName, Double expectedStatValue) {
    Object o = facetResponse.get(expectedFacetName);
    if (o == null) fail("Response has no top-level facet named '" + expectedFacetName + "'");
    if (!(o instanceof Number)) fail("Response has a property for the expected facet '" + expectedFacetName + "' property, but it is not a Number");

    final Number actualStatValueAsNumber = (Number) o;
    final Double actualStatValueAsDouble = ((Number) o).doubleValue();
    assertEquals(expectedStatValue, actualStatValueAsDouble, 0.5);
  }
}
