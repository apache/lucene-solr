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
import org.apache.solr.client.solrj.response.json.BucketJsonFacet;
import org.apache.solr.client.solrj.response.json.NestableJsonFacet;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    assertHasFacetWithBucketValues(topLevelFacetData, "top_cats",
        new FacetBucket("electronics", NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    assertHasFacetWithBucketValues(topLevelFacetData,"top_cats",
        new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
    assertHasFacetWithBucketValues(topLevelFacetData,"top_manufacturers",
        new FacetBucket("corsair",NUM_CORSAIR),
        new FacetBucket("belkin", NUM_BELKIN),
        new FacetBucket("canon", NUM_CANON));
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    assertHasFacetWithBucketValues(topLevelFacetData,"prices",
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    assertHasFacetWithBucketValues(topLevelFacetData,"prices",
        new FacetBucket(0.0f, 5),
        new FacetBucket(20.0f, 0),
        new FacetBucket(40.0f, 0),
        new FacetBucket(60.0f, 1),
        new FacetBucket(80.0f, 1));
    assertHasFacetWithBucketValues(topLevelFacetData, "shipping_weights",
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasStatFacetWithValue(topLevelFacetData,"sum_price", 5251.270030975342);
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasStatFacetWithValue(topLevelFacetData,"sum_price", 5251.270030975342);
    assertHasStatFacetWithValue(topLevelFacetData,"avg_price", 328.20437693595886);
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasStatFacetWithValue(topLevelFacetData,"avg_price", 328.20437693595886);
    assertHasFacetWithBucketValues(topLevelFacetData,"top_cats",
        new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
  }

  @Test
  public void testNestedTermsFacet() throws Exception {
    final String subfacetName = "top_manufacturers_for_cat";
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    // Test top level facets
    assertHasFacetWithBucketValues(topLevelFacetData,"top_cats",
        new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
    // Test subfacet values for each top-level facet bucket
    final List<BucketJsonFacet> catBuckets = topLevelFacetData.getBucketBasedFacets("top_cats").getBuckets();
    assertHasFacetWithBucketValues(catBuckets.get(0), subfacetName, new FacetBucket("corsair", 3));
    assertHasFacetWithBucketValues(catBuckets.get(1), subfacetName, new FacetBucket("boa", 1));
    assertHasFacetWithBucketValues(catBuckets.get(2), subfacetName, new FacetBucket("corsair", 3));
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    // Test top level facets
    assertHasFacetWithBucketValues(topLevelFacetData,"top_cats",
        new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
    // Test subfacet values for each top-level facet bucket
    final List<BucketJsonFacet> catBuckets = topLevelFacetData.getBucketBasedFacets("top_cats").getBuckets();
    assertHasStatFacetWithValue(catBuckets.get(0), subfacetName, 252.02909261530095); // electronics
    assertHasStatFacetWithValue(catBuckets.get(1), subfacetName, 0.0); // currency
    assertHasStatFacetWithValue(catBuckets.get(2), subfacetName, 129.99499893188477); // memory
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"top_popular_cats",
        new FacetBucket("electronics",9),
        new FacetBucket("graphics card", 2),
        new FacetBucket("hard drive", 2));
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"top_popular_cats",
        new FacetBucket("electronics",9),
        new FacetBucket("graphics card", 2),
        new FacetBucket("hard drive", 2));
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_ELECTRONICS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"top_cats",
        new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_ELECTRONICS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"largest_search_cats",
        new FacetBucket("search",2),
        new FacetBucket("software", 2));
  }

  @Test
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_ELECTRONICS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"cats_matching_solr",
        new FacetBucket("search",1),
        new FacetBucket("software", 1));
  }

  @Test
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_ELECTRONICS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"cats_matching_solr",
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

    assertExpectedDocumentsFoundAndReturned(response, NUM_IN_STOCK, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"in_stock_only",
        new FacetBucket("electronics",8),
        new FacetBucket("currency", 4));
    assertHasFacetWithBucketValues(topLevelFacetData,"all",
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

  private void assertHasStatFacetWithValue(NestableJsonFacet response, String expectedFacetName, Double expectedStatValue) {
    assertTrue("Expected response to have stat facet named '" + expectedFacetName + "'",
        response.getStatValue(expectedFacetName) != null);
    assertEquals(expectedStatValue, response.getStatValue(expectedFacetName));
  }

  private void assertExpectedDocumentsFoundAndReturned(QueryResponse response, int expectedNumFound, int expectedReturned) {
    assertEquals(0, response.getStatus());
    final SolrDocumentList documents = response.getResults();
    assertEquals(expectedNumFound, documents.getNumFound());
    assertEquals(expectedReturned, documents.size());
  }
}
