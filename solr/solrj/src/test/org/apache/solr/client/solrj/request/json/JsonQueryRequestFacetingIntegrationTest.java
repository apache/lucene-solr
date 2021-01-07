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
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.json.BucketJsonFacet;
import org.apache.solr.client.solrj.response.json.NestableJsonFacet;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

public class JsonQueryRequestFacetingIntegrationTest extends SolrCloudTestCase {

  private static final String COLLECTION_NAME = "techproducts";
  private static final String CONFIG_NAME = "techproducts_config";
  private static final int NUM_TECHPRODUCTS_DOCS = 32;
  private static final int NUM_IN_STOCK = 17;
  private static final int NUM_CATEGORIES = 16;
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

    CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());

    ContentStreamUpdateRequest up = new ContentStreamUpdateRequest("/update");
    up.setParam("collection", COLLECTION_NAME);
    up.addFile(getFile("solrj/techproducts.xml"), "application/xml");
    up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    UpdateResponse updateResponse = up.process(cluster.getSolrClient());
    assertEquals(0, updateResponse.getStatus());
  }

  @Test
  public void testSingleTermsFacet() throws Exception {
    final TermsFacetMap categoriesFacetMap = new TermsFacetMap("cat")
        .setLimit(3);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_cats", categoriesFacetMap);

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
  public void testTermsFacetWithPrelimSort() throws Exception {
    final TermsFacetMap categoriesFacetMap = new TermsFacetMap("cat")
        .setPreliminarySort("count desc")
        .setSort("index desc")
        .setLimit(3);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_cats", categoriesFacetMap);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    //The prelim_sort/sort combination should give us the 3 most popular categories in reverse-alpha order
    assertHasFacetWithBucketValues(topLevelFacetData, "top_cats",
        new FacetBucket("memory", NUM_MEMORY),
        new FacetBucket("electronics", NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY));

  }

  @Test
  public void testTermsFacetWithNumBucketsRequested() throws Exception {
    final TermsFacetMap categoriesFacetMap = new TermsFacetMap("cat")
        .includeTotalNumBuckets(true)
        .setLimit(3);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_cats", categoriesFacetMap);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();

    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    assertHasFacetWithBucketValues(topLevelFacetData, "top_cats",
        new FacetBucket("electronics", NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
    assertEquals(NUM_CATEGORIES, topLevelFacetData.getBucketBasedFacets("top_cats").getNumBucketsCount());
  }

  @Test
  public void testTermsFacetWithAllBucketsRequested() throws Exception {
    final TermsFacetMap categoriesFacetMap = new TermsFacetMap("cat")
        .includeAllBucketsUnionBucket(true)
        .setLimit(3);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_cats", categoriesFacetMap);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();

    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    assertHasFacetWithBucketValues(topLevelFacetData, "top_cats",
        new FacetBucket("electronics", NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
    assertEquals(37, topLevelFacetData.getBucketBasedFacets("top_cats").getAllBuckets());
  }

  @Test
  public void testFacetCanBeRepresentedByMapWriter() throws Exception {
    final MapWriter categoriesFacet = new MapWriter() {
      @Override
      public void writeMap(EntryWriter ew) throws IOException {
        ew.put("type", "terms");
        ew.put("field", "cat");
        ew.put("limit", 3);
      }
    };
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_cats", categoriesFacet);

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
    final TermsFacetMap categoriesFacetMap = new TermsFacetMap("cat")
        .setLimit(3);
    final TermsFacetMap manufacturersFacetMap = new TermsFacetMap("manu_id_s")
        .setLimit(3);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_cats", categoriesFacetMap)
        .withFacet("top_manufacturers", manufacturersFacetMap);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    assertHasFacetWithBucketValues(topLevelFacetData, "top_cats",
        new FacetBucket("electronics", NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
    assertHasFacetWithBucketValues(topLevelFacetData, "top_manufacturers",
        new FacetBucket("corsair", NUM_CORSAIR),
        new FacetBucket("belkin", NUM_BELKIN),
        new FacetBucket("canon", NUM_CANON));
  }

  @Test
  public void testSingleRangeFacet() throws Exception {
    final RangeFacetMap pricesFacet = new RangeFacetMap("price", 0, 100, 20);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("prices", pricesFacet);

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
  public void testSingleDateRangeFacet() throws Exception {
    final Date startDate = new Date(Instant.parse("2005-08-01T16:30:25Z").toEpochMilli());
    final Date endDate = new Date(Instant.parse("2006-02-13T15:26:37Z").toEpochMilli());
    final RangeFacetMap manufactureDateFacet = new RangeFacetMap("manufacturedate_dt", startDate, endDate, "+1MONTH")
        .setMinCount(1);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("man_date", manufactureDateFacet);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    assertHasFacetWithBucketValues(topLevelFacetData,"man_date",
        new FacetBucket(new Date(Instant.parse("2005-08-01T16:30:25Z").toEpochMilli()), 1),
        new FacetBucket(new Date(Instant.parse("2005-10-01T16:30:25Z").toEpochMilli()), 1),
        new FacetBucket(new Date(Instant.parse("2006-02-01T16:30:25Z").toEpochMilli()), 9));
  }

  @Test
  public void testMultiRangeFacet() throws Exception {
    final RangeFacetMap pricesFacet = new RangeFacetMap("price", 0, 100, 20);
    final RangeFacetMap shippingWeightFacet = new RangeFacetMap("weight", 0, 200, 50);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("prices", pricesFacet)
        .withFacet("shipping_weights", shippingWeightFacet);

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
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withStatFacet("sum_price", "sum(price)");

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertEquals(NUM_TECHPRODUCTS_DOCS, topLevelFacetData.getCount());
    assertHasStatFacetWithValue(topLevelFacetData,"sum_price", 5251.270030975342);
  }

  @Test
  public void testMultiStatFacet() throws Exception {
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withStatFacet("sum_price", "sum(price)")
        .withStatFacet("avg_price", "avg(price)");

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasStatFacetWithValue(topLevelFacetData,"sum_price", 5251.270030975342);
    assertHasStatFacetWithValue(topLevelFacetData,"avg_price", 328.20437693595886);
  }

  @Test
  public void testMultiFacetsMixedTypes() throws Exception {
    final TermsFacetMap categoryFacet = new TermsFacetMap("cat")
        .setLimit(3);
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withStatFacet("avg_price", "avg(price)")
        .withFacet("top_cats", categoryFacet);

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
    final TermsFacetMap categoriesFacet = new TermsFacetMap("cat")
        .setLimit(3)
        .withSubFacet("top_manufacturers_for_cat", new TermsFacetMap("manu_id_s").setLimit(1));
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_cats", categoriesFacet);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    // Test top level facets
    assertHasFacetWithBucketValues(topLevelFacetData,"top_cats",
        new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
    // Test subfacet values for each top-level facet bucket
    final List<BucketJsonFacet> topCatsBuckets = topLevelFacetData.getBucketBasedFacets("top_cats").getBuckets();
    final NestableJsonFacet electronicsFacet = topCatsBuckets.get(0);
    assertHasFacetWithBucketValues(electronicsFacet, "top_manufacturers_for_cat", new FacetBucket("corsair", 3));
    final NestableJsonFacet currencyFacet = topCatsBuckets.get(1);
    assertHasFacetWithBucketValues(currencyFacet, "top_manufacturers_for_cat", new FacetBucket("boa", 1));
    final NestableJsonFacet memoryFacet = topCatsBuckets.get(2);
    assertHasFacetWithBucketValues(memoryFacet, "top_manufacturers_for_cat", new FacetBucket("corsair", 3));
  }

  @Test
  public void testNestedFacetsOfMixedTypes() throws Exception {
    final String subfacetName = "avg_price_for_cat";

    final TermsFacetMap categoriesFacet = new TermsFacetMap("cat")
        .setLimit(3)
        .withStatSubFacet(subfacetName, "avg(price)");
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_cats", categoriesFacet);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_TECHPRODUCTS_DOCS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    // Test top level facets
    assertHasFacetWithBucketValues(topLevelFacetData,"top_cats",
        new FacetBucket("electronics",NUM_ELECTRONICS),
        new FacetBucket("currency", NUM_CURRENCY),
        new FacetBucket("memory", NUM_MEMORY));
    // Test subfacet values for each top-level facet bucket
    final List<BucketJsonFacet> topCatsResultBuckets = topLevelFacetData.getBucketBasedFacets("top_cats").getBuckets();
    assertHasStatFacetWithValue(topCatsResultBuckets.get(0), subfacetName, 252.02909261530095); // electronics
    assertHasStatFacetWithValue(topCatsResultBuckets.get(1), subfacetName, 0.0); // currency
    assertHasStatFacetWithValue(topCatsResultBuckets.get(2), subfacetName, 129.99499893188477); // memory
  }

  @Test
  public void testFacetWithDomainFilteredBySimpleQueryString() throws Exception {
    final TermsFacetMap popularCategoriesFacet = new TermsFacetMap("cat")
        .setLimit(3)
        .withDomain(new DomainMap()
            .withFilter("popularity:[5 TO 10]"));
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_popular_cats", popularCategoriesFacet);

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
    final TermsFacetMap popularCategoriesFacet = new TermsFacetMap("cat")
        .setLimit(3)
        .withDomain(new DomainMap()
            .withFilter("{!lucene df=\"popularity\" v=\"[5 TO 10]\"}"));

    JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("top_popular_cats", popularCategoriesFacet);

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
    final TermsFacetMap categoriesFacet = new TermsFacetMap("cat")
        .setLimit(3)
        .withDomain(new DomainMap()
            .withQuery("*:*"));
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("cat:electronics")
        .withFacet("top_cats", categoriesFacet);

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
    final TermsFacetMap searchCategoriesFacet = new TermsFacetMap("cat")
        .withDomain(new DomainMap()
            .withQuery("{!lucene df=\"cat\" v=\"search\"}"));
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("cat:electronics")
        .withFacet("largest_search_cats", searchCategoriesFacet);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_ELECTRONICS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"largest_search_cats",
        new FacetBucket("search",2),
        new FacetBucket("software", 2));
  }

  @Test
  public void testFacetWithMultipleSimpleQueryClausesInArbitraryDomain() throws Exception {
    final TermsFacetMap solrCategoriesFacet = new TermsFacetMap("cat")
        .withDomain(new DomainMap()
            .withQuery("cat:search")
            .withQuery("name:Solr"));
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("cat:electronics")
        .withFacet("cats_matching_solr", solrCategoriesFacet);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_ELECTRONICS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"cats_matching_solr",
        new FacetBucket("search",1),
        new FacetBucket("software", 1));
  }

  @Test
  public void testFacetWithMultipleLocalParamsQueryClausesInArbitraryDomain() throws Exception {
    final TermsFacetMap solrCategoriesFacet = new TermsFacetMap("cat")
        .withDomain(new DomainMap()
            .withQuery("{!lucene df=\"cat\" v=\"search\"}")
            .withQuery("{!lucene df=\"name\" v=\"Solr\"}"));
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("cat:electronics")
        .withFacet("cats_matching_solr", solrCategoriesFacet);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_ELECTRONICS, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"cats_matching_solr",
        new FacetBucket("search",1),
        new FacetBucket("software", 1));
  }

  @Test
  public void testFacetWithDomainWidenedUsingExcludeTagsToIgnoreFilters() throws Exception {
    final TermsFacetMap inStockFacet = new TermsFacetMap("cat")
        .setLimit(2);
    final TermsFacetMap allProductsFacet = new TermsFacetMap("cat")
        .setLimit(2).withDomain(new DomainMap().withTagsToExclude("on_shelf"));
    final Map<String, Object> taggedFilterMap = new HashMap<>();
    taggedFilterMap.put("#on_shelf", "inStock:true");
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFilter(taggedFilterMap)
        .withFacet("in_stock_only", inStockFacet)
        .withFacet("all", allProductsFacet);

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertExpectedDocumentsFoundAndReturned(response, NUM_IN_STOCK, 10);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();
    assertHasFacetWithBucketValues(topLevelFacetData,"in_stock_only",
        new FacetBucket("electronics",8),
        new FacetBucket("currency", 4));
    assertHasFacetWithBucketValues(topLevelFacetData  ,"all",
        new FacetBucket("electronics",12),
        new FacetBucket("currency", 4));
  }

  @Test
  public void testRangeFacetWithOtherBucketsRequested() throws Exception {
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .withFacet("price_range",
            new RangeFacetMap("price", 0, 100, 20)
                .setOtherBuckets(RangeFacetMap.OtherBuckets.ALL)
        );

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);
    final NestableJsonFacet topLevelFacetData = response.getJsonFacetingResponse();

    assertHasFacetWithBucketValues(topLevelFacetData, "price_range",
        new FacetBucket(0.0f, 5),
        new FacetBucket(20.0f, 0),
        new FacetBucket(40.0f, 0),
        new FacetBucket(60.0f, 1),
        new FacetBucket(80.0f, 1));
    assertEquals(0, topLevelFacetData.getBucketBasedFacets("price_range").getBeforeCount());
    assertEquals(9, topLevelFacetData.getBucketBasedFacets("price_range").getAfterCount());
    assertEquals(7, topLevelFacetData.getBucketBasedFacets("price_range").getBetweenCount());
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
