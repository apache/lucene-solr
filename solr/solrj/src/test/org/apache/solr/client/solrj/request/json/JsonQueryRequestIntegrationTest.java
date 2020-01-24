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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for {@link JsonQueryRequest}
 */
public class JsonQueryRequestIntegrationTest extends SolrCloudTestCase {

  private static final String COLLECTION_NAME = "books";
  private static final String CONFIG_NAME = "techproducts_config";

  private static final int NUM_BOOKS_TOTAL = 10;
  private static final int NUM_SCIFI_BOOKS = 2;
  private static final int NUM_IN_STOCK = 8;
  private static final int NUM_IN_STOCK_AND_FIRST_IN_SERIES = 5;

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
    up.addFile(getFile("solrj/books.csv"), "application/csv");
    up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    UpdateResponse updateResponse = up.process(cluster.getSolrClient());
    assertEquals(0, updateResponse.getStatus());
  }

  @Test
  public void testEmptyJson() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest();
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);

    // No q.alt in techproducts configset, so request should gracefully find no results
    assertEquals(0, queryResponse.getStatus());
    assertEquals(0, queryResponse.getResults().getNumFound());
  }

  @Test
  public void testCanRunSimpleQueries() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("*:*");
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_BOOKS_TOTAL, queryResponse.getResults().getNumFound());
  }

  @Test
  public void testQueriesCanUseLocalParamsSyntax() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("{!lucene df=genre_s v='scifi'}");
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_SCIFI_BOOKS, queryResponse.getResults().getNumFound());
  }


  @Test
  public void testQueriesCanUseExpandedSyntax() throws Exception {
    //Construct a tree representing the JSON: {lucene: {df:'genre_s', 'query': 'scifi'}}
    final Map<String, Object> queryMap = new HashMap<>();
    final Map<String, Object> luceneQueryParamMap = new HashMap<>();
    queryMap.put("lucene", luceneQueryParamMap);
    luceneQueryParamMap.put("df", "genre_s");
    luceneQueryParamMap.put("query", "scifi");

    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery(queryMap);
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_SCIFI_BOOKS, queryResponse.getResults().getNumFound());
  }

  @Test
  public void testQueriesCanBeRepresentedUsingMapWriters() throws Exception {
    final MapWriter queryWriter = new MapWriter() {
      @Override
      public void writeMap(EntryWriter ew) throws IOException {
        ew.put("lucene", (MapWriter) queryParamWriter -> {
          queryParamWriter.put("df", "genre_s");
          queryParamWriter.put("query", "scifi");
        });
      }
    };

    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery(queryWriter);
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_SCIFI_BOOKS, queryResponse.getResults().getNumFound());
  }

  @Test
  public void testQueriesCanBeNested() throws Exception {
    final Map<String, Object> queryJsonMap = new HashMap<>();
    final Map<String, Object> clausesJsonMap = new HashMap<>();
    queryJsonMap.put("bool", clausesJsonMap);
    clausesJsonMap.put("must", "genre_s:scifi");
    clausesJsonMap.put("must_not", "series_t:Ender");

    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery(queryJsonMap);
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(1, queryResponse.getResults().getNumFound()); // 2 scifi books, only 1 is NOT "Ender's Game"
  }

  @Test
  public void testFiltersCanBeAddedToQueries() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("*:*")
        .withFilter("inStock:true");
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_IN_STOCK, queryResponse.getResults().getNumFound());
  }

  @Test
  public void testFiltersCanUseLocalParamsSyntax() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("*:*")
        .withFilter("{!lucene df=inStock v='true'}");
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_IN_STOCK, queryResponse.getResults().getNumFound());
  }

  @Test
  public void testFiltersCanUseExpandedSyntax() throws Exception {
    final Map<String, Object> filterJsonMap = new HashMap<>();
    final Map<String, Object> luceneQueryParamsMap = new HashMap<>();
    filterJsonMap.put("lucene", luceneQueryParamsMap);
    luceneQueryParamsMap.put("df", "genre_s");
    luceneQueryParamsMap.put("query", "scifi");

    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("*:*")
        .withFilter(filterJsonMap);
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_SCIFI_BOOKS, queryResponse.getResults().getNumFound());
  }

  @Test
  public void testMultipleFiltersCanBeUsed() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("*:*")
        .withFilter("sequence_i:1") // 7 books are the first of a series
        .withFilter("inStock:true");// but only 5 are in stock
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_IN_STOCK_AND_FIRST_IN_SERIES, queryResponse.getResults().getNumFound());
  }

  @Test
  public void canSpecifyFieldsToBeReturned() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("*:*")
        .returnFields("id", "name");
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    final SolrDocumentList docs = queryResponse.getResults();
    assertEquals(NUM_BOOKS_TOTAL, docs.getNumFound());
    for (SolrDocument returnedDoc : docs) {
      final Collection<String> fields = returnedDoc.getFieldNames();
      assertEquals(2, fields.size());
      assertTrue("Expected field list to contain 'id'", fields.contains("id"));
      assertTrue("Expected field list to contain 'name'", fields.contains("name"));
    }
  }

  @Test
  public void testObeysResultLimit() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("*:*")
        .setLimit(5);
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_BOOKS_TOTAL, queryResponse.getResults().getNumFound());
    assertEquals(5, queryResponse.getResults().size());
  }

  @Test
  public void testAcceptsTraditionalQueryParamNamesInParamsBlock() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .withParam("q", "*:*")
        .withParam("rows", 4);
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_BOOKS_TOTAL, queryResponse.getResults().getNumFound());
    assertEquals(4, queryResponse.getResults().size());
  }

  @Test
  public void testReturnsResultsStartingAtOffset() throws Exception {
    final JsonQueryRequest originalDocsQuery = new JsonQueryRequest()
        .setQuery("*:*");
    QueryResponse originalDocsResponse = originalDocsQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, originalDocsResponse.getStatus());
    assertEquals(NUM_BOOKS_TOTAL, originalDocsResponse.getResults().size());
    final SolrDocumentList originalDocs = originalDocsResponse.getResults();

    final int offset = 2;
    final JsonQueryRequest offsetDocsQuery = new JsonQueryRequest()
        .setQuery("*:*")
        .setOffset(offset);
    QueryResponse offsetDocsResponse = offsetDocsQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, offsetDocsResponse.getStatus());
    assertEquals(NUM_BOOKS_TOTAL - offset, offsetDocsResponse.getResults().size());
    final SolrDocumentList offsetDocs = offsetDocsResponse.getResults();

    // Ensure the same docs are returned, shifted by 'offset'
    for (int i = 0; i < offsetDocs.size(); i++) {
      final String offsetId = (String) offsetDocs.get(i).getFieldValue("id");
      final String originalId = (String) originalDocs.get(i + offset).getFieldValue("id");
      assertEquals(offsetId, originalId);
    }
  }

  @Test
  public void testReturnsReturnsResultsWithSpecifiedSort() throws Exception {
    final JsonQueryRequest simpleQuery = new JsonQueryRequest()
        .setQuery("*:*")
        .setSort("price desc");
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);

    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_BOOKS_TOTAL, queryResponse.getResults().getNumFound());
    final SolrDocumentList docs = queryResponse.getResults();
    for (int i = 0; i < docs.size() - 1; i++) {
      final float pricierDocPrice = (Float) docs.get(i).getFieldValue("price");
      final float cheaperDocPrice = (Float) docs.get(i+1).getFieldValue("price");
      assertTrue("Expected doc at index " + i + " doc to be more expensive than doc at " + (i+1),
          pricierDocPrice >= cheaperDocPrice);
    }
  }

  @Test
  public void testCombinesJsonParamsWithUriParams() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("fq", "inStock:true");
    final JsonQueryRequest simpleQuery = new JsonQueryRequest(params)
        .setQuery("*:*");
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_IN_STOCK, queryResponse.getResults().getNumFound());
  }

  @Test
  public void testExpandsParameterMacros() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("FIELD", "inStock");
    params.set("VALUE", "true");
    final JsonQueryRequest simpleQuery = new JsonQueryRequest(params)
        .setQuery("${FIELD}:${VALUE}");
    QueryResponse queryResponse = simpleQuery.process(cluster.getSolrClient(), COLLECTION_NAME);
    assertEquals(0, queryResponse.getStatus());
    assertEquals(NUM_IN_STOCK, queryResponse.getResults().getNumFound());
  }
}
