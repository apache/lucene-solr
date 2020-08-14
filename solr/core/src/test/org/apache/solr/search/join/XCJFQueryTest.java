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

package org.apache.solr.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class XCJFQueryTest extends SolrCloudTestCase {

  private static final int NUM_NODES = 3;
  private static final int NUM_SHARDS = 3;
  private static final int NUM_REPLICAS = 1;

  private static final int NUM_PRODUCTS = 200;
  private static final String[] SIZES = new String[]{"S", "M", "L", "XL"};

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_NODES)
        .addConfig("xcjf", configset("xcjf"))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();


    CollectionAdminRequest.createCollection("products", "xcjf", NUM_SHARDS, NUM_REPLICAS)
        .process(cluster.getSolrClient());

    CollectionAdminRequest.createCollection("parts", "xcjf", NUM_SHARDS, NUM_REPLICAS)
        .process(cluster.getSolrClient());

  }

  public static void setupIndexes(boolean routeByKey) throws IOException, SolrServerException {
    clearCollection("products");
    clearCollection("parts");

    buildIndexes(routeByKey);

    assertResultCount("products", "*:*", NUM_PRODUCTS, true);
    assertResultCount("parts", "*:*", NUM_PRODUCTS * 10 / 4, true);
  }

  private static void clearCollection(String collection) throws IOException, SolrServerException {
    UpdateRequest update = new UpdateRequest();
    update.deleteByQuery("*:*");
    update.process(cluster.getSolrClient(), collection);
  }

  private static void buildIndexes(boolean routeByKey) throws IOException, SolrServerException {
    List<SolrInputDocument> productDocs = new ArrayList<>();
    List<SolrInputDocument> partDocs = new ArrayList<>();

    for (int productId = 0; productId < NUM_PRODUCTS; ++productId) {
      int sizeNum = productId % SIZES.length;
      String size = SIZES[sizeNum];

      productDocs.add(new SolrInputDocument(
          "id", buildId(productId, String.valueOf(productId), routeByKey),
          "product_id_i", String.valueOf(productId),
          "product_id_l", String.valueOf(productId),
          "product_id_s", String.valueOf(productId),
          "size_s", size));

      // Index 1 parts document for each small product, 2 for each medium, 3 for each large, etc.
      for (int partNum = 0; partNum <= sizeNum; partNum++) {
        String partId = String.format(Locale.ROOT, "%d_%d", productId, partNum);
        partDocs.add(new SolrInputDocument(
            "id", buildId(productId, partId, routeByKey),
            "product_id_i", String.valueOf(productId),
            "product_id_l", String.valueOf(productId),
            "product_id_s", String.valueOf(productId)));
      }
    }

    indexDocs("products", productDocs);
    cluster.getSolrClient().commit("products");

    indexDocs("parts", partDocs);
    cluster.getSolrClient().commit("parts");
  }

  private static String buildId(int productId, String id, boolean routeByKey) {
    return routeByKey ? productId + "!" + id : id;
  }

  private static void indexDocs(String collection, Collection<SolrInputDocument> docs) throws IOException, SolrServerException {
    UpdateRequest update = new UpdateRequest();
    update.add(docs);
    update.process(cluster.getSolrClient(), collection);
  }

  private String getSolrUrl() {
    List<JettySolrRunner> runners = cluster.getJettySolrRunners();
    JettySolrRunner runner = runners.get(random().nextInt(runners.size()));
    return runner.getBaseUrl().toString();
  }

  @Test
  public void testXcjfRoutedCollection() throws Exception {
    setupIndexes(true);
    testXcjfQuery("{!xcjf collection=products from=product_id_i to=product_id_i}size_s:M",true);
    int i = 0;
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      i++;
      String url = runner.getBaseUrl().toString();
      System.setProperty("test.xcjf.solr.url." + i, url);
    }
    try {
      // now we need to re-upload our config , now that we know a valid solr url for the cluster.
      CloudSolrClient client = cluster.getSolrClient();
      ((ZkClientClusterStateProvider) client.getClusterStateProvider()).uploadConfig(configset("xcjf"), "xcjf");
      // reload the cores with the updated whitelisted solr url config.
      CollectionAdminRequest.Reload.reloadCollection("products").process(client);
      CollectionAdminRequest.Reload.reloadCollection("parts").process(client);
      Thread.sleep(10000);

      testXcjfQuery("{!xcjf collection=products from=product_id_i to=product_id_i}size_s:M",true);

      testXcjfQuery(String.format(Locale.ROOT,
          "{!xcjf solrUrl=\"%s\" collection=products from=product_id_i to=product_id_i}size_s:M", getSolrUrl()),
          true);

      testXcjfQuery("{!xcjf collection=products from=product_id_l to=product_id_l}size_s:M",
          true);
      testXcjfQuery(String.format(Locale.ROOT,
          "{!xcjf solrUrl=\"%s\" collection=products from=product_id_l to=product_id_l}size_s:M",
          getSolrUrl()),
          true);

      testXcjfQuery("{!xcjf collection=products from=product_id_s to=product_id_s}size_s:M",
          true);
      testXcjfQuery(String.format(Locale.ROOT,
          "{!xcjf solrUrl=\"%s\" collection=products from=product_id_s to=product_id_s}size_s:M",
          getSolrUrl()),
          true);
      testXcjfQuery(String.format(Locale.ROOT,
          "{!xcjf zkHost=\"%s\" collection=products from=product_id_s to=product_id_s}size_s:M",
          cluster.getSolrClient().getZkHost()),
          true);

      // Test the ability to set other parameters on xcjf and have them passed through
      assertResultCount("parts",
          "{!xcjf collection=products from=product_id_s to=product_id_s fq=product_id_s:1}size_s:M",
          2, true);
      assertResultCount("parts",
          String.format(Locale.ROOT,
              "{!xcjf solrUrl=\"%s\" collection=products from=product_id_s to=product_id_s fq=product_id_s:1}size_s:M",
              getSolrUrl()), 2, true);
    } finally {
      for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
        i++;
        System.getProperties().remove("test.xcjf.solr.url." + i);
      }
    }
  }

  @Test
  public void testXcjfNonroutedCollection() throws Exception {
    setupIndexes(false);

    // This query will expect the collection to have been routed on product_id, so it should return
    // incomplete results.
    testXcjfQuery("{!xcjf collection=products from=product_id_s to=product_id_s}size_s:M",
        false);
    // Now if we set routed=false we should get a complete set of results.
    testXcjfQuery("{!xcjf collection=products from=product_id_s to=product_id_s routed=false}size_s:M",
        true);
    // The xcjf_nonrouted query parser doesn't assume that the collection was routed on product_id,
    // so we should get the full set of results.
    testXcjfQuery("{!xcjf_nonrouted collection=products from=product_id_s to=product_id_s}size_s:M",
        true);
    // But if we set routed=true, we are now assuming again that the collection was routed on product_id,
    // so we should get incomplete results.
    testXcjfQuery("{!xcjf_nonrouted collection=products from=product_id_s to=product_id_s routed=true}size_s:M",
        false);
  }

  @Test
  public void testSolrUrlWhitelist() throws Exception {
    setupIndexes(false);

    // programmatically add the current jetty solr url to the solrUrl whitelist property in the solrconfig.xml
    int i = 0;
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      i++;
      System.setProperty("test.xcjf.solr.url." + i, runner.getBaseUrl().toString());
    }
    try {
      // now we need to re-upload our config , now that we know a valid solr url for the cluster.
      CloudSolrClient client = cluster.getSolrClient();
      ((ZkClientClusterStateProvider) client.getClusterStateProvider()).uploadConfig(configset("xcjf"), "xcjf");
      // reload the cores with the updated whitelisted solr url config.
      CollectionAdminRequest.Reload.reloadCollection("products").process(client);
      CollectionAdminRequest.Reload.reloadCollection("parts").process(client);

      final ModifiableSolrParams params = new ModifiableSolrParams();
      //  a bogus solrUrl
      params.add("q", "");
      params.add("rows", "0");

      // we expect an exception because bogus url isn't valid.
      try {
        // This should throw an exception.
        // verify the xcfj_whitelist definition has the current valid urls and works.
        testXcjfQuery(String.format(Locale.ROOT,
            "{!xcjf_whitelist solrUrl=\"%s\" collection=products from=product_id_i to=product_id_i}size_s:M",
            "http://bogus.example.com:8983/solr"),
            true);
        fail("The query invovling bogus.example.com should not succeed");
      } catch (Exception e) {
        // should get here.
        String message = e.getMessage();
        assertTrue("message was " + message, message.contains("SyntaxError: Solr Url was not in the whitelist"));
      }

      // verify the xcfj_whitelist definition has the current valid urls and works.
      testXcjfQuery(String.format(Locale.ROOT,
          "{!xcjf_whitelist solrUrl=\"%s\" collection=products from=product_id_i to=product_id_i}size_s:M",
          getSolrUrl()),
          true);

    } finally {
      for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
        i++;
        System.getProperties().remove("test.xcjf.solr.url." + i);
      }
    }
  }

  public void testXcjfQuery(String query, boolean expectFullResults) throws Exception {
    assertResultCount("parts", query, NUM_PRODUCTS / 2, expectFullResults);
  }

  private static void assertResultCount(String collection, String query, long expectedCount, boolean expectFullResults)
      throws IOException, SolrServerException {

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", query);
    params.add("rows", "0");

    QueryResponse resp = cluster.getSolrClient().query(collection, params);

    if (expectFullResults) {
      assertEquals(expectedCount, resp.getResults().getNumFound());
    } else {
      assertTrue(resp.getResults().getNumFound() < expectedCount);
    }
  }
}
