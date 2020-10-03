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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.api.collections.CategoryRoutedAlias;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CategoryRoutedAliasUpdateProcessorTest extends RoutedAliasUpdateProcessorTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // use this for example categories
  private static final String[] SHIPS = {
      "Constructor",
      "Heart of Gold",
      "Stunt Ship",
      "B-ark",
      "Bi$tromath"
  };

  private static final String categoryField = "ship_name_en";
  private static final String intField = "integer_i";

  private int lastDocId = 0;
  private static CloudSolrClient solrClient;
  private int numDocsDeletedOrFailed = 0;

  @Before
  public void doBefore() throws Exception {
    configureCluster(4).configure();
    solrClient = getCloudSolrClient(cluster);
    //log this to help debug potential causes of problems
    if (log.isInfoEnabled()) {
      log.info("SolrClient: {}", solrClient);
      log.info("ClusterStateProvider {}", solrClient.getClusterStateProvider()); // nowarn
    }
  }

  @After
  public void doAfter() throws Exception {
    IOUtils.close(solrClient);
    if (null != cluster) {
      shutdownCluster();
    }
  }

  @AfterClass
  public static void cleanUpAfterClass() throws Exception {
    solrClient = null;
  }

  public void testNonEnglish() throws Exception {
    // test to document the expected behavior with non-english text for categories
    // the present expectation is that non-latin text and many accented latin characters
    // will get replaced with '_'. This is necessary to maintain collection naming
    // conventions. The net effect is that documents get sorted by the number of characters
    // in the category rather than the actual categories.

    // This should be changed in an enhancement (wherein the category is RFC-4648 url-safe encoded).
    // For now document it as an expected limitation.

    String somethingInChinese = "中文的东西";      // 5 chars
    String somethingInHebrew = "משהו בסינית";      // 11 chars
    String somethingInThai = "บางอย่างในภาษาจีน";   // 17 chars
    String somethingInArabic = "شيء في الصينية"; // 14 chars
    String somethingInGreek = "κάτι κινεζικό";   // 13 chars
    String somethingInGujarati = "િનીમાં કંઈક";       // 11 chars (same as hebrew)

    String ONE_   = "_";
    String TWO_   = "__";
    String THREE_ = "___";
    String FOUR_  = "____";
    String FIVE_  = "_____";

    String collectionChinese  = getAlias() + "__CRA__" + FIVE_;
    String collectionHebrew   = getAlias() + "__CRA__" + FIVE_ + FIVE_ + ONE_;
    String collectionThai     = getAlias() + "__CRA__" + FIVE_ + FIVE_ + FIVE_ + TWO_;
    String collectionArabic   = getAlias() + "__CRA__" + FIVE_ + FIVE_ + FOUR_;
    String collectionGreek    = getAlias() + "__CRA__" + FIVE_ + FIVE_ + THREE_;
    // Note Gujarati not listed, because it duplicates hebrew.

    String configName = getSaferTestName();
    createConfigSet(configName);

    List<String> retrievedConfigSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();
    List<String> expectedConfigSetNames = Arrays.asList("_default", configName);

    // config sets leak between tests so we can't be any more specific than this on the next 2 asserts
    assertTrue("We expect at least 2 configSets",
        retrievedConfigSetNames.size() >= expectedConfigSetNames.size());
    assertTrue("ConfigNames should include :" + expectedConfigSetNames, retrievedConfigSetNames.containsAll(expectedConfigSetNames));

    CollectionAdminRequest.createCategoryRoutedAlias(getAlias(), categoryField, 20,
        CollectionAdminRequest.createCollection("_unused_", configName, 1, 1)
            .setMaxShardsPerNode(2))
        .process(solrClient);
    addDocsAndCommit(true,
        newDoc(somethingInChinese),
        newDoc(somethingInHebrew),
        newDoc(somethingInThai),
        newDoc(somethingInArabic),
        newDoc(somethingInGreek),
        newDoc(somethingInGujarati));

    // Note Gujarati not listed, because it duplicates hebrew.
    assertInvariants(collectionChinese, collectionHebrew, collectionThai, collectionArabic, collectionGreek);

    assertColHasDocCount(collectionChinese, 1);
    assertColHasDocCount(collectionHebrew, 2);
    assertColHasDocCount(collectionThai, 1);
    assertColHasDocCount(collectionArabic, 1);
    assertColHasDocCount(collectionGreek, 1);

  }

  private void assertColHasDocCount(String collectionChinese, int expected) throws SolrServerException, IOException {
    final QueryResponse colResponse = solrClient.query(collectionChinese, params(
        "q", "*:*",
        "rows", "0"));
    long aliasNumFound = colResponse.getResults().getNumFound();
    assertEquals(expected,aliasNumFound);
  }

  @Slow
  @Test
  public void test() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);

    // Start with one collection manually created (and use higher numShards & replicas than we'll use for others)
    //  This tests we may pre-create the collection and it's acceptable.
    final String colVogon = getAlias() + "__CRA__" + SHIPS[0];

    // we expect changes ensuring a legal collection name.
    final String colHoG = getAlias() + "__CRA__" + noSpaces(SHIPS[1]);
    final String colStunt = getAlias() + "__CRA__" + noSpaces(SHIPS[2]);
    final String colArk = getAlias() + "__CRA__" + noDashes(SHIPS[3]);
    final String colBistro = getAlias() + "__CRA__" + noDollar(SHIPS[4]);

    List<String> retrievedConfigSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();
    List<String> expectedConfigSetNames = Arrays.asList("_default", configName);

    // config sets leak between tests so we can't be any more specific than this on the next 2 asserts
    assertTrue("We expect at least 2 configSets",
        retrievedConfigSetNames.size() >= expectedConfigSetNames.size());
    assertTrue("ConfigNames should include :" + expectedConfigSetNames, retrievedConfigSetNames.containsAll(expectedConfigSetNames));

    CollectionAdminRequest.createCategoryRoutedAlias(getAlias(), categoryField, 20,
        CollectionAdminRequest.createCollection("_unused_", configName, 1, 1)
            .setMaxShardsPerNode(2))
        .process(solrClient);

    // now we index a document
    addDocsAndCommit(true, newDoc(SHIPS[0]));
    //assertDocRoutedToCol(lastDocId, col23rd);

    String uninitialized = getAlias() + "__CRA__" + CategoryRoutedAlias.UNINITIALIZED;

    // important to test that we don't try to delete the temp collection on the first document. If we did so
    // we would be at risk of out of order execution of the deletion/creation which would leave a window
    // of time where there were no collections in the alias. That would likely break all manner of other
    // parts of solr.
    assertInvariants(colVogon, uninitialized);

    addDocsAndCommit(true,
        newDoc(SHIPS[1]),
        newDoc(SHIPS[2]),
        newDoc(SHIPS[3]),
        newDoc(SHIPS[4]));

    // NOW the temp collection should be gone!
    assertInvariants(colVogon, colHoG, colStunt, colArk, colBistro);

    // make sure we fail if we have no value to route on.
    testFailedDocument(newDoc(null), "Route value is null");
    testFailedDocument(newDoc("foo__CRA__bar"), "7 character sequence __CRA__");
    testFailedDocument(newDoc("fóóCRAóóbar"), "7 character sequence __CRA__");

  }

  private String noSpaces(String ship) {
    return ship.replaceAll("\\s", "_");
  }
  private String noDashes(String ship) {
    return ship.replaceAll("-", "_");
  }
  private String noDollar(String ship) {
    return ship.replaceAll("\\$", "_");
  }

  @Slow
  @Test
  public void testMustMatch() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);
    final String mustMatchRegex = "HHS\\s.+_solr";

    final int maxCardinality = Integer.MAX_VALUE; // max cardinality for current test

    // Start with one collection manually created (and use higher numShards & replicas than we'll use for others)
    //  This tests we may pre-create the collection and it's acceptable.
    final String colVogon = getAlias() + "__CRA__" + noSpaces("HHS "+ SHIPS[0]) + "_solr";

    // we expect changes ensuring a legal collection name.
    final String colHoG = getAlias() + "__CRA__" + noSpaces("HHS "+ SHIPS[1]) + "_solr";

    List<String> retrievedConfigSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();
    List<String> expectedConfigSetNames = Arrays.asList("_default", configName);

    // config sets leak between tests so we can't be any more specific than this on the next 2 asserts
    assertTrue("We expect at least 2 configSets",
        retrievedConfigSetNames.size() >= expectedConfigSetNames.size());
    assertTrue("ConfigNames should include :" + expectedConfigSetNames, retrievedConfigSetNames.containsAll(expectedConfigSetNames));

    CollectionAdminRequest.createCategoryRoutedAlias(getAlias(), categoryField, maxCardinality,
        CollectionAdminRequest.createCollection("_unused_", configName, 1, 1)
            .setMaxShardsPerNode(2))
        .setMustMatch(mustMatchRegex)
        .process(solrClient);

    // now we index a document
    addDocsAndCommit(true, newDoc("HHS " + SHIPS[0] + "_solr"));
    //assertDocRoutedToCol(lastDocId, col23rd);

    String uninitialized = getAlias() + "__CRA__" + CategoryRoutedAlias.UNINITIALIZED;
    assertInvariants(colVogon, uninitialized);

    addDocsAndCommit(true, newDoc("HHS "+ SHIPS[1] + "_solr"));

    assertInvariants(colVogon, colHoG);

    // should fail since max cardinality is reached
    testFailedDocument(newDoc(SHIPS[2]), "does not match " + CategoryRoutedAlias.ROUTER_MUST_MATCH);
    assertInvariants(colVogon, colHoG);
  }

  @Slow
  @Test
  public void testInvalidMustMatch() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);
    // Not a valid regex
    final String mustMatchRegex = "+_solr";

    final int maxCardinality = Integer.MAX_VALUE; // max cardinality for current test

    List<String> retrievedConfigSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();
    List<String> expectedConfigSetNames = Arrays.asList("_default", configName);

    // config sets leak between tests so we can't be any more specific than this on the next 2 asserts
    assertTrue("We expect at least 2 configSets",
        retrievedConfigSetNames.size() >= expectedConfigSetNames.size());
    assertTrue("ConfigNames should include :" + expectedConfigSetNames, retrievedConfigSetNames.containsAll(expectedConfigSetNames));

    SolrException e = expectThrows(SolrException.class, () -> CollectionAdminRequest.createCategoryRoutedAlias(getAlias(), categoryField, maxCardinality,
        CollectionAdminRequest.createCollection("_unused_", configName, 1, 1)
            .setMaxShardsPerNode(2))
        .setMustMatch(mustMatchRegex)
        .process(solrClient)
    );

    assertTrue("Create Alias should fail since router.mustMatch must be a valid regular expression",
        e.getMessage().contains("router.mustMatch must be a valid regular expression"));
  }

  @Slow
  @Test
  public void testMaxCardinality() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);

    final int maxCardinality = 2; // max cardinality for current test

    // Start with one collection manually created (and use higher numShards & replicas than we'll use for others)
    //  This tests we may pre-create the collection and it's acceptable.
    final String colVogon = getAlias() + "__CRA__" + SHIPS[0];

    // we expect changes ensuring a legal collection name.
    final String colHoG = getAlias() + "__CRA__" + SHIPS[1].replaceAll("\\s", "_");

    List<String> retrievedConfigSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();
    List<String> expectedConfigSetNames = Arrays.asList("_default", configName);

    // config sets leak between tests so we can't be any more specific than this on the next 2 asserts
    assertTrue("We expect at least 2 configSets",
        retrievedConfigSetNames.size() >= expectedConfigSetNames.size());
    assertTrue("ConfigNames should include :" + expectedConfigSetNames, retrievedConfigSetNames.containsAll(expectedConfigSetNames));

    CollectionAdminRequest.createCategoryRoutedAlias(getAlias(), categoryField, maxCardinality,
        CollectionAdminRequest.createCollection("_unused_", configName, 1, 1)
            .setMaxShardsPerNode(2))
        .process(solrClient);

    // now we index a document
    addDocsAndCommit(true, newDoc(SHIPS[0]));
    //assertDocRoutedToCol(lastDocId, col23rd);

    String uninitialized = getAlias() + "__CRA__" + CategoryRoutedAlias.UNINITIALIZED;
    assertInvariants(colVogon, uninitialized);

    addDocsAndCommit(true, newDoc(SHIPS[1]));

    assertInvariants(colVogon, colHoG);

    // should fail since max cardinality is reached
    testFailedDocument(newDoc(SHIPS[2]), "Max cardinality");
    assertInvariants(colVogon, colHoG);
  }


  /**
   * Test that the Update Processor Factory routes documents to leader shards and thus
   * avoids the possibility of introducing an extra hop to find the leader.
   *
   * @throws Exception when it blows up unexpectedly :)
   */
  @Slow
  @Test
  @LogLevel("org.apache.solr.update.processor.TrackingUpdateProcessorFactory=DEBUG")
  public void testSliceRouting() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);

    // each collection has 4 shards with 3 replicas for 12 possible destinations
    // 4 of which are leaders, and 8 of which should fail this test.
    final int numShards = 1 + random().nextInt(4);
    final int numReplicas = 1 + random().nextInt(3);
    CollectionAdminRequest.createCategoryRoutedAlias(getAlias(), categoryField, 20,
        CollectionAdminRequest.createCollection("_unused_", configName, numShards, numReplicas)
            .setMaxShardsPerNode(numReplicas))
        .process(solrClient);

    // cause some collections to be created
    assertUpdateResponse(solrClient.add(getAlias(), new SolrInputDocument("id","1",categoryField, SHIPS[0])));
    assertUpdateResponse(solrClient.add(getAlias(), new SolrInputDocument("id","2",categoryField, SHIPS[1])));
    assertUpdateResponse(solrClient.add(getAlias(), new SolrInputDocument("id","3",categoryField, SHIPS[2])));
    assertUpdateResponse(solrClient.commit(getAlias()));

    // wait for all the collections to exist...

    waitColAndAlias(getAlias(), "__CRA__", SHIPS[0], numShards);
    waitColAndAlias(getAlias(), "__CRA__", noSpaces(SHIPS[1]), numShards);
    waitColAndAlias(getAlias(), "__CRA__", noSpaces(SHIPS[2]), numShards);

    // at this point we now have 3 collections with 4 shards each, and 3 replicas per shard for a total of
    // 36 total replicas, 1/3 of which are leaders. We will add 3 docs and each has a 33% chance of hitting a
    // leader randomly and not causing a failure if the code is broken, but as a whole this test will therefore only have
    // about a 3.6% false positive rate (0.33^3). If that's not good enough, add more docs or more replicas per shard :).

    final String trackGroupName = getTrackUpdatesGroupName();
    final List<UpdateCommand> updateCommands;
    try {
      TrackingUpdateProcessorFactory.startRecording(trackGroupName);

      ModifiableSolrParams params = params("post-processor", "tracking-" + trackGroupName);
      List<SolrInputDocument> list = Arrays.asList(
          sdoc("id", "4", categoryField, SHIPS[0]),
          sdoc("id", "5", categoryField, SHIPS[1]),
          sdoc("id", "6", categoryField, SHIPS[2]));
      Collections.shuffle(list, random()); // order should not matter here
      assertUpdateResponse(add(getAlias(), list,
          params));
    } finally {
      updateCommands = TrackingUpdateProcessorFactory.stopRecording(trackGroupName);
    }
    assertRouting(numShards, updateCommands);
  }


  private void assertInvariants(String... expectedColls) throws IOException, SolrServerException {
    final int expectNumFound = lastDocId - numDocsDeletedOrFailed; //lastDocId is effectively # generated docs

    List<String> cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(getAlias());
    cols = new ArrayList<>(cols);
    cols.sort(String::compareTo); // don't really care about the order here.
    assert !cols.isEmpty();

    int totalNumFound = 0;
    for (String col : cols) {
      final QueryResponse colResponse = solrClient.query(col, params(
          "q", "*:*",
          "rows", "0"));
      long numFound = colResponse.getResults().getNumFound();
      if (numFound > 0) {
        totalNumFound += numFound;
      }
    }
    final QueryResponse colResponse = solrClient.query(getAlias(), params(
        "q", "*:*",
        "rows", "0"));
    long aliasNumFound = colResponse.getResults().getNumFound();
    List<String> actual = Arrays.asList(expectedColls);
    actual.sort(String::compareTo);
    assertArrayEquals("Expected " + expectedColls.length + " collections, found " + cols.size() + ":\n" +
            cols + " vs \n" + actual, expectedColls, cols.toArray());
    assertEquals("Expected collections and alias to have same number of documents",
        aliasNumFound, totalNumFound);
    assertEquals("Expected to find " + expectNumFound + " docs but found " + aliasNumFound,
        expectNumFound, aliasNumFound);
  }

  private SolrInputDocument newDoc(String routedValue) {
    if (routedValue != null) {
      return sdoc("id", Integer.toString(++lastDocId),
          categoryField, routedValue,
          intField, "0"); // always 0
    } else {
      return sdoc("id", Integer.toString(++lastDocId),
          intField, "0"); // always 0
    }
  }

  @Override
  public String getAlias() {
    return "myAlias";
  }

  @Override
  public CloudSolrClient getSolrClient() {
    return solrClient;
  }


  public static class IncrementURPFactory extends FieldMutatingUpdateProcessorFactory {

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return FieldValueMutatingUpdateProcessor.valueMutator(getSelector(), next,
          (src) -> Integer.valueOf(src.toString()) + 1);
    }
  }

  private void testFailedDocument(SolrInputDocument sdoc, String errorMsg) throws SolrServerException, IOException {
    try {
      final UpdateResponse resp = solrClient.add(getAlias(), sdoc);
      // if we have a TolerantUpdateProcessor then we see it there)
      final Object errors = resp.getResponseHeader().get("errors"); // Tolerant URP
      assertNotNull(errors);
      assertTrue("Expected to find " + errorMsg + " in errors: " + errors.toString(),errors.toString().contains(errorMsg));
    } catch (SolrException e) {
      assertTrue(e.getMessage().contains(errorMsg));
    }
    ++numDocsDeletedOrFailed;
  }

}
