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
import java.util.List;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.api.collections.CategoryRoutedAlias;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
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
  private int numDocsDeletedOrFailed = 0;
  private static CloudSolrClient solrClient;

  @Before
  public void doBefore() throws Exception {
    configureCluster(4).configure();
    solrClient = getCloudSolrClient(cluster);
    //log this to help debug potential causes of problems
    log.info("SolrClient: {}", solrClient);
    log.info("ClusterStateProvider {}", solrClient.getClusterStateProvider());
  }

  @After
  public void doAfter() throws Exception {
    solrClient.close();
    shutdownCluster();
  }

  @AfterClass
  public static void finish() throws Exception {
    IOUtils.close(solrClient);
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
    final String colHoG = getAlias() + "__CRA__" + SHIPS[1].replaceAll("\\s", "_");
    final String colStunt = getAlias() + "__CRA__" + SHIPS[2].replaceAll("\\s", "_");
    final String colArk = getAlias() + "__CRA__" + SHIPS[3].replaceAll("-","_");
    final String colBistro = getAlias() + "__CRA__" + SHIPS[4].replaceAll("\\$", "_");

    List<String> retrievedConfigSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();
    List<String> expectedConfigSetNames = Arrays.asList("_default", configName);

    // config sets leak between tests so we can't be any more specific than this on the next 2 asserts
    assertTrue("We expect at least 2 configSets",
        retrievedConfigSetNames.size() >= expectedConfigSetNames.size());
    assertTrue("ConfigNames should include :" + expectedConfigSetNames, retrievedConfigSetNames.containsAll(expectedConfigSetNames));

    CollectionAdminRequest.createCategoryRoutedAlias(getAlias(), categoryField,
        CollectionAdminRequest.createCollection("_unused_", configName, 1, 1)
            .setMaxShardsPerNode(2))
        .process(solrClient);

    // now we index a document
    addDocsAndCommit(true, newDoc(SHIPS[0]));
    //assertDocRoutedToCol(lastDocId, col23rd);

    String uninitialized = getAlias() + "__CRA__" + CategoryRoutedAlias.UNINITIALIZED;
    assertInvariants(colVogon, uninitialized);

    addDocsAndCommit(true,
        newDoc(SHIPS[1]),
        newDoc(SHIPS[2]),
        newDoc(SHIPS[3]),
        newDoc(SHIPS[4]));

    assertInvariants(colVogon, colHoG, colStunt, colArk, colBistro);
  }

  private void createConfigSet(String configName) throws SolrServerException, IOException {
    // First create a configSet
    // Then we create a collection with the name of the eventual config.
    // We configure it, and ultimately delete the collection, leaving a modified config-set behind.
    // Later we create the "real" collections referencing this modified config-set.
    assertEquals(0, new ConfigSetAdminRequest.Create()
        .setConfigSetName(configName)
        .setBaseConfigSetName("_default")
        .process(solrClient).getStatus());

    CollectionAdminRequest.createCollection(configName, configName, 1, 1).process(solrClient);

    // TODO: fix SOLR-13059, a where this wait isn't working ~0.3% of the time.
    waitCol(1, configName);
    // manipulate the config...
    checkNoError(solrClient.request(new V2Request.Builder("/collections/" + configName + "/config")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{" +
            "  'set-user-property' : {'update.autoCreateFields':false}," + // no data driven
            "  'add-updateprocessor' : {" +
            "    'name':'tolerant', 'class':'solr.TolerantUpdateProcessorFactory'" +
            "  }," +
            "  'add-updateprocessor' : {" + // for testing
            "    'name':'inc', 'class':'" + IncrementURPFactory.class.getName() + "'," +
            "    'fieldName':'" + intField + "'" +
            "  }," +
            "}").build()));
    // only sometimes test with "tolerant" URP:
    final String urpNames = "inc" + (random().nextBoolean() ? ",tolerant" : "");
    checkNoError(solrClient.request(new V2Request.Builder("/collections/" + configName + "/config/params")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{" +
            "  'set' : {" +
            "    '_UPDATE' : {'processor':'" + urpNames + "'}" +
            "  }" +
            "}").build()));

    CollectionAdminRequest.deleteCollection(configName).process(solrClient);
    assertTrue(
        new ConfigSetAdminRequest.List().process(solrClient).getConfigSets()
            .contains(configName)
    );
  }

  private void checkNoError(NamedList<Object> response) {
    Object errors = response.get("errorMessages");
    assertNull("" + errors, errors);
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
    return sdoc("id", Integer.toString(++lastDocId),
        categoryField, routedValue,
        intField, "0"); // always 0
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

}
