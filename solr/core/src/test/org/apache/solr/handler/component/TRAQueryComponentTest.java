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

package org.apache.solr.handler.component;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessorTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TRAQueryComponentTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TRAConfigName = "tra-distrib";
  private static final String optimizerConfName = "tra-query-component";
  private static final String alias = "myalias";
  private static final String timeField = "timestamp_dt";
  private static final String intField = "integer_i";
  private static final String SOLR_END_POINT = "/solr/";

  private static CloudSolrClient solrClient;

  private int lastDocId = 0;

  @Before
  public void doBefore() throws Exception {
    configureCluster(4)
        .addConfig(optimizerConfName, configset(TRAConfigName))
        .configure();
    solrClient = getCloudSolrClient(cluster);
    //log this to help debug potential causes of problems
    log.info("SolrClient: {}", solrClient);
    log.info("ClusterStateProvider {}",solrClient.getClusterStateProvider());
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
  public void testSortSearch() throws Exception {

    createConfigSet(TRAConfigName);

    // Start with one collection manually created (and use higher numShards & replicas than we'll use for others)
    //  This tests we may pre-create the collection and it's acceptable.
    final String col23rd = alias + "_2017-10-23";
    CollectionAdminRequest.createCollection(col23rd, TRAConfigName, 2, 2)
        .setMaxShardsPerNode(2)
        .withProperty(TimeRoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP, alias)
        .process(solrClient);

    cluster.waitForActiveCollection(col23rd, 2, 4);

    List<String> retrievedConfigSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();
    List<String> expectedConfigSetNames = Arrays.asList("_default", "tra-distrib");

    // config sets leak between tests so we can't be any more specific than this on the next 2 asserts
    assertTrue("We expect at least 2 configSets",
        retrievedConfigSetNames.size() >= expectedConfigSetNames.size());
    assertTrue("ConfigNames should include :" + expectedConfigSetNames, retrievedConfigSetNames.containsAll(expectedConfigSetNames));

    CollectionAdminRequest.createTimeRoutedAlias(alias, "2017-10-23T00:00:00Z", "+1DAY", timeField,
        CollectionAdminRequest.createCollection("_unused_", "tra-distrib", 1, 1)
            .setMaxShardsPerNode(2))
        .process(solrClient);

    // now we index a document
    assertUpdateResponse(solrClient.add(alias, newDoc(Instant.parse("2017-10-23T00:00:00Z"))));
    solrClient.commit(alias);

    // now we're going to add documents that will trigger more collections to be created
    //   for 25th & 26th
    assertUpdateResponse(solrClient.add(alias, Arrays.asList(
        newDoc(Instant.parse("2017-10-24T03:00:00Z")),
        newDoc(Instant.parse("2017-10-25T04:00:00Z")),
        newDoc(Instant.parse("2017-10-26T05:00:00Z")),
        newDoc(Instant.parse("2017-10-26T06:00:00Z")),
        newDoc(Instant.parse("2017-10-26T07:00:00Z"))
    )));
    solrClient.commit(alias);

    // set debug to true to assert only right shards were queried
    final QueryResponse qResp = solrClient.query(alias, params("q", "*:*", "sort", timeField + " desc",
        "rows", "2", "debug", "true")
    );
    final String debugInfo = (String)qResp.getDebugMap().getOrDefault(RoutedAliasOptimizeQueryComponent.COMPONENT_NAME, "");
    assertNotNull("debug info was not added", debugInfo);
    List<String> shards = getShardsFromQueryDebug(debugInfo);
    assertTrue("was  expected to only get fields from '_2017-10-26', but instead got : " + shards,
        shards.stream().anyMatch(x -> x.contains("_2017-10-26")));
    assertEquals(6, qResp.getResults().getNumFound());
    assertEquals(1, shards.size());
    // assertEquals();
    assertEquals(Instant.parse("2017-10-26T07:00:00Z"), ((Date)qResp.getResults().get(0).get(timeField)).toInstant());
    assertEquals(Instant.parse("2017-10-26T06:00:00Z"), ((Date)qResp.getResults().get(1).get(timeField)).toInstant());
  }

  private void assertUpdateResponse(UpdateResponse rsp) {
    // use of TolerantUpdateProcessor can cause non-thrown "errors" that we need to check for
    List errors = (List) rsp.getResponseHeader().get("errors");
    assertTrue("Expected no errors: " + errors,errors == null || errors.isEmpty());
  }

  private SolrInputDocument newDoc(Instant timestamp) {
    return sdoc("id", Integer.toString(++lastDocId),
        timeField, timestamp.toString(),
        intField, "0"); // always 0
  }

  private void createConfigSet(String configName) throws SolrServerException, IOException {
    // First create a configSet
    // Then we create a collection with the name of the eventual config.
    // We configure it, and ultimately delete the collection, leaving a modified config-set behind.
    // Later we create the "real" collections referencing this modified config-set.

    this.checkClusterConfiguration();
    assertEquals(0, new ConfigSetAdminRequest.Create()
        .setConfigSetName(configName)
        .setBaseConfigSetName(optimizerConfName)
        .process(solrClient).getStatus());

    CollectionAdminRequest.createCollection(configName, configName, 1, 1).process(solrClient);

    // manipulate the config...
    checkNoError(solrClient.request(new V2Request.Builder("/collections/" + configName + "/config")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{" +
            "  'set-user-property' : {'update.autoCreateFields':false}," + // no data driven
            "  'add-updateprocessor' : {" +
            "    'name':'tolerant', 'class':'solr.TolerantUpdateProcessorFactory'" +
            "  }," +
            // See TrackingUpdateProcessorFactory javadocs for details...
            "  'add-updateprocessor' : {" +
            "    'name':'tracking-testSliceRouting', 'class':'solr.TrackingUpdateProcessorFactory', 'group':'" + getSaferTestName() + "'" +
            "  }," +
            "  'add-updateprocessor' : {" + // for testing
            "    'name':'inc', 'class':'" + TimeRoutedAliasUpdateProcessorTest.IncrementURPFactory.class.getName() + "'," +
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

  private void checkNoError(NamedList<Object> response) { //TODO rename
    Object errors = response.get("errorMessages");
    assertNull("" + errors, errors);
  }

  private List<String> getShardsFromQueryDebug(String debugInfo) {
    int idx = debugInfo.indexOf(": ");
    if (idx == -1) {
      return Collections.emptyList();
    }
    String shards = debugInfo.substring(debugInfo.indexOf(": ") + 2);

    return StrUtils.splitSmart(shards, ',').stream()
        .map(x -> x.substring(x.indexOf(SOLR_END_POINT) + SOLR_END_POINT.length()))
        .collect(Collectors.toList());
  }

}
