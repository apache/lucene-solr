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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimeRoutedAliasUpdateProcessorTest extends SolrCloudTestCase {

  static final String configName = "timeConfig";
  static final String alias = "myalias";
  static final String timeField = "timestamp_dt";
  static final String intField = "integer_i";

  static SolrClient solrClient;

  private int lastDocId = 0;
  private int numDocsDeletedOrFailed = 0;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).configure();
    solrClient = getCloudSolrClient(cluster);
  }

  @AfterClass
  public static void finish() throws Exception {
    IOUtils.close(solrClient);
  }

  //TODO this is necessary when -Dtests.iters but why? Some other tests aren't affected
  @Before
  public void doBefore() throws Exception {
    for (String col : CollectionAdminRequest.listCollections(solrClient)) {
      CollectionAdminRequest.deleteCollection(col).process(solrClient);
    }
  }

  @Test
  public void test() throws Exception {
    // First create a config using REST API.  To do this, we create a collection with the name of the eventual config.
    // We configure it, and ultimately delete it the collection, leaving a config with the same name behind.
    // Then when we create the "real" collections referencing this config.
    CollectionAdminRequest.createCollection(configName, 1, 1).process(solrClient);
    // manipulate the config...
    checkNoError(solrClient.request(new V2Request.Builder("/collections/" + configName + "/config")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{" +
            "  'set-user-property' : {'timePartitionAliasName':'" + alias + "'}," + // no data driven
            "  'set-user-property' : {'update.autoCreateFields':false}," + // no data driven
            "  'add-updateprocessor' : {" +
            "    'name':'tolerant', 'class':'solr.TolerantUpdateProcessorFactory'" +
            "  }," +
            "  'add-updateprocessor' : {" + // for testing
            "    'name':'inc', 'class':'" + IncrementURPFactory.class.getName() + "'," +
            "    'fieldName':'" + intField + "'" +
            "  }," +
            "}").build()));
    // only sometimes test with "tolerant" URP
    final String urpNames = "inc" + (random().nextBoolean() ? ",tolerant" : "");
    checkNoError(solrClient.request(new V2Request.Builder("/collections/" + configName + "/config/params")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{" +
            "  'set' : {" +
            "    '_UPDATE' : {'processor':'" + urpNames + "'}" +
            "  }" +
            "}").build()));
    CollectionAdminRequest.deleteCollection(configName).process(solrClient);

    // start with one collection and an alias for it
    final String col23rd = alias + "_2017-10-23";
    CollectionAdminRequest.createCollection(col23rd, configName, 2, 2)
        .setMaxShardsPerNode(2)
        .withProperty(TimeRoutedAliasUpdateProcessor.TIME_PARTITION_ALIAS_NAME_CORE_PROP, alias)
        .process(solrClient);

    assertEquals("We only expect 2 configSets",
        Arrays.asList("_default", configName), new ConfigSetAdminRequest.List().process(solrClient).getConfigSets());

    CollectionAdminRequest.createAlias(alias, col23rd).process(solrClient);
    //TODO use SOLR-11617 client API to set alias metadata
    final ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();

    zkStateReader.aliasesHolder.applyModificationAndExportToZk(a ->
        a.cloneWithCollectionAliasMetadata(alias, TimeRoutedAliasUpdateProcessor.ROUTER_FIELD_METADATA, timeField)
        .cloneWithCollectionAliasMetadata(alias, "collection-create.collection.configName", configName)
        .cloneWithCollectionAliasMetadata(alias, "collection-create.numShards", "1")
        .cloneWithCollectionAliasMetadata(alias, "collection-create.replicationFactor", "1")
        .cloneWithCollectionAliasMetadata(alias, "router.interval", "+1DAY"));

    // now we index a document
    assertUpdateResponse(solrClient.add(alias, newDoc(Instant.parse("2017-10-23T00:00:00Z"))));
    solrClient.commit(alias);
    //assertDocRoutedToCol(lastDocId, col23rd);
    assertInvariants(col23rd);

    // a document that is too old
    testFailedDocument(Instant.parse("2017-10-01T00:00:00Z"), "couldn't be routed");

    // a document which is too far into the future
    testFailedDocument(Instant.now().plus(30, ChronoUnit.MINUTES), "too far in the future");

    // add another collection, add to alias  (soonest comes first)
    final String col24th = alias + "_2017-10-24";
    CollectionAdminRequest.createCollection(col24th, configName,  1, 1) // more shards and replicas now
        .withProperty("timePartitionAliasName", alias)
        .process(solrClient);
    CollectionAdminRequest.createAlias(alias, col24th + "," + col23rd).process(solrClient);

    // index 3 documents in a random fashion
    addDocsAndCommit(
        newDoc(Instant.parse("2017-10-23T00:00:00Z")),
        newDoc(Instant.parse("2017-10-24T01:00:00Z")),
        newDoc(Instant.parse("2017-10-24T02:00:00Z"))
    );
    assertInvariants(col24th, col23rd);

    // assert that the IncrementURP has updated all '0' to '1'
    final SolrDocumentList checkIncResults = solrClient.query(alias, params("q", "NOT " + intField + ":1")).getResults();
    assertEquals(checkIncResults.toString(), 0, checkIncResults.getNumFound());

    //delete a random document id; ensure we don't find it
    int idToDelete = 1 + random().nextInt(lastDocId);
    if (idToDelete == 2 || idToDelete == 3) { // these didn't make it
      idToDelete = 4;
    }
    assertUpdateResponse(solrClient.deleteById(alias, Integer.toString(idToDelete)));
    assertUpdateResponse(solrClient.commit(alias));
    numDocsDeletedOrFailed++;
    assertInvariants(col24th, col23rd);

    // delete the Oct23rd (save memory)...
    //   make sure we track that we are effectively deleting docs there
    numDocsDeletedOrFailed += solrClient.query(col23rd, params("q", "*:*", "rows", "0")).getResults().getNumFound();
    //   remove from alias
    CollectionAdminRequest.createAlias(alias, col24th).process(solrClient);
    //   delete the collection
    CollectionAdminRequest.deleteCollection(col23rd).process(solrClient);

    // now we're going to add documents that will trigger more collections to be created
    //   for 25th & 26th
    addDocsAndCommit(
        newDoc(Instant.parse("2017-10-24T03:00:00Z")),
        newDoc(Instant.parse("2017-10-25T04:00:00Z")),
        newDoc(Instant.parse("2017-10-26T05:00:00Z"))
    );
    assertInvariants(alias + "_2017-10-26", alias + "_2017-10-25", col24th);
  }

  private void testFailedDocument(Instant timestamp, String errorMsg) throws SolrServerException, IOException {
    try {
      final UpdateResponse resp = solrClient.add(alias, newDoc(timestamp));
      // if we have a TolerantUpdateProcessor then we see it there)
      final Object errors = resp.getResponseHeader().get("errors"); // Tolerant URP
      assertTrue(errors != null && errors.toString().contains(errorMsg));
    } catch (SolrException e) {
      assertTrue(e.getMessage().contains(errorMsg));
    }
    numDocsDeletedOrFailed++;
  }

  private void checkNoError(NamedList<Object> response) { //TODO rename
    Object errors = response.get("errorMessages");
    assertNull("" + errors, errors);
  }

  /** Adds these documents and commits, returning when they are committed.
   * We randomly go about this in different ways. */
  private void addDocsAndCommit(SolrInputDocument... solrInputDocuments) throws Exception {
    // we assume all docs will be added (none too old/new to cause exception)
    Collections.shuffle(Arrays.asList(solrInputDocuments), random());

    // this is a list of the collections & the alias name.  Use to pick randomly where to send.
    //   (it doesn't matter where we send docs since the alias is honored at the URP level)
    List<String> collections = new ArrayList<>();
    collections.add(alias);
    collections.addAll(new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias));

    int commitWithin = random().nextBoolean() ? -1 : 500; // if -1, we commit explicitly instead
    int numDocsBefore = queryNumDocs();
    if (random().nextBoolean()) {
      // Send in separate threads. Choose random collection & solrClient
      try (CloudSolrClient solrClient = getCloudSolrClient(cluster)) {
        ExecutorService exec = ExecutorUtil.newMDCAwareFixedThreadPool(1 + random().nextInt(2),
            new DefaultSolrThreadFactory(getTestName()));
        List<Future<UpdateResponse>> futures = new ArrayList<>(solrInputDocuments.length);
        for (SolrInputDocument solrInputDocument : solrInputDocuments) {
          String col = collections.get(random().nextInt(collections.size()));
          futures.add(exec.submit(() -> solrClient.add(col, solrInputDocument, commitWithin)));
        }
        for (Future<UpdateResponse> future : futures) {
          assertUpdateResponse(future.get());
        }
        // at this point there shouldn't be any tasks running
        assertEquals(0, exec.shutdownNow().size());
      }
    } else {
      // send in a batch.
      String col = collections.get(random().nextInt(collections.size()));
      try (CloudSolrClient solrClient = getCloudSolrClient(cluster)) {
        assertUpdateResponse(solrClient.add(col, Arrays.asList(solrInputDocuments), commitWithin));
      }
    }
    String col = collections.get(random().nextInt(collections.size()));
    if (commitWithin == -1) {
      solrClient.commit(col);
    } else {
      // check that it all got committed eventually
      int numDocs = queryNumDocs();
      if (numDocs == numDocsBefore + solrInputDocuments.length) {
        System.err.println("Docs committed sooner than expected.  Bug or slow test env?");
        return;
      }
      // wait until it's committed, plus some play time for commit to become visible
      Thread.sleep(commitWithin + 200);
      numDocs = queryNumDocs();
      assertEquals("not committed.  Bug or a slow test?",
          numDocsBefore + solrInputDocuments.length, numDocs);
    }
  }

  private void assertUpdateResponse(UpdateResponse rsp) {
    // use of TolerantUpdateProcessor can cause non-thrown "errors" that we need to check for
    List errors = (List) rsp.getResponseHeader().get("errors");
    assertTrue("Expected no errors: " + errors,errors == null || errors.isEmpty());
  }

  private int queryNumDocs() throws SolrServerException, IOException {
    return (int) solrClient.query(alias, params("q", "*:*", "rows", "0")).getResults().getNumFound();
  }

  private void assertInvariants(String... expectedColls) throws IOException, SolrServerException {
    final int expectNumFound = lastDocId - numDocsDeletedOrFailed; //lastDocId is effectively # generated docs

    final List<String> cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assert !cols.isEmpty();

    assertArrayEquals("expected reverse sorted",
        cols.stream().sorted(Collections.reverseOrder()).toArray(),
        cols.toArray());

    int totalNumFound = 0;
    Instant colEndInstant = null; // exclusive end
    for (String col : cols) { // ASSUMPTION: reverse sorted order
      final Instant colStartInstant = TimeRoutedAliasUpdateProcessor.parseInstantFromCollectionName(alias, col);
      final QueryResponse colStatsResp = solrClient.query(col, params(
          "q", "*:*",
          "rows", "0",
          "stats", "true",
          "stats.field", timeField));
      long numFound = colStatsResp.getResults().getNumFound();
      if (numFound > 0) {
        totalNumFound += numFound;
        final FieldStatsInfo timestampStats = colStatsResp.getFieldStatsInfo().get(timeField);
        assertTrue(colStartInstant.toEpochMilli() <= ((Date)timestampStats.getMin()).getTime());
        if (colEndInstant != null) {
          assertTrue(colEndInstant.toEpochMilli() > ((Date)timestampStats.getMax()).getTime());
        }
      }

      colEndInstant = colStartInstant; // next older segment will max out at our current start time
    }
    assertEquals(expectNumFound, totalNumFound);
    assertArrayEquals(expectedColls, cols.toArray());
  }

  private SolrInputDocument newDoc(Instant timestamp) {
    return sdoc("id", Integer.toString(++lastDocId),
        timeField, timestamp.toString(),
        intField, "0"); // always 0
  }

  @Test
  public void testParse() {
    assertEquals(Instant.parse("2017-10-02T03:04:05Z"),
      TimeRoutedAliasUpdateProcessor.parseInstantFromCollectionName(alias, alias + "_2017-10-02_03_04_05"));
    assertEquals(Instant.parse("2017-10-02T03:04:00Z"),
      TimeRoutedAliasUpdateProcessor.parseInstantFromCollectionName(alias, alias + "_2017-10-02_03_04"));
    assertEquals(Instant.parse("2017-10-02T03:00:00Z"),
      TimeRoutedAliasUpdateProcessor.parseInstantFromCollectionName(alias, alias + "_2017-10-02_03"));
    assertEquals(Instant.parse("2017-10-02T00:00:00Z"),
      TimeRoutedAliasUpdateProcessor.parseInstantFromCollectionName(alias, alias + "_2017-10-02"));
  }

  public static class IncrementURPFactory extends FieldMutatingUpdateProcessorFactory {

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return FieldValueMutatingUpdateProcessor.valueMutator( getSelector(), next,
          (src) -> Integer.valueOf(src.toString()) + 1);
    }
  }

}
