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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.LogLevel;
import org.junit.After;
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
    configureCluster(4).configure();
  }

  @Before
  public void doBefore() throws Exception {
    solrClient = getCloudSolrClient(cluster);
    //log this to help debug potential causes of problems
    System.out.println("SolrClient: " + solrClient);
    if (solrClient instanceof CloudSolrClient) {
      System.out.println(((CloudSolrClient) solrClient).getClusterStateProvider());
    }
  }

  @After
  public void doAfter() throws Exception {
    cluster.deleteAllCollections(); // deletes aliases too
    solrClient.close();
  }

  @AfterClass
  public static void finish() throws Exception {
    IOUtils.close(solrClient);
  }

  @Slow
  @Test
  @LogLevel("org.apache.solr.update.processor.TimeRoutedAlias=DEBUG;org.apache.solr.cloud=DEBUG")
  public void test() throws Exception {
    String configName = TimeRoutedAliasUpdateProcessorTest.configName + getTestName();
    createConfigSet(configName);

    // Start with one collection manually created (and use higher numShards & replicas than we'll use for others)
    //  This tests we may pre-create the collection and it's acceptable.
    final String col23rd = alias + "_2017-10-23";
    CollectionAdminRequest.createCollection(col23rd, configName, 2, 2)
        .setMaxShardsPerNode(2)
        .withProperty(TimeRoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP, alias)
        .process(solrClient);

    List<String> retrievedConfigSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();
    List<String> expectedConfigSetNames = Arrays.asList("_default", configName);

    // config sets leak between tests so we can't be any more specific than this on the next 2 asserts
    assertTrue("We expect at least 2 configSets",
        retrievedConfigSetNames.size() >= expectedConfigSetNames.size());
    assertTrue("ConfigNames should include :" + expectedConfigSetNames, retrievedConfigSetNames.containsAll(expectedConfigSetNames));

    CollectionAdminRequest.createTimeRoutedAlias(alias, "2017-10-23T00:00:00Z", "+1DAY", timeField,
        CollectionAdminRequest.createCollection("_unused_", configName, 1, 1)
            .setMaxShardsPerNode(2))
        .process(solrClient);

    // now we index a document
    assertUpdateResponse(solrClient.add(alias, newDoc(Instant.parse("2017-10-23T00:00:00Z"))));
    solrClient.commit(alias);
    //assertDocRoutedToCol(lastDocId, col23rd);
    assertInvariants(col23rd);

    // a document that is too old
    testFailedDocument(Instant.parse("2017-10-01T00:00:00Z"), "couldn't be routed");

    // a document which is too far into the future
    testFailedDocument(Instant.now().plus(30, ChronoUnit.MINUTES), "too far in the future");

    // add another collection with the precise name we expect, but don't add to alias explicitly.  When we add a document
    //   destined for this collection, Solr will see it already exists and add it to the alias.
    final String col24th = alias + "_2017-10-24";
    CollectionAdminRequest.createCollection(col24th, configName,  1, 1) // more shards and replicas now
        .withProperty(TimeRoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP, alias)
        .process(solrClient);

    // index 3 documents in a random fashion
    addDocsAndCommit(false, // send these to alias & collections
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
    //   remove from the alias
    CollectionAdminRequest.createAlias(alias, col24th).process(solrClient);
    //   delete the collection
    CollectionAdminRequest.deleteCollection(col23rd).process(solrClient);

    // now we're going to add documents that will trigger more collections to be created
    //   for 25th & 26th
    addDocsAndCommit(false, // send these to alias & collections
        newDoc(Instant.parse("2017-10-24T03:00:00Z")),
        newDoc(Instant.parse("2017-10-25T04:00:00Z")),
        newDoc(Instant.parse("2017-10-26T05:00:00Z")),
        newDoc(Instant.parse("2017-10-26T06:00:00Z"))
    );
    assertInvariants(alias + "_2017-10-26", alias + "_2017-10-25", col24th);


    // update metadata to auto-delete oldest collections
    CollectionAdminRequest.setAliasProperty(alias)
        .addProperty(TimeRoutedAlias.ROUTER_AUTO_DELETE_AGE, "-1DAY")  // thus usually keep 2 collections of a day size
        .process(solrClient);

    // add more docs, creating one new collection, but trigger ones prior to
    int numDocsToBeAutoDeleted = queryNumDocs(timeField+":[* TO \"2017-10-26T00:00:00Z\"}");
    addDocsAndCommit(true, // send these to alias only
        newDoc(Instant.parse("2017-10-26T07:00:00Z")), // existing
        newDoc(Instant.parse("2017-10-27T08:00:00Z")) // new
    );
    numDocsDeletedOrFailed += numDocsToBeAutoDeleted;
    assertInvariants(alias + "_2017-10-27", alias + "_2017-10-26");
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

    // manipulate the config...
    checkNoError(solrClient.request(new V2Request.Builder("/collections/" + configName + "/config")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{" +
            "  'set-user-property' : {'update.autoCreateFields':false}," + // no data driven
            "  'add-updateprocessor' : {" +
            "    'name':'tolerant', 'class':'solr.TolerantUpdateProcessorFactory'" +
            "  }," +
            // if other tracking tests are written, add another TUPF with a unique group name, don't re-use this one!
            // See TrackingUpdateProcessorFactory javadocs for details...
            "  'add-updateprocessor' : {" +
            "    'name':'tracking-testSliceRouting', 'class':'solr.TrackingUpdateProcessorFactory', 'group':'testSliceRouting'" +
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

  /**
   * Test that the Tracking Update Processor Factory routes documents to leader shards and thus
   * avoids the possibility of introducing an extra hop to find the leader.
   *
   * @throws Exception when it blows up unexpectedly :)
   */
  @Slow
  @Nightly
  @Test
  public void testSliceRouting() throws Exception {
    String configName = TimeRoutedAliasUpdateProcessorTest.configName + getTestName();
    createConfigSet(configName);

    // each collection has 4 shards with 3 replicas for 12 possible destinations
    // 4 of which are leaders, and 8 of which should fail this test.
    final int numShards = 1 + random().nextInt(4);
    final int numReplicas = 1 + random().nextInt(3);
    CollectionAdminRequest.createTimeRoutedAlias(alias, "2017-10-23T00:00:00Z", "+1DAY", timeField,
        CollectionAdminRequest.createCollection("_unused_", configName, numShards, numReplicas)
            .setMaxShardsPerNode(numReplicas))
        .process(solrClient);

    // cause some collections to be created
    assertUpdateResponse(solrClient.add(alias, new SolrInputDocument("id","1","timestamp_dt", "2017-10-25T00:00:00Z")));
    assertUpdateResponse(solrClient.commit(alias));

    // wait for all the collections to exist...
    waitCol("2017-10-23", numShards);
    waitCol("2017-10-24", numShards);
    waitCol("2017-10-25", numShards);

    // at this point we now have 3 collections with 4 shards each, and 3 replicas per shard for a total of
    // 36 total replicas, 1/3 of which are leaders. We will add 3 docs and each has a 33% chance of hitting a
    // leader randomly and not causing a failure if the code is broken, but as a whole this test will therefore only have
    // about a 3.6% false positive rate (0.33^3). If that's not good enough, add more docs or more replicas per shard :).

    try {
      TrackingUpdateProcessorFactory.startRecording(getTestName());

      // cause some collections to be created

      ModifiableSolrParams params = params("post-processor", "tracking-" + getTestName());
      assertUpdateResponse(add(alias, Arrays.asList(
          sdoc("id", "2", "timestamp_dt", "2017-10-24T00:00:00Z"),
          sdoc("id", "3", "timestamp_dt", "2017-10-25T00:00:00Z"),
          sdoc("id", "4", "timestamp_dt", "2017-10-23T00:00:00Z")),
          params));
    } finally {
      TrackingUpdateProcessorFactory.stopRecording(getTestName());
    }

    try (CloudSolrClient cloudSolrClient = getCloudSolrClient(cluster)) {
      ClusterStateProvider clusterStateProvider = cloudSolrClient.getClusterStateProvider();
      clusterStateProvider.connect();
      Set<String> leaders = getLeaderCoreNames(clusterStateProvider.getClusterState());
      assertEquals("should have " + 3 * numShards + " leaders, " + numShards + " per collection", 3 * numShards, leaders.size());

      List<UpdateCommand> updateCommands = TrackingUpdateProcessorFactory.commandsForGroup(getTestName());
      assertEquals(3, updateCommands.size());
      for (UpdateCommand updateCommand : updateCommands) {
        String node = (String) updateCommand.getReq().getContext().get(TrackingUpdateProcessorFactory.REQUEST_NODE);
        assertTrue("Update was not routed to a leader (" + node + " not in list of leaders" + leaders, leaders.contains(node));
      }
    }
  }

  private Set<String> getLeaderCoreNames(ClusterState clusterState) {
    Set<String> leaders = new TreeSet<>(); // sorted just to make it easier to read when debugging...
    List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
    for (JettySolrRunner jettySolrRunner : jettySolrRunners) {
      List<CoreDescriptor> coreDescriptors = jettySolrRunner.getCoreContainer().getCoreDescriptors();
      for (CoreDescriptor core : coreDescriptors) {
        String nodeName = jettySolrRunner.getNodeName();
        String collectionName = core.getCollectionName();
        DocCollection collectionOrNull = clusterState.getCollectionOrNull(collectionName);
        List<Replica> leaderReplicas = collectionOrNull.getLeaderReplicas(nodeName);
        if (leaderReplicas != null) {
          for (Replica leaderReplica : leaderReplicas) {
            leaders.add(leaderReplica.getCoreName());
          }
        }
      }
    }
    return leaders;
  }

  private void waitCol(final String datePart, int slices) {
    waitForState("waiting for collections to be created",alias + "_" + datePart,
        (liveNodes, collectionState) -> collectionState.getActiveSlices().size() == slices);
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
  private void addDocsAndCommit(boolean aliasOnly, SolrInputDocument... solrInputDocuments) throws Exception {
    // we assume all docs will be added (none too old/new to cause exception)
    Collections.shuffle(Arrays.asList(solrInputDocuments), random());

    // this is a list of the collections & the alias name.  Use to pick randomly where to send.
    //   (it doesn't matter where we send docs since the alias is honored at the URP level)
    List<String> collections = new ArrayList<>();
    collections.add(alias);
    if (!aliasOnly) {
      collections.addAll(new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias));
    }

    int commitWithin = random().nextBoolean() ? -1 : 500; // if -1, we commit explicitly instead

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
      String docsQ =
          "{!terms f=id}"
          + Arrays.stream(solrInputDocuments).map(d -> d.getFieldValue("id").toString())
              .collect(Collectors.joining(","));
      int numDocs = queryNumDocs(docsQ);
      if (numDocs == solrInputDocuments.length) {
        System.err.println("Docs committed sooner than expected.  Bug or slow test env?");
        return;
      }
      // wait until it's committed
      Thread.sleep(commitWithin);
      for (int idx = 0; idx < 100; ++idx) { // Loop for up to 10 seconds waiting for commit to catch up
        numDocs = queryNumDocs(docsQ);
        if (numDocs == solrInputDocuments.length) break;
        Thread.sleep(100);
      }

      assertEquals("not committed.  Bug or a slow test?",
          solrInputDocuments.length, numDocs);
    }
  }

  private void assertUpdateResponse(UpdateResponse rsp) {
    // use of TolerantUpdateProcessor can cause non-thrown "errors" that we need to check for
    List errors = (List) rsp.getResponseHeader().get("errors");
    assertTrue("Expected no errors: " + errors,errors == null || errors.isEmpty());
  }

  private int queryNumDocs(String q) throws SolrServerException, IOException {
    return (int) solrClient.query(alias, params("q", q, "rows", "0")).getResults().getNumFound();
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
      final Instant colStartInstant = TimeRoutedAlias.parseInstantFromCollectionName(alias, col);
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

  /** Adds the docs to Solr via {@link #solrClient} with the params */
  private static UpdateResponse add(String collection, Collection<SolrInputDocument> docs, SolrParams params) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    if (params != null) {
      req.setParams(new ModifiableSolrParams(params));// copy because will be modified
    }
    req.add(docs);
    return req.process(solrClient, collection);
  }

  @Test
  public void testParse() {
    assertEquals(Instant.parse("2017-10-02T03:04:05Z"),
      TimeRoutedAlias.parseInstantFromCollectionName(alias, alias + "_2017-10-02_03_04_05"));
    assertEquals(Instant.parse("2017-10-02T03:04:00Z"),
      TimeRoutedAlias.parseInstantFromCollectionName(alias, alias + "_2017-10-02_03_04"));
    assertEquals(Instant.parse("2017-10-02T03:00:00Z"),
      TimeRoutedAlias.parseInstantFromCollectionName(alias, alias + "_2017-10-02_03"));
    assertEquals(Instant.parse("2017-10-02T00:00:00Z"),
      TimeRoutedAlias.parseInstantFromCollectionName(alias, alias + "_2017-10-02"));
  }

  public static class IncrementURPFactory extends FieldMutatingUpdateProcessorFactory {

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return FieldValueMutatingUpdateProcessor.valueMutator( getSelector(), next,
          (src) -> Integer.valueOf(src.toString()) + 1);
    }
  }

}
