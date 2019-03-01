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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
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
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-13059")
public class TimeRoutedAliasUpdateProcessorTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String alias = "myalias";
  private static final String alias2 = "myalias2";
  private static final String timeField = "timestamp_dt";
  private static final String intField = "integer_i";

  private static CloudSolrClient solrClient;

  private int lastDocId = 0;
  private int numDocsDeletedOrFailed = 0;

  @Before
  public void doBefore() throws Exception {
    configureCluster(4).configure();
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
  @LogLevel("org.apache.solr.update.processor.TimeRoutedAlias=DEBUG;org.apache.solr.cloud=DEBUG")
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void test() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);

    // Start with one collection manually created (and use higher numShards & replicas than we'll use for others)
    //  This tests we may pre-create the collection and it's acceptable.
    final String col23rd = alias + "_2017-10-23";
    CollectionAdminRequest.createCollection(col23rd, configName, 2, 2)
        .setMaxShardsPerNode(2)
        .withProperty(TimeRoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP, alias)
        .process(solrClient);

    cluster.waitForActiveCollection(col23rd, 2, 4);

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

    // TODO: fix SOLR-13059, a where this wait isn't working ~0.3% of the time.
    waitCol(1,configName);
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
            "    'name':'tracking-testSliceRouting', 'class':'solr.TrackingUpdateProcessorFactory', 'group':'" + getTrackUpdatesGroupName() + "'" +
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
  @Test
  @LogLevel("org.apache.solr.update.processor.TrackingUpdateProcessorFactory=DEBUG")
  public void testSliceRouting() throws Exception {
    String configName = getSaferTestName();
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
    waitColAndAlias("2017-10-23", numShards, alias);
    waitColAndAlias("2017-10-24", numShards, alias);
    waitColAndAlias("2017-10-25", numShards, alias);

    // at this point we now have 3 collections with 4 shards each, and 3 replicas per shard for a total of
    // 36 total replicas, 1/3 of which are leaders. We will add 3 docs and each has a 33% chance of hitting a
    // leader randomly and not causing a failure if the code is broken, but as a whole this test will therefore only have
    // about a 3.6% false positive rate (0.33^3). If that's not good enough, add more docs or more replicas per shard :).

    final String trackGroupName = getTrackUpdatesGroupName();
    final List<UpdateCommand> updateCommands;
    try {
      TrackingUpdateProcessorFactory.startRecording(trackGroupName);

      // cause some collections to be created

      ModifiableSolrParams params = params("post-processor", "tracking-" + trackGroupName);
      assertUpdateResponse(add(alias, Arrays.asList(
          sdoc("id", "2", "timestamp_dt", "2017-10-24T00:00:00Z"),
          sdoc("id", "3", "timestamp_dt", "2017-10-25T00:00:00Z"),
          sdoc("id", "4", "timestamp_dt", "2017-10-23T00:00:00Z")),
          params));
    } finally {
      updateCommands = TrackingUpdateProcessorFactory.stopRecording(trackGroupName);
    }

    try (CloudSolrClient cloudSolrClient = getCloudSolrClient(cluster)) {
      ClusterStateProvider clusterStateProvider = cloudSolrClient.getClusterStateProvider();
      clusterStateProvider.connect();
      Set<String> leaders = getLeaderCoreNames(clusterStateProvider.getClusterState());
      assertEquals("should have " + 3 * numShards + " leaders, " + numShards + " per collection", 3 * numShards, leaders.size());

      assertEquals(3, updateCommands.size());
      for (UpdateCommand updateCommand : updateCommands) {
        String node = (String) updateCommand.getReq().getContext().get(TrackingUpdateProcessorFactory.REQUEST_NODE);
        assertTrue("Update was not routed to a leader (" + node + " not in list of leaders" + leaders, leaders.contains(node));
      }
    }
  }

  /** @see TrackingUpdateProcessorFactory */
  private String getTrackUpdatesGroupName() {
    return getSaferTestName();
  }

  @Test
  @Slow
  public void testPreemptiveCreation() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);

    final int numShards = 1 ;
    final int numReplicas = 1 ;
    CollectionAdminRequest.createTimeRoutedAlias(alias, "2017-10-23T00:00:00Z", "+1DAY", timeField,
        CollectionAdminRequest.createCollection("_unused_", configName, numShards, numReplicas)
            .setMaxShardsPerNode(numReplicas)).setPreemptiveCreateWindow("3HOUR")
        .process(solrClient);

    // needed to verify that preemptive creation in one alias doesn't inhibit preemptive creation in another
    CollectionAdminRequest.createTimeRoutedAlias(alias2, "2017-10-23T00:00:00Z", "+1DAY", timeField,
        CollectionAdminRequest.createCollection("_unused_", configName, numShards, numReplicas)
            .setMaxShardsPerNode(numReplicas)).setPreemptiveCreateWindow("3HOUR")
        .process(solrClient);

    addOneDocSynchCreation(numShards, alias);
    addOneDocSynchCreation(numShards, alias2);

    List<String> cols;
    ModifiableSolrParams params = params();

    // Using threads to ensure that two TRA's  are simultaneously preemptively creating and don't
    // interfere with each other
    ExecutorService executorService = ExecutorUtil.newMDCAwareCachedThreadPool("TimeRoutedAliasProcessorTestx-testPreemptiveCreation");

    Exception[] threadExceptions = new Exception[2];
    boolean[] threadStarted = new boolean[2];
    boolean[] threadFinished = new boolean[2];
    try {
      CountDownLatch starter = new CountDownLatch(1);
      executorService.submit(() -> {
        threadStarted[0] = true;
        try {
          starter.await();
          concurrentUpdates(params, alias);
        } catch (Exception e) {
          threadExceptions[0] = e;
        }
        threadFinished[0] = true;
      });

      executorService.submit(() -> {
        threadStarted[1] = true;
        try {
          starter.await();
          concurrentUpdates(params, alias2);
        } catch (Exception e) {
          threadExceptions[1] = e;
        }
        threadFinished[1] = true;
      });
      starter.countDown();
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executorService);
    }

    // threads are known to be terminated by now, check for exceptions
    for (Exception threadException : threadExceptions) {
      if (threadException != null) {
        Thread.sleep(5000); // avoid spurious fails due to TRA thread not done yet
        //noinspection ThrowFromFinallyBlock
        throw threadException;
      }
    }

    // just for confidence that there's nothing dodgy about how the threads executed.
    assertTrue(threadStarted[0]);
    assertTrue(threadStarted[1]);
    assertTrue(threadFinished[0]);
    assertTrue(threadFinished[1]);

    // if one of these times out then the test has failed due to interference between aliases
    waitColAndAlias("2017-10-26", numShards, alias);
    waitColAndAlias("2017-10-26", numShards, alias2);

    // after this we can ignore alias2
    checkPreemptiveCase1(alias);
    checkPreemptiveCase1(alias2);

    // Some designs contemplated with close hooks were not properly restricted to the core and would have
    // failed after other cores with other TRAs were stopped. Make sure that we don't fall into that trap in
    // the future. The basic problem with a close hook solution is that one either winds up putting the
    // executor on the TRAUP where it's duplicated/initiated for every request, or putting it at the class level
    // in which case the hook will remove it for all TRA's which can pass a single TRA test nicely but is not safe
    // where multiple TRA's might come and go.
    //
    // Start and stop some cores that have TRA's... 2x2 used to ensure every jetty gets at least one

    CollectionAdminRequest.createTimeRoutedAlias("foo", "2017-10-23T00:00:00Z", "+1DAY", timeField,
        CollectionAdminRequest.createCollection("_unused_", configName, 2, 2)
            .setMaxShardsPerNode(numReplicas)).setPreemptiveCreateWindow("3HOUR")
        .process(solrClient);

    waitColAndAlias("2017-10-23",2, "foo");
    waitCoreCount("foo_2017-10-23", 1); // prove this works, for confidence in deletion checking below.
    assertUpdateResponse(solrClient.add("foo",
        sdoc("id","1","timestamp_dt", "2017-10-23T00:00:00Z") // no extra collections should be created
    ));
    assertUpdateResponse(solrClient.commit("foo"));

    List<String> foo = solrClient.getClusterStateProvider().resolveAlias("foo");

    CollectionAdminRequest.deleteAlias("foo").process(solrClient);

    for (String colName : foo) {
      CollectionAdminRequest.deleteCollection(colName).process(solrClient);
      waitCoreCount(colName, 0);
    }

    // if the design for terminating our executor is correct create/delete above will not cause failures below
    // continue testing...

    // now test with pre-create window longer than time slice, and forcing multiple creations.
    CollectionAdminRequest.setAliasProperty(alias)
        .addProperty(TimeRoutedAlias.ROUTER_PREEMPTIVE_CREATE_MATH, "3DAY").process(solrClient);

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "7", "timestamp_dt", "2017-10-25T23:01:00Z")), // should cause preemptive creation of 10-27 now
        params));
    assertUpdateResponse(solrClient.commit(alias));
    waitColAndAlias("2017-10-27", numShards, alias);

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(5,cols.size()); // only one created in async case
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 5, alias);
    assertNumDocs("2017-10-26", 0, alias);
    assertNumDocs("2017-10-27", 0, alias);

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "8", "timestamp_dt", "2017-10-25T23:01:00Z")), // should cause preemptive creation of 10-28 now
        params));
    assertUpdateResponse(solrClient.commit(alias));
    waitColAndAlias("2017-10-27", numShards, alias);
    waitColAndAlias("2017-10-28", numShards, alias);

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(6,cols.size()); // Subsequent documents continue to create up to limit
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 6, alias);
    assertNumDocs("2017-10-26", 0, alias);
    assertNumDocs("2017-10-27", 0, alias);
    assertNumDocs("2017-10-28", 0, alias);

    QueryResponse resp;
    resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "10"));
    assertEquals(8, resp.getResults().getNumFound());

    assertUpdateResponse(add(alias, Arrays.asList(
        sdoc("id", "9", "timestamp_dt", "2017-10-27T23:01:00Z"), // should cause preemptive creation

        // If these are not ignored properly this test will fail during cleanup with a message about router.name being
        // required. This happens because the test finishes while overseer threads are still trying to invoke maintain
        // after the @After method has deleted collections and emptied out the aliases.... this leaves the maintain
        // command cloning alias properties Aliases.EMPTY and thus not getting a value from router.name
        // (normally router.name == 'time') The check for non-blank router.name  happens to be the first validation.
        // There is a small chance this could slip through without a fail occasionally, but it was 100% with just one
        // of these.
        sdoc("id", "10", "timestamp_dt", "2017-10-28T23:01:00Z"),  // should be ignored due to in progress creation
        sdoc("id", "11", "timestamp_dt", "2017-10-28T23:02:00Z"),  // should be ignored due to in progress creation
        sdoc("id", "12", "timestamp_dt", "2017-10-28T23:03:00Z")), // should be ignored due to in progress creation
        params));
    assertUpdateResponse(solrClient.commit(alias));
    waitColAndAlias("2017-10-29", numShards, alias);

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(7,cols.size());
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 6, alias);
    assertNumDocs("2017-10-26", 0, alias);
    assertNumDocs("2017-10-27", 1, alias);
    assertNumDocs("2017-10-28", 3, alias); // should get through even though preemptive creation ignored it.
    assertNumDocs("2017-10-29", 0, alias);

    resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "0"));
    assertEquals(12, resp.getResults().getNumFound());

    // Sych creation with an interval longer than the time slice for the alias..
    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "13", "timestamp_dt", "2017-10-30T23:03:00Z")), // lucky?
        params));
    assertUpdateResponse(solrClient.commit(alias));
    waitColAndAlias("2017-10-30", numShards, alias);
    waitColAndAlias("2017-10-31", numShards, alias); // spooky! async case arising in middle of sync creation!!

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(9,cols.size());
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 6, alias);
    assertNumDocs("2017-10-26", 0, alias);
    assertNumDocs("2017-10-27", 1, alias);
    assertNumDocs("2017-10-28", 3, alias); // should get through even though preemptive creation ignored it.
    assertNumDocs("2017-10-29", 0, alias);
    assertNumDocs("2017-10-30", 1, alias);
    assertNumDocs("2017-10-31", 0, alias);

    resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "0"));
    assertEquals(13, resp.getResults().getNumFound());

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "14", "timestamp_dt", "2017-10-31T23:01:00Z")), // should cause preemptive creation 11-01
        params));
    waitColAndAlias("2017-11-01", numShards, alias);

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "15", "timestamp_dt", "2017-10-31T23:01:00Z")), // should cause preemptive creation 11-02
        params));
    waitColAndAlias("2017-11-02", numShards, alias);

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "16", "timestamp_dt", "2017-10-31T23:01:00Z")), // should cause preemptive creation 11-03
        params));
    waitColAndAlias("2017-11-03", numShards, alias);

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "17", "timestamp_dt", "2017-10-31T23:01:00Z")), // should NOT cause preemptive creation 11-04
        params));

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertTrue("Preemptive creation beyond ROUTER_PREEMPTIVE_CREATE_MATH setting of 3DAY!",!cols.contains("myalias_2017-11-04"));

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "18", "timestamp_dt", "2017-11-01T23:01:00Z")), // should cause preemptive creation 11-04
        params));
    waitColAndAlias("2017-11-04",numShards, alias);

  }

  // used to verify a core has been deleted (count = 0)
  private void waitCoreCount(String collection, int count) {
    long start = System.nanoTime();
    CoreContainer coreContainer = cluster.getRandomJetty(random()).getCoreContainer();
    int coreFooCount;
    do {
      coreFooCount = 0;
      List<CoreDescriptor> coreDescriptors = coreContainer.getCoreDescriptors();
      for (CoreDescriptor coreDescriptor : coreDescriptors) {
        String collectionName = coreDescriptor.getCollectionName();
        if (collection.equals(collectionName)) {
          coreFooCount ++;
        }
      }
      if (NANOSECONDS.toSeconds(System.nanoTime() - start) > 10) {
        fail("took over 10 seconds after collection creation to update aliases");
      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }

    } while(coreFooCount != count);
  }

  private void concurrentUpdates(ModifiableSolrParams params, String alias) throws SolrServerException, IOException {
    // In this method we intentionally rely on timing of a race condition but the gap in collection creation time vs
    // requesting the list of aliases and adding a single doc should be very large (1-2 seconds vs a few ms so we
    // should always win the race) This is necessary  because we are testing that we can guard against specific race
    // conditions that happen while a collection is being created. To test this without timing sensitivity we would
    // need a means to pass a semaphore to the server that it can use to delay collection creation
    //
    // This method must NOT gain any Thread.sleep() statements, nor should it gain any long running operations
    assertUpdateResponse(add(alias, Arrays.asList(
        sdoc("id", "2", "timestamp_dt", "2017-10-24T00:00:00Z"),
        sdoc("id", "3", "timestamp_dt", "2017-10-25T00:00:00Z"),
        sdoc("id", "4", "timestamp_dt", "2017-10-23T00:00:00Z"),
        sdoc("id", "5", "timestamp_dt", "2017-10-25T23:00:00Z")), // should cause preemptive creation
        params));
    assertUpdateResponse(solrClient.commit(alias));

    List<String> colsT1;
    colsT1 = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(3, colsT1.size());
    assertTrue("Preemptive creation appears to not be asynchronous anymore", !colsT1.contains("myalias_2017-10-26"));
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 3, alias);

    // Here we quickly add another doc in a separate request, before the collection creation has completed.
    // This has the potential to incorrectly cause preemptive collection creation to run twice and create a
    // second collection. TimeRoutedAliasUpdateProcessor is meant to guard against this race condition.
    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "6", "timestamp_dt", "2017-10-25T23:01:00Z")), // might cause duplicate preemptive creation
        params));
    assertUpdateResponse(solrClient.commit(alias));
  }

  private void checkPreemptiveCase1(String alias) throws SolrServerException, IOException {
    List<String> cols;
    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertTrue("Preemptive creation happened twice and created a collection " +
        "further in the future than the configured time slice!",!cols.contains("myalias_2017-10-27"));

    assertEquals(4, cols.size());
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 4, alias);
    assertNumDocs("2017-10-26", 0, alias);
  }

  private void addOneDocSynchCreation(int numShards, String alias) throws SolrServerException, IOException, InterruptedException {
    // cause some collections to be created
    assertUpdateResponse(solrClient.add(alias,
        sdoc("id","1","timestamp_dt", "2017-10-25T00:00:00Z")
    ));
    assertUpdateResponse(solrClient.commit(alias));

    // wait for all the collections to exist...
    waitColAndAlias("2017-10-23", numShards, alias); // This one should have already existed from the alias creation
    waitColAndAlias("2017-10-24", numShards, alias); // Create 1
    waitColAndAlias("2017-10-25", numShards, alias); // Create 2nd synchronously (ensure this is not broken)

    // normal update, nothing special, no collection creation required.
    List<String> cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(3,cols.size());

    assertNumDocs("2017-10-23", 0, alias);
    assertNumDocs("2017-10-24", 0, alias);
    assertNumDocs("2017-10-25", 1, alias);
  }

  private void assertNumDocs(final String datePart, int expected, String alias) throws SolrServerException, IOException {
    QueryResponse resp = solrClient.query(alias + "_" + datePart, params(
        "q", "*:*",
        "rows", "10"));
    assertEquals(expected, resp.getResults().getNumFound());
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

  private void waitColAndAlias(final String datePart, int slices, String alias) throws InterruptedException {
    // collection to exist
    String collection = alias + "_" + datePart;
    waitCol(slices, collection);
    // and alias to be aware of collection
    long start = System.nanoTime(); // mumble mumble precommit mumble mumble...
    while (!haveCollection(alias, collection)) {
      if (NANOSECONDS.toSeconds(System.nanoTime() - start) > 10) {
        fail("took over 10 seconds after collection creation to update aliases");
      } else {
        Thread.sleep(500);
      }
    }
  }

  private boolean haveCollection(String alias, String collection) {
    // separated into separate lines to make it easier to track down an NPE that occurred once
    // 3000 runs if it shows up again...
    CloudSolrClient solrClient = cluster.getSolrClient();
    ZkStateReader zkStateReader = solrClient.getZkStateReader();
    Aliases aliases = zkStateReader.getAliases();
    Map<String, List<String>> collectionAliasListMap = aliases.getCollectionAliasListMap();
    List<String> strings = collectionAliasListMap.get(alias);
    return strings.contains(collection);
  }

  private void waitCol(int slices, String collection) {
    waitForState("waiting for collections to be created", collection,
        (liveNodes, collectionState) -> {
          if (collectionState == null) {
            // per predicate javadoc, this is what we get if the collection doesn't exist at all.
            return false;
          }
          Collection<Slice> activeSlices = collectionState.getActiveSlices();
          int size = activeSlices.size();
          return size == slices;
        });
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
            new DefaultSolrThreadFactory(getSaferTestName()));
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
  @SuppressWarnings("SameParameterValue")
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
