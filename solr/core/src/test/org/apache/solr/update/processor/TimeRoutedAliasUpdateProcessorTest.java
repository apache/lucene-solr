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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpClusterStateProvider;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.api.collections.RoutedAlias;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.RoutedAliasTypes.TIME;
import static org.apache.solr.cloud.api.collections.RoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP;
import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.ROUTER_START;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTIONS_ZKNODE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROPS_ZKNODE;

@LuceneTestCase.BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-13059")
public class TimeRoutedAliasUpdateProcessorTest extends RoutedAliasUpdateProcessorTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String alias = "myalias";
  private static final String alias2 = "myalias2";
  private static final String timeField = "timestamp_dt";
  public static final String TRA = TIME.getSeparatorPrefix();

  private  CloudSolrClient solrClient;

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
    if (null != solrClient) {
      solrClient.close();
    }
    shutdownCluster();
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
    final String col23rd = alias + TRA + "2017-10-23";
    CollectionAdminRequest.createCollection(col23rd, configName, 2, 2)
        .setMaxShardsPerNode(2)
        .withProperty(ROUTED_ALIAS_NAME_CORE_PROP, alias)
        .process(solrClient);

    cluster.waitForActiveCollection(col23rd, 2, 4);

    List<String> retrievedConfigSetNames = new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();
    List<String> expectedConfigSetNames = Arrays.asList("_default", configName);

    // config sets leak between tests so we can't be any more specific than this on the next 2 asserts
    assertTrue("We expect at least 2 configSets",
        retrievedConfigSetNames.size() >= expectedConfigSetNames.size());
    assertTrue("ConfigNames should include :" + expectedConfigSetNames, retrievedConfigSetNames.containsAll(expectedConfigSetNames));

    CollectionAdminRequest.createTimeRoutedAlias(alias, "2017-10-23T00:00:00Z", "+1DAY", getTimeField(),
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
    final String col24th = alias + TRA + "2017-10-24";
    CollectionAdminRequest.createCollection(col24th, configName,  1, 1) // more shards and replicas now
        .withProperty(ROUTED_ALIAS_NAME_CORE_PROP, alias)
        .process(solrClient);

    // index 3 documents in a random fashion
    addDocsAndCommit(false, // send these to alias & collections
        newDoc(Instant.parse("2017-10-23T00:00:00Z")),
        newDoc(Instant.parse("2017-10-24T01:00:00Z")),
        newDoc(Instant.parse("2017-10-24T02:00:00Z"))
    );
//    System.out.println(cluster.getRandomJetty(random()).getBaseUrl());
//    Thread.sleep(1000000);
    assertInvariants(col24th, col23rd);

    // assert that the IncrementURP has updated all '0' to '1'
    final SolrDocumentList checkIncResults = solrClient.query(alias, params("q", "NOT " + getIntField() + ":1")).getResults();
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
    assertInvariants(alias + TRA + "2017-10-26", alias + TRA + "2017-10-25", col24th);

    // verify that collection properties are set when the collections are created. Note: first 2 collections in
    // this test have a core property instead, of a collection property but that MUST continue to work as well
    // for back compatibility's reasons.
    Thread.sleep(1000);
    byte[] data = cluster.getZkClient()
        .getData(COLLECTIONS_ZKNODE + "/" + alias + TRA + "2017-10-26" + "/" + COLLECTION_PROPS_ZKNODE,null, null, true);
    assertNotNull(data);
    assertTrue(data.length > 0);
    @SuppressWarnings("unchecked")
    Map<String,String> props = (Map<String, String>) Utils.fromJSON(data);
    assertTrue(props.containsKey(ROUTED_ALIAS_NAME_CORE_PROP));
    assertEquals(alias,props.get(ROUTED_ALIAS_NAME_CORE_PROP));

    // update metadata to auto-delete oldest collections
    CollectionAdminRequest.setAliasProperty(alias)
        .addProperty(TimeRoutedAlias.ROUTER_AUTO_DELETE_AGE, "-1DAY")  // thus usually keep 2 collections of a day size
        .process(solrClient);

    // add more docs, creating one new collection, but trigger ones prior to
    int numDocsToBeAutoDeleted = queryNumDocs(getTimeField() +":[* TO \"2017-10-26T00:00:00Z\"}");
    addDocsAndCommit(true, // send these to alias only
        newDoc(Instant.parse("2017-10-26T07:00:00Z")), // existing
        newDoc(Instant.parse("2017-10-27T08:00:00Z")) // new
    );
    numDocsDeletedOrFailed += numDocsToBeAutoDeleted;
    assertInvariants(alias + TRA + "2017-10-27", alias + TRA + "2017-10-26");
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
    CollectionAdminRequest.createTimeRoutedAlias(alias, "2017-10-23T00:00:00Z", "+1DAY", getTimeField(),
        CollectionAdminRequest.createCollection("_unused_", configName, numShards, numReplicas)
            .setMaxShardsPerNode(numReplicas))
        .process(solrClient);

    // cause some collections to be created
    assertUpdateResponse(solrClient.add(alias, new SolrInputDocument("id","1","timestamp_dt", "2017-10-25T00:00:00Z")));
    assertUpdateResponse(solrClient.commit(alias));

    // wait for all the collections to exist...
    waitColAndAlias(alias, TRA, "2017-10-23", numShards);
    waitColAndAlias(alias, TRA, "2017-10-24", numShards);
    waitColAndAlias(alias, TRA, "2017-10-25", numShards);

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

    assertRouting(numShards, updateCommands);
  }

  @Test
  @Slow
  public void testPreemptiveCreation() throws Exception {
    String configName = getSaferTestName();
    createConfigSet(configName);

    final int numShards = 1 ;
    final int numReplicas = 1 ;
    CollectionAdminRequest.createTimeRoutedAlias(alias, "2017-10-23T00:00:00Z", "+1DAY", getTimeField(),
        CollectionAdminRequest.createCollection("_unused_", configName, numShards, numReplicas)
            .setMaxShardsPerNode(numReplicas)).setPreemptiveCreateWindow("3HOUR")
        .process(solrClient);

    // needed to verify that preemptive creation in one alias doesn't inhibit preemptive creation in another
    CollectionAdminRequest.createTimeRoutedAlias(alias2, "2017-10-23T00:00:00Z", "+1DAY", getTimeField(),
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
        throw threadException;
      }
    }

    // just for confidence that there's nothing dodgy about how the threads executed.
    assertTrue(threadStarted[0]);
    assertTrue(threadStarted[1]);
    assertTrue(threadFinished[0]);
    assertTrue(threadFinished[1]);

    // if one of these times out then the test has failed due to interference between aliases
    waitColAndAlias(alias, TRA, "2017-10-26", numShards);
    waitColAndAlias(alias2, TRA, "2017-10-26", numShards);

    // these next checks will be checking that a collection DID NOT get created asynchronously, there's
    // no way to wait for something that should never exist to not exist... so all we can do is sleep
    // a good while before checking
    Thread.sleep(5000);

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

    CollectionAdminRequest.createTimeRoutedAlias(getSaferTestName() + "foo", "2017-10-23T00:00:00Z", "+1DAY", getTimeField(),
        CollectionAdminRequest.createCollection("_unused_", configName, 2, 2)
            .setMaxShardsPerNode(numReplicas)).setPreemptiveCreateWindow("3HOUR")
        .process(solrClient);

    waitColAndAlias(getSaferTestName() + "foo", TRA, "2017-10-23",2);
    waitCoreCount(getSaferTestName() + "foo" + TRA + "2017-10-23", 4); // prove this works, for confidence in deletion checking below.
    assertUpdateResponse(solrClient.add(getSaferTestName() + "foo",
        sdoc("id","1","timestamp_dt", "2017-10-23T00:00:00Z") // no extra collections should be created
    ));
    assertUpdateResponse(solrClient.commit(getSaferTestName() + "foo"));

    List<String> foo = solrClient.getClusterStateProvider().resolveAlias(getSaferTestName() + "foo");

    CollectionAdminRequest.deleteAlias(getSaferTestName() + "foo").process(solrClient);

    for (String colName : foo) {
      CollectionAdminRequest.deleteCollection(colName).process(solrClient);
      waitCoreCount(colName, 0);
    }

    // if the design for terminating our executor is correct create/delete above will not cause failures below
    // continue testing...

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(4,cols.size()); // only one created in async case

    // now test with pre-create window longer than time slice, only one creation per request
    CollectionAdminRequest.setAliasProperty(alias)
        .addProperty(TimeRoutedAlias.ROUTER_PREEMPTIVE_CREATE_MATH, "3DAY").process(solrClient);

    assertUpdateResponse(add(alias, Arrays.asList(
        sdoc("id", "7", "timestamp_dt", "2017-10-25T23:01:00Z"), // should cause preemptive creation of 10-27 now
        sdoc("id", "71", "timestamp_dt", "2017-10-25T23:02:00Z")), // should not cause preemptive creation of 10-28 now
        params));
    assertUpdateResponse(solrClient.commit(alias));
    waitColAndAlias(alias, TRA, "2017-10-27", numShards);

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(5,cols.size()); // only one created in async case
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 6, alias);
    assertNumDocs("2017-10-26", 0, alias);
    assertNumDocs("2017-10-27", 0, alias);


    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "8", "timestamp_dt", "2017-10-25T23:01:00Z")), // should cause preemptive creation of 10-28 now
        params));
    assertUpdateResponse(solrClient.commit(alias));
    waitColAndAlias(alias, TRA, "2017-10-28", numShards);

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(6,cols.size());
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 7, alias);
    assertNumDocs("2017-10-26", 0, alias);
    assertNumDocs("2017-10-27", 0, alias);
    assertNumDocs("2017-10-28", 0, alias);

    QueryResponse resp;
    resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "10"));
    assertEquals(9, resp.getResults().getNumFound());

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
    waitColAndAlias(alias, TRA, "2017-10-29", numShards);

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(7,cols.size());
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 7, alias);
    assertNumDocs("2017-10-26", 0, alias);
    assertNumDocs("2017-10-27", 1, alias);
    assertNumDocs("2017-10-28", 3, alias); // should get through even though preemptive creation ignored it.
    assertNumDocs("2017-10-29", 0, alias);

    resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "0"));
    assertEquals(13, resp.getResults().getNumFound());

    // Sych creation with an interval longer than the time slice for the alias..
    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "13", "timestamp_dt", "2017-10-30T23:03:00Z")), // lucky?
        params));
    assertUpdateResponse(solrClient.commit(alias));
    waitColAndAlias(alias, TRA, "2017-10-30", numShards);

    // removed support for this case because it created a LOT of complexity for the benefit of attempting to
    // (maybe) not pause again after already hitting a synchronous creation (but only if asynch gets it done first,
    // otherwise we have a race... not enough benefit to justify the support/complexity.
    //
    // Now we just let the next doc take care of it...
    //
    // waitColAndAlias(alias, TRA, "2017-10-31", numShards); // spooky! async case arising in middle of sync creation!!

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(8,cols.size());
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 7, alias);
    assertNumDocs("2017-10-26", 0, alias);
    assertNumDocs("2017-10-27", 1, alias);
    assertNumDocs("2017-10-28", 3, alias); // should get through even though preemptive creation ignored it.
    assertNumDocs("2017-10-29", 0, alias);
    assertNumDocs("2017-10-30", 1, alias);

    resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "0"));
    assertEquals(14, resp.getResults().getNumFound());

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "14", "timestamp_dt", "2017-10-30T23:01:00Z")), // should cause preemptive creation 10-31
        params));
    waitColAndAlias(alias, TRA, "2017-10-31", numShards);

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "15", "timestamp_dt", "2017-10-30T23:01:00Z")), // should cause preemptive creation 11-01
        params));
    waitColAndAlias(alias, TRA, "2017-11-01", numShards);

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "16", "timestamp_dt", "2017-10-30T23:01:00Z")), // should cause preemptive creation 11-02
        params));
    waitColAndAlias(alias, TRA, "2017-11-02", numShards);

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "17", "timestamp_dt", "2017-10-30T23:01:00Z")), // should NOT cause preemptive creation 11-03
        params));

    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertFalse(cols.contains("myalias" + TRA + "2017-11-03"));

    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "18", "timestamp_dt", "2017-10-31T23:01:00Z")), // should cause preemptive creation 11-03
        params));
    waitColAndAlias(alias, TRA, "2017-11-03",numShards);

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
    assertTrue("Preemptive creation appears to not be asynchronous anymore", !colsT1.contains("myalias" + TRA + "2017-10-26"));
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 3, alias);

    // Here we quickly add another doc in a separate request, before the collection creation has completed.
    // This has the potential to incorrectly cause preemptive collection creation to run twice and create a
    // second collection. MaintainRoutedAliasCmd is meant to guard against this race condition by acquiring
    // a lock on the collection name.
    assertUpdateResponse(add(alias, Collections.singletonList(
        sdoc("id", "6", "timestamp_dt", "2017-10-25T23:01:00Z")), // might cause duplicate preemptive creation
        params));
    assertUpdateResponse(solrClient.commit(alias));
  }

  private void checkPreemptiveCase1(String alias) throws SolrServerException, IOException {
    List<String> cols;
    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertTrue("Preemptive creation happened twice and created a collection " +
        "further in the future than the configured time slice!",!cols.contains("myalias" + TRA + "2017-10-27"));

    validateCollectionCountAndAvailability(alias, 4, "Only 4 cols expected (premptive create happened" +
        "twice among threads");
    assertEquals(4, cols.size());
    assertNumDocs("2017-10-23", 1, alias);
    assertNumDocs("2017-10-24", 1, alias);
    assertNumDocs("2017-10-25", 4, alias);
    assertNumDocs("2017-10-26", 0, alias);
  }

  @SuppressWarnings("SameParameterValue")
  private void addOneDocSynchCreation(int numShards, String alias) throws SolrServerException, IOException, InterruptedException {
    // cause some collections to be created
    assertUpdateResponse(solrClient.add(alias,
        sdoc("id","1","timestamp_dt", "2017-10-25T00:00:00Z")
    ));
    assertUpdateResponse(solrClient.commit(alias));

    // wait for all the collections to exist...
    waitColAndAlias(alias, TRA, "2017-10-23", numShards); // This one should have already existed from the alias creation
    waitColAndAlias(alias, TRA, "2017-10-24", numShards); // Create 1
    waitColAndAlias(alias, TRA, "2017-10-25", numShards); // Create 2nd synchronously (ensure this is not broken)

    // normal update, nothing special, no collection creation required.
    List<String> cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(3,cols.size());

    assertNumDocs("2017-10-23", 0, alias);
    assertNumDocs("2017-10-24", 0, alias);
    assertNumDocs("2017-10-25", 1, alias);

    validateCollectionCountAndAvailability(alias, 3, "was expecting 3 live collections");
  }

  private void validateCollectionCountAndAvailability(String alias, int expected, String message) throws SolrServerException, IOException {
    List<String> cols;
    cols = new CollectionAdminRequest.ListAliases().process(solrClient).getAliasesAsLists().get(alias);
    assertEquals(message,expected,cols.size()); // only one created in async case

    // make sure they all exist
    for (String col : cols) {
      try {
        solrClient.query(col, params("q", "*:*","rows", "10"));
      } catch (SolrException e) {
        e.printStackTrace();
        fail("Unable to query " + col);
      }
    }
  }

  private void assertNumDocs(final String datePart, int expected, String alias) throws SolrServerException, IOException {
    QueryResponse resp = solrClient.query(alias + TRA + datePart, params(
        "q", "*:*",
        "rows", "10"));
    assertEquals(expected, resp.getResults().getNumFound());
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


  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public CloudSolrClient getSolrClient() {
    return solrClient;
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
          "stats.field", getTimeField()));
      long numFound = colStatsResp.getResults().getNumFound();
      if (numFound > 0) {
        totalNumFound += numFound;
        final FieldStatsInfo timestampStats = colStatsResp.getFieldStatsInfo().get(getTimeField());
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
        getTimeField(), timestamp.toString(),
        getIntField(), "0"); // always 0
  }

  private String getTimeField() {
    return timeField;
  }

  @Test
  public void testParse() {
    assertEquals(Instant.parse("2017-10-02T03:04:05Z"),
      TimeRoutedAlias.parseInstantFromCollectionName(alias, alias + TRA + "2017-10-02_03_04_05"));
    assertEquals(Instant.parse("2017-10-02T03:04:00Z"),
      TimeRoutedAlias.parseInstantFromCollectionName(alias, alias + TRA + "2017-10-02_03_04"));
    assertEquals(Instant.parse("2017-10-02T03:00:00Z"),
      TimeRoutedAlias.parseInstantFromCollectionName(alias, alias + TRA + "2017-10-02_03"));
    assertEquals(Instant.parse("2017-10-02T00:00:00Z"),
      TimeRoutedAlias.parseInstantFromCollectionName(alias, alias + TRA + "2017-10-02"));
  }

  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13943")
  @Test
  public void testDateMathInStart() throws Exception {
    ClusterStateProvider clusterStateProvider = solrClient.getClusterStateProvider();
    Class<? extends ClusterStateProvider> aClass = clusterStateProvider.getClass();
    System.out.println("CSPROVIDER:" + aClass);

    // This test prevents recurrence of SOLR-13760

    String configName = getSaferTestName();
    createConfigSet(configName);
    CountDownLatch aliasUpdate = new CountDownLatch(1);
    monitorAlias(aliasUpdate);

    // each collection has 4 shards with 3 replicas for 12 possible destinations
    // 4 of which are leaders, and 8 of which should fail this test.
    final int numShards = 1 + random().nextInt(4);
    final int numReplicas = 1 + random().nextInt(3);
    CollectionAdminRequest.createTimeRoutedAlias(alias, "2019-09-14T03:00:00Z/DAY", "+1DAY", getTimeField(),
        CollectionAdminRequest.createCollection("_unused_", configName, numShards, numReplicas)
            .setMaxShardsPerNode(numReplicas))
        .process(solrClient);

    aliasUpdate.await();
    if (BaseHttpClusterStateProvider.class.isAssignableFrom(aClass)) {
      ((BaseHttpClusterStateProvider)clusterStateProvider).resolveAlias(getAlias(), true);
    }
    aliasUpdate = new CountDownLatch(1);
    monitorAlias(aliasUpdate);

    ModifiableSolrParams params = params();
    String nowDay = DateTimeFormatter.ISO_INSTANT.format(DateMathParser.parseMath(new Date(), "2019-09-14T01:00:00Z").toInstant());
    assertUpdateResponse(add(alias, Arrays.asList(
        sdoc("id", "1", "timestamp_dt", nowDay)), // should not cause preemptive creation of 10-28 now
        params));

    // this process should have lead to the modification of the start time for the alias, converting it into
    // a parsable date, removing the DateMath

    // what we test next happens in a separate thread, so we have to give it some time to happen
    aliasUpdate.await();
    if (BaseHttpClusterStateProvider.class.isAssignableFrom(aClass)) {
      ((BaseHttpClusterStateProvider)clusterStateProvider).resolveAlias(getAlias(), true);
    }

    String hopeFullyModified = clusterStateProvider.getAliasProperties(getAlias()).get(ROUTER_START);
    try {
      Instant.parse(hopeFullyModified);
    } catch (DateTimeParseException e) {
      fail(ROUTER_START + " should not have any date math by this point and parse as an instant. Using "+ aClass +" Found:" + hopeFullyModified);
    }
  }

  private void monitorAlias(CountDownLatch aliasUpdate) throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    zkClient().getData("/aliases.json", new Watcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        aliasUpdate.countDown();
      }
    }, stat, true);
  }

  /**
   * Need to ensure that the existing TRA's gracefully handle, old, new and mixtures thereof. TRA's with
   * an autoDeleteAge setting will gracefully convert to the new format over time.
   */
  @Test
  public void handleLegacyCollectionNames() throws Exception {
    manuallyConstructLegacyTRA();

    // OK we now have an alias with legacy names and 2 documents. Let's try to query it to ensure query time back compat

    QueryResponse resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "10"
    ));

    assertEquals(2,resp.getResults().getNumFound());

    // verify that we can still add documents to it.
    assertUpdateResponse(solrClient.add(alias,
        sdoc("id","3","timestamp_dt", "2017-10-23T00:00:01Z")
    ));
    solrClient.commit(alias);
    resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "10"
    ));
    assertEquals(3,resp.getResults().getNumFound());


    // verify that it can create new collections
    assertUpdateResponse(solrClient.add(alias,
        sdoc("id","4","timestamp_dt", "2017-10-24T23:00:01Z") // preemptive
    ));
    solrClient.commit(alias);
    waitColAndAlias(alias, TRA, "2017-10-25",1);

    // verify that mixed old/new collections works for update/query
    resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "10"
    ));
    assertEquals(4,resp.getResults().getNumFound());

    // verify that documents go to the right collections

    assertUpdateResponse(solrClient.add(alias,
        sdoc("id","5","timestamp_dt", "2017-10-25T12:00:01Z") // preemptive
    ));
    solrClient.commit(alias);

    resp = solrClient.query("myalias_2017-10-23", params(
        "q", "*:*",
        "rows", "10",
        "sort", "id asc"
    ));
    assertEquals(2,resp.getResults().getNumFound());
    assertEquals(resp.getResults().get(0).getFirstValue("id"), "1");
    assertEquals(resp.getResults().get(1).getFirstValue("id"), "3");

    resp = solrClient.query("myalias_2017-10-24", params(
        "q", "*:*",
        "rows", "10",
        "sort", "id asc"
    ));
    assertEquals(2,resp.getResults().getNumFound());
    assertEquals(resp.getResults().get(0).getFirstValue("id"), "2");
    assertEquals(resp.getResults().get(1).getFirstValue("id"), "4");

    resp = solrClient.query("myalias" + TRA + "2017-10-25", params(
        "q", "*:*",
        "rows", "10",
        "sort", "id asc"
    ));
    assertEquals(1,resp.getResults().getNumFound());
    assertEquals(resp.getResults().get(0).getFirstValue("id"), "5");

    // verify that auto-delete will age out old collections
    checkCollectionCountIs(3);

    assertUpdateResponse(solrClient.add(alias,
        sdoc("id","6","timestamp_dt", "2017-10-26T12:00:01Z") // preemptive
    ));
    waitColAndAlias(alias, TRA,"2017-10-26",1);
    checkCollectionCountIs(3)
        .containsAll(Arrays.asList(
            "myalias_2017-10-24",
            "myalias" + TRA + "2017-10-25",
            "myalias" + TRA + "2017-10-26"));

    assertUpdateResponse(solrClient.add(alias,
        sdoc("id","7","timestamp_dt", "2017-10-27T12:00:01Z") // preemptive
    ));
    waitColAndAlias(alias, TRA,"2017-10-27",1);
    waitCoreCount("myalias_2017-10-23",0);
    checkCollectionCountIs(3)
        .containsAll(Arrays.asList(
            "myalias" + TRA + "2017-10-25",
            "myalias" + TRA + "2017-10-26",
            "myalias" + TRA + "2017-10-27"));

    // verify that auto-delete works on new collections.
    assertUpdateResponse(solrClient.add(alias,
        sdoc("id","8","timestamp_dt", "2017-10-28T12:00:01Z") // preemptive
    ));
    waitColAndAlias(alias, TRA,"2017-10-28",1);
    waitCoreCount("myalias_2017-10-24",0);
    checkCollectionCountIs(3)
        .containsAll(Arrays.asList(
            "myalias" + TRA + "2017-10-26",
            "myalias" + TRA + "2017-10-27",
            "myalias" + TRA + "2017-10-28"));


    solrClient.commit(alias);

    resp = solrClient.query(alias, params(
        "q", "*:*",
        "rows", "10"
    ));
    assertEquals(3,resp.getResults().getNumFound());

  }

  private List<String> checkCollectionCountIs(int num) {
    ClusterStateProvider clusterStateProvider = solrClient.getClusterStateProvider();
    List<String> collections = clusterStateProvider.resolveAlias(alias);
    if (clusterStateProvider instanceof BaseHttpClusterStateProvider) {
      collections = ((BaseHttpClusterStateProvider)clusterStateProvider).resolveAlias(alias,true);
    }
//    System.out.println();
//    System.out.println(clusterStateProvider.getClass());
//    System.out.println(collections);
//    System.out.println();
    assertEquals(num, collections.size()); // starting point
    return collections;
  }

  // here we do things not to be emulated elsewhere to create a legacy condition and ensure that we can
  // work with both old and new formats.
  private void manuallyConstructLegacyTRA() throws Exception {
    // first create a "modern" alias
    String configName = getSaferTestName();
    createConfigSet(configName);

    final int numShards = 1 ;
    final int numReplicas = 1 ;
    CollectionAdminRequest.createTimeRoutedAlias(alias, "2017-10-23T00:00:00Z", "+1DAY", getTimeField(),
        CollectionAdminRequest.createCollection("_unused_", configName, numShards, numReplicas)
            .setMaxShardsPerNode(numReplicas)).setPreemptiveCreateWindow("3HOUR").setAutoDeleteAge("/DAY-3DAYS")
        .process(solrClient);

    // now create collections that look like the legacy (pre __TRA__) names...
    String legacy23 = alias + "_" + "2017-10-23";
    CollectionAdminRequest.createCollection(legacy23, configName, numShards,numReplicas).process(solrClient);
    String legacy24 = alias + "_" + "2017-10-24";
    CollectionAdminRequest.createCollection(legacy24, configName, numShards,numReplicas).process(solrClient);

    waitCol(1,legacy23);
    waitCol(1,legacy24);
    // put some data in the legacy collections:
    assertUpdateResponse(solrClient.add(legacy23,
        sdoc("id","1","timestamp_dt", "2017-10-23T00:00:01Z")
    ));
    assertUpdateResponse(solrClient.add(legacy24,
        sdoc("id","2","timestamp_dt", "2017-10-24T00:00:01Z")
    ));

    solrClient.commit(legacy23);
    solrClient.commit(legacy24);

    QueryResponse resp = solrClient.query(legacy23, params(
        "q", "*:*",
        "rows", "10"));
    assertEquals(1,resp.getResults().getNumFound());

    resp = solrClient.query(legacy24, params(
        "q", "*:*",
        "rows", "10"));
    assertEquals(1,resp.getResults().getNumFound());

    // now knock out the collection backing our alias
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    Aliases aliases = zkStateReader.getAliases();
    List<String> collections = aliases.getCollectionAliasListMap().get(alias);
    for (String collection : collections) {
      CollectionAdminRequest.deleteCollection(collection);
    }

    // now grab the zk data so we can hack in our legacy collections..
    byte[] data = zkStateReader.getZkClient().getData("/aliases.json", null, null, true);

    // some tidbits for handling zk data here are swiped from Aliases.json
    Map<String, Map> aliasMap;
    if (data == null || data.length == 0) {
      aliasMap = Collections.emptyMap();
    } else {
      //noinspection unchecked
      aliasMap = (Map<String, Map>) Utils.fromJSON(data);
    }
    assertNotEquals(0, aliasMap.size());

    Map colAliases = aliasMap.getOrDefault("collection", Collections.emptyMap());
    assertNotEquals(0,colAliases.size());

    String singleInitialCollection = (String) colAliases.get(alias);
    assertFalse(singleInitialCollection.contains(","));

    // replace with our two new collections... in asc order!
    colAliases.put(alias,String.join(",",legacy24,legacy23));

    data = Utils.toJSON(aliasMap);
    zkStateReader.getZkClient().setData("/aliases.json",data,true);

    zkStateReader.aliasesManager.update(); // make sure we've updated with the data we just sent

    aliases = zkStateReader.aliasesManager.getAliases();
    assertEquals(2,aliases.getCollectionAliasListMap().get(alias).size());

    CollectionAdminRequest.deleteCollection(singleInitialCollection).process(solrClient);

    waitCoreCount(singleInitialCollection,0);

    // now make the legacy collections part of the alias
    CollectionAdminRequest.setCollectionProperty(legacy23,RoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP, alias).process(solrClient);
    CollectionAdminRequest.setCollectionProperty(legacy24,RoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP, alias).process(solrClient);
    CollectionAdminRequest.reloadCollection(legacy23).process(solrClient);
    CollectionAdminRequest.reloadCollection(legacy24).process(solrClient);

    cluster.getOpenOverseer().getCoreContainer().getZkController().getZkStateReader().aliasesManager.update();
  }

}
