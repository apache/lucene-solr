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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
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
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Ignore;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Ignore  // don't try too run abstract base class
public abstract class RoutedAliasUpdateProcessorTest extends SolrCloudTestCase {

  private static final String intField = "integer_i";

  void waitColAndAlias(String alias, String separator, final String suffix, int slices) throws InterruptedException {
    // collection to exist
    String collection = alias + separator + suffix;
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

  /** @see TrackingUpdateProcessorFactory */
  String getTrackUpdatesGroupName() {
    return getSaferTestName();
  }

  void createConfigSet(String configName) throws SolrServerException, IOException {
    // First create a configSet
    // Then we create a collection with the name of the eventual config.
    // We configure it, and ultimately delete the collection, leaving a modified config-set behind.
    // Later we create the "real" collections referencing this modified config-set.
    assertEquals(0, new ConfigSetAdminRequest.Create()
        .setConfigSetName(configName)
        .setBaseConfigSetName("_default")
        .process(getSolrClient()).getStatus());

    CollectionAdminRequest.createCollection(configName, configName, 1, 1).process(getSolrClient());

    // TODO: fix SOLR-13059, a where this wait isn't working ~0.3% of the time.
    waitCol(1,configName);
    // manipulate the config...
    checkNoError(getSolrClient().request(new V2Request.Builder("/collections/" + configName + "/config")
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
            "    'fieldName':'" + getIntField() + "'" +
            "  }," +
            "}").build()));
    // only sometimes test with "tolerant" URP:
    final String urpNames = "inc" + (random().nextBoolean() ? ",tolerant" : "");
    checkNoError(getSolrClient().request(new V2Request.Builder("/collections/" + configName + "/config/params")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{" +
            "  'set' : {" +
            "    '_UPDATE' : {'processor':'" + urpNames + "'}" +
            "  }" +
            "}").build()));

    CollectionAdminRequest.deleteCollection(configName).process(getSolrClient());
    assertTrue(
        new ConfigSetAdminRequest.List().process(getSolrClient()).getConfigSets()
            .contains(configName)
    );
  }

  String getIntField() {
    return intField;
  }

  @SuppressWarnings("WeakerAccess")
  void checkNoError(NamedList<Object> response) {
    Object errors = response.get("errorMessages");
    assertNull("" + errors, errors);
  }

  @SuppressWarnings("WeakerAccess")
  Set<String> getLeaderCoreNames(ClusterState clusterState) {
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

  void assertRouting(int numShards, List<UpdateCommand> updateCommands) throws IOException {
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

  public abstract String getAlias() ;

  public abstract CloudSolrClient getSolrClient() ;


  @SuppressWarnings("WeakerAccess")
  void waitCol(int slices, String collection) {
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

  /** Adds these documents and commits, returning when they are committed.
   * We randomly go about this in different ways. */
  void addDocsAndCommit(boolean aliasOnly, SolrInputDocument... solrInputDocuments) throws Exception {
    // we assume all docs will be added (none too old/new to cause exception)
    Collections.shuffle(Arrays.asList(solrInputDocuments), random());

    // this is a list of the collections & the alias name.  Use to pick randomly where to send.
    //   (it doesn't matter where we send docs since the alias is honored at the URP level)
    List<String> collections = new ArrayList<>();
    collections.add(getAlias());
    if (!aliasOnly) {
      collections.addAll(new CollectionAdminRequest.ListAliases().process(getSolrClient()).getAliasesAsLists().get(getAlias()));
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
      getSolrClient().commit(col);
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

  void assertUpdateResponse(UpdateResponse rsp) {
    // use of TolerantUpdateProcessor can cause non-thrown "errors" that we need to check for
    List errors = (List) rsp.getResponseHeader().get("errors");
    assertTrue("Expected no errors: " + errors,errors == null || errors.isEmpty());
  }

  private int queryNumDocs(String q) throws SolrServerException, IOException {
    return (int) getSolrClient().query(getAlias(), params("q", q, "rows", "0")).getResults().getNumFound();
  }

  /** Adds the docs to Solr via {@link #getSolrClient()} with the params */
  @SuppressWarnings("SameParameterValue")
  protected UpdateResponse add(String collection, Collection<SolrInputDocument> docs, SolrParams params) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    if (params != null) {
      req.setParams(new ModifiableSolrParams(params));// copy because will be modified
    }
    req.add(docs);
    return req.process(getSolrClient(), collection);
  }

  public static class IncrementURPFactory extends FieldMutatingUpdateProcessorFactory {

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return FieldValueMutatingUpdateProcessor.valueMutator( getSelector(), next,
          (src) -> Integer.valueOf(src.toString()) + 1);
    }
  }
}
