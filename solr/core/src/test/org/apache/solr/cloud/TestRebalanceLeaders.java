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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Slow
public class TestRebalanceLeaders extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION_NAME = "TestColl";

  private static int numNodes;
  private static int numShards;
  private static int numReplicas;

  private static boolean useAdminToSetProps = false;

  @BeforeClass
  public static void setupCluster() throws Exception {

    numNodes = random().nextInt(4) + 4;
    numShards = random().nextInt(3) + 3;
    numReplicas = random().nextInt(2) + 2;
    useAdminToSetProps = random().nextBoolean();

    configureCluster(numNodes)
        .addConfig(COLLECTION_NAME, configset("cloud-minimal"))
        .configure();

    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(COLLECTION_NAME, COLLECTION_NAME,
        numShards, numReplicas, 0, 0)
        .setMaxShardsPerNode((numShards * numReplicas) / numNodes + 1)
        .process(cluster.getSolrClient());
    assertEquals("Admin request failed; ", 0, resp.getStatus());
    cluster.waitForActiveCollection(COLLECTION_NAME, numShards, numShards * numReplicas);

  }

  @Before
  public void removeAllProperties() throws KeeperException, InterruptedException {
    forceUpdateCollectionStatus();
    DocCollection docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
    for (Slice slice : docCollection.getSlices()) {
      for (Replica rep : slice.getReplicas()) {
        rep.getProperties().forEach((key, value) -> {
          if (key.startsWith("property.")) {
            try {
              delProp(slice, rep, key);
            } catch (IOException | SolrServerException e) {
              fail("Caught unexpected exception in @Before " + e.getMessage());
            }
          }
        });
      }
    }
  }

  int timeoutMs = 60000;


  // test that setting an arbitrary "slice unique" property un-sets the property if it's on another replica in the
  // slice. This is testing when the property is set on an _individual_ replica whereas testBalancePropertySliceUnique
  // tests whether changing an individual _replica_ un-sets the property on other replicas _in that slice_.
  //
  // NOTE: There were significant problems because at one point the code implicitly defined
  // shardUnique=true for the special property preferredLeader. That was removed at one point so we're explicitly
  // testing that as well.
  @Test
  public void testSetArbitraryPropertySliceUnique() throws IOException, SolrServerException, InterruptedException, KeeperException {
    // Check both special (preferredLeader) and something arbitrary.
    doTestSetArbitraryPropertySliceUnique("foo" + random().nextInt(1_000_000));
    removeAllProperties();
    doTestSetArbitraryPropertySliceUnique("preferredleader");
  }


  // Test that automatically distributing a slice unique property un-sets that property if it's in any other replica
  // on that slice.
  // This is different than the test above. The test above sets individual properties on individual nodes. This one
  // relies on Solr to pick which replicas to set the property on
  @Test
  public void testBalancePropertySliceUnique() throws KeeperException, InterruptedException, IOException, SolrServerException {
    // Check both cases of "special" property preferred(Ll)eader
    doTestBalancePropertySliceUnique("foo" + random().nextInt(1_000_000));
    removeAllProperties();
    doTestBalancePropertySliceUnique("preferredleader");
  }

  // We've moved on from a property being tested, we need to check if rebalancing the leaders actually chantges the
  // leader appropriately.
  @Test
  public void testRebalanceLeaders() throws Exception {

    // First let's unbalance the preferredLeader property, do all the leaders get reassigned properly?
    concentrateProp("preferredLeader");
    sendRebalanceCommand();
    checkPreferredsAreLeaders();

    // Now follow up by evenly distributing the property as well as possible.
    doTestBalancePropertySliceUnique("preferredLeader");
    sendRebalanceCommand();
    checkPreferredsAreLeaders();

    // Now check the condition we saw "in the wild" where you could not rebalance properly when Jetty was restarted.
    concentratePropByRestartingJettys();
    sendRebalanceCommand();
    checkPreferredsAreLeaders();
  }

  // Insure that the property is set on only one replica per slice when changing a unique property on an individual
  // replica.
  private void doTestSetArbitraryPropertySliceUnique(String propIn) throws InterruptedException, KeeperException, IOException, SolrServerException {
    final String prop = (random().nextBoolean()) ? propIn : propIn.toUpperCase(Locale.ROOT);
    // First set the property in some replica in some slice
    forceUpdateCollectionStatus();
    DocCollection docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);

    Slice[] slices = docCollection.getSlices().toArray(new Slice[0]);
    Slice slice = slices[random().nextInt(slices.length)];

    // Bounce around a bit setting this property and insure it's only set in one replica.
    Replica[] reps = slice.getReplicas().toArray(new Replica[0]);
    for (int idx = 0; idx < 4; ++idx) {
      Replica rep = reps[random().nextInt(reps.length)];
      // Set the property on a particular replica
      setProp(slice, rep, prop);
      TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);

      long count = 0;
      boolean rightRep = false;
      Slice modSlice;
      DocCollection modColl = null; // keeps IDE happy

      // insure that no other replica in that slice has the property when we return.
      while (timeout.hasTimedOut() == false) {
        forceUpdateCollectionStatus();
        modColl = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
        modSlice = modColl.getSlice(slice.getName());
        rightRep = modSlice.getReplica(rep.getName()).getBool("property." + prop.toLowerCase(Locale.ROOT), false);
        count = modSlice.getReplicas().stream().filter(thisRep -> thisRep.getBool("property." + prop.toLowerCase(Locale.ROOT), false)).count();

        if (count == 1 && rightRep) {
          break;
        }

        TimeUnit.MILLISECONDS.sleep(100);
      }
      if (count != 1 || rightRep == false) {
        fail("The property " + prop + " was not uniquely distributed in slice " + slice.getName()
            + " " + modColl.toString());
      }
    }
  }


  // Fail if we the replicas with the preferredLeader property are _not_ also the leaders.
  private void checkPreferredsAreLeaders() throws InterruptedException, KeeperException {
    // Make sure that the shard unique are where you expect.
    TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);

    while (timeout.hasTimedOut() == false) {
      if (checkPreferredsAreLeaders(false)) {
        // Ok, all preferreds are leaders. Just for Let's also get the election queue and guarantee that every
        // live replica is in the queue and none are repeated.
        checkElectionQueues();
        return;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }

    log.error("Leaders are not all preferres {}", cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME));
    // Show the errors
    checkPreferredsAreLeaders(true);
  }

  // Do all active nodes in each slice appear exactly once in the slice's leader election queue?
  // Since we assert that the number of live replicas is the same size as the leader election queue, we only
  // have to compare one way.
  private void checkElectionQueues() throws KeeperException, InterruptedException {

    DocCollection docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
    Set<String> liveNodes = cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes();

    for (Slice slice : docCollection.getSlices()) {
      Set<Replica> liveReplicas = new HashSet<>();
      slice.getReplicas().forEach(replica -> {
        if (replica.isActive(liveNodes)) {
          liveReplicas.add(replica);
        }
      });
      checkOneQueue(docCollection, slice, liveReplicas);
    }
  }

  // Helper method to check one leader election queue's consistency.
  private void checkOneQueue(DocCollection coll, Slice slice, Set<Replica> liveReplicas) throws KeeperException, InterruptedException {

    List<String> leaderQueue = cluster.getSolrClient().getZkStateReader().getZkClient().getChildren("/collections/" + COLLECTION_NAME +
        "/leader_elect/" + slice.getName() + "/election", null, true);

    if (leaderQueue.size() != liveReplicas.size()) {

      log.error("One or more replicas is missing from the leader election queue! Slice {}, election queue: {}, collection: {}"
          , slice.getName(), leaderQueue, coll);
      fail("One or more replicas is missing from the leader election queue");
    }
    // Check that each election node has a corresponding live replica.
    for (String electionNode : leaderQueue) {
      String replica = LeaderElector.getNodeName(electionNode);
      if (slice.getReplica(replica) == null) {
        log.error("Replica {} is not in the election queue: {}", replica, leaderQueue);
        fail("Replica is not in the election queue!");
      }
    }
  }

  // Just an encapsulation for checkPreferredsAreLeaders to make returning easier.
  // the doAsserts var is to actually print the problem and fail the test if the condition is not met.
  private boolean checkPreferredsAreLeaders(boolean doAsserts) throws KeeperException, InterruptedException {
    forceUpdateCollectionStatus();
    DocCollection docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
    for (Slice slice : docCollection.getSlices()) {
      for (Replica rep : slice.getReplicas()) {
        if (rep.getBool("property.preferredleader", false)) {
          boolean isLeader = rep.getBool("leader", false);
          if (doAsserts) {
            assertTrue("PreferredLeader should be the leader: ", isLeader);
          } else if (isLeader == false) {
            return false;
          }
        }
      }
    }
    return true;
  }

  // Arbitrarily send the rebalance command either with the SolrJ interface or with an HTTP request.
  private void sendRebalanceCommand() throws SolrServerException, InterruptedException, IOException {
    if (random().nextBoolean()) {
      rebalanceLeaderUsingSolrJAPI();
    } else {
      rebalanceLeaderUsingStandardRequest();
    }
  }

  // Helper method to make sure the property is _unbalanced_ first, then it gets properly re-assigned with the
  // BALANCESHARDUNIQUE command.
  private void doTestBalancePropertySliceUnique(String propIn) throws InterruptedException, IOException, KeeperException, SolrServerException {
    final String prop = (random().nextBoolean()) ? propIn : propIn.toUpperCase(Locale.ROOT);

    // Concentrate the properties on as few replicas a possible
    concentrateProp(prop);

    // issue the BALANCESHARDUNIQUE command
    rebalancePropAndCheck(prop);

    // Verify that there are no more than one replica with the property per shard.
    verifyPropUniquePerShard(prop);

    // Verify that the property is reasonably evenly distributed
    verifyPropCorrectlyDistributed(prop);

  }

  private void verifyPropCorrectlyDistributed(String prop) throws KeeperException, InterruptedException {

    TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);

    String propLC = prop.toLowerCase(Locale.ROOT);
    DocCollection docCollection = null;
    while (timeout.hasTimedOut() == false) {
      forceUpdateCollectionStatus();
      docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
      int maxPropCount = Integer.MAX_VALUE;
      int minPropCount = Integer.MIN_VALUE;
      for (Slice slice : docCollection.getSlices()) {
        int repCount = 0;
        for (Replica rep : slice.getReplicas()) {
          if (rep.getBool("property." + propLC, false)) {
            repCount++;
          }
        }
        maxPropCount = Math.max(maxPropCount, repCount);
        minPropCount = Math.min(minPropCount, repCount);
      }
      if (Math.abs(maxPropCount - minPropCount) < 2) return;
    }
    log.error("Property {} is not distributed evenly. {}", prop, docCollection);
    fail("Property is not distributed evenly " + prop);
  }

  // Used when we concentrate the leader on a few nodes.
  private void verifyPropDistributedAsExpected(Map<String, String> expectedShardReplicaMap, String prop) throws InterruptedException, KeeperException {
    // Make sure that the shard unique are where you expect.
    TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);

    String propLC = prop.toLowerCase(Locale.ROOT);
    boolean failure = false;
    DocCollection docCollection = null;
    while (timeout.hasTimedOut() == false) {
      forceUpdateCollectionStatus();
      docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
      failure = false;
      for (Map.Entry<String, String> ent : expectedShardReplicaMap.entrySet()) {
        Replica rep = docCollection.getSlice(ent.getKey()).getReplica(ent.getValue());
        if (rep.getBool("property." + propLC, false) == false) {
          failure = true;
        }
      }
      if (failure == false) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }

    fail(prop + " properties are not on the expected replicas: " + docCollection.toString()
        + System.lineSeparator() + "Expected " + expectedShardReplicaMap.toString());
  }

  // Just check that the property is distributed as expectecd. This does _not_ rebalance the leaders
  private void rebalancePropAndCheck(String prop) throws IOException, SolrServerException, InterruptedException, KeeperException {

    if (random().nextBoolean()) {
      rebalancePropUsingSolrJAPI(prop);
    } else {
      rebalancePropUsingStandardRequest(prop);
    }
  }


  private void rebalanceLeaderUsingSolrJAPI() throws IOException, SolrServerException, InterruptedException {
    CollectionAdminResponse resp = CollectionAdminRequest
        .rebalanceLeaders(COLLECTION_NAME)
        .process(cluster.getSolrClient());
    assertTrue("All leaders should have been verified", resp.getResponse().get("Summary").toString().contains("Success"));
    assertEquals("Admin request failed; ", 0, resp.getStatus());
  }

  private void rebalanceLeaderUsingStandardRequest() throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.REBALANCELEADERS.toString());
    params.set("collection", COLLECTION_NAME);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    QueryResponse resp = request.process(cluster.getSolrClient());
    assertTrue("All leaders should have been verified", resp.getResponse().get("Summary").toString().contains("Success"));
    assertEquals("Call to rebalanceLeaders failed ", 0, resp.getStatus());
  }


  private void rebalancePropUsingSolrJAPI(String prop) throws IOException, SolrServerException, InterruptedException {
    // Don't set the value, that should be done automatically.
    CollectionAdminResponse resp;

    if (prop.toLowerCase(Locale.ROOT).contains("preferredleader")) {
      resp = CollectionAdminRequest
          .balanceReplicaProperty(COLLECTION_NAME, prop)
          .process(cluster.getSolrClient());

    } else {
      resp = CollectionAdminRequest
          .balanceReplicaProperty(COLLECTION_NAME, prop)
          .setShardUnique(true)
          .process(cluster.getSolrClient());

    }
    assertEquals("Admin request failed; ", 0, resp.getStatus());
  }

  private void rebalancePropUsingStandardRequest(String prop) throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.BALANCESHARDUNIQUE.toString());
    params.set("property", prop);

    params.set("collection", COLLECTION_NAME);
    if (prop.toLowerCase(Locale.ROOT).contains("preferredleader") == false) {
      params.set("shardUnique", true);
    }
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    QueryResponse resp = request.process(cluster.getSolrClient());
    assertEquals("Call to rebalanceLeaders failed ", 0, resp.getStatus());
  }

  // This important. I've (Erick Erickson) run across a situation where the "standard request" causes failures, but
  // never the Admin request. So let's test both all the time for a given test.
  //
  // This sets an _individual_ replica to have the property, not collection-wide
  private void setProp(Slice slice, Replica rep, String prop) throws IOException, SolrServerException {
    if (useAdminToSetProps) {
      setPropWithAdminRequest(slice, rep, prop);
    } else {
      setPropWithStandardRequest(slice, rep, prop);
    }
  }

  void setPropWithStandardRequest(Slice slice, Replica rep, String prop) throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString());

    params.set("collection", COLLECTION_NAME);
    params.set("shard", slice.getName());
    params.set("replica", rep.getName());
    params.set("property", prop);
    params.set("property.value", "true");
    // Test to insure that implicit shardUnique is added for preferredLeader.
    if (prop.toLowerCase(Locale.ROOT).equals("preferredleader") == false) {
      params.set("shardUnique", "true");
    }

    @SuppressWarnings({"rawtypes"})
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    cluster.getSolrClient().request(request);
    String propLC = prop.toLowerCase(Locale.ROOT);
    waitForState("Expecting property '" + prop + "'to appear on replica " + rep.getName(), COLLECTION_NAME,
        (n, c) -> "true".equals(c.getReplica(rep.getName()).getProperty(propLC)));

  }

  void setPropWithAdminRequest(Slice slice, Replica rep, String prop) throws IOException, SolrServerException {
    boolean setUnique = (prop.toLowerCase(Locale.ROOT).equals("preferredleader") == false);
    CollectionAdminRequest.AddReplicaProp addProp =
        CollectionAdminRequest.addReplicaProperty(COLLECTION_NAME, slice.getName(), rep.getName(), prop, "true");
    if (setUnique) {
      addProp.setShardUnique(true);
    }
    CollectionAdminResponse resp = addProp.process(cluster.getSolrClient());
    assertEquals(0, resp.getStatus());
    String propLC = prop.toLowerCase(Locale.ROOT);
    waitForState("Expecting property '" + prop + "'to appear on replica " + rep.getName(), COLLECTION_NAME,
        (n, c) -> "true".equals(c.getReplica(rep.getName()).getProperty(propLC)));

  }

  private void delProp(Slice slice, Replica rep, String prop) throws IOException, SolrServerException {
    String propLC = prop.toLowerCase(Locale.ROOT);
    CollectionAdminResponse resp = CollectionAdminRequest.deleteReplicaProperty(COLLECTION_NAME, slice.getName(), rep.getName(), propLC)
        .process(cluster.getSolrClient());
    assertEquals("Admin request failed; ", 0, resp.getStatus());
    waitForState("Expecting property '" + prop + "' to be removed from replica " + rep.getName(), COLLECTION_NAME,
        (n, c) -> c.getReplica(rep.getName()).getProperty(prop) == null);
  }

  // Intentionally un-balance the property to insure that BALANCESHARDUNIQUE does its job. There was an odd case
  // where rebalancing didn't work very well if the Solr nodes were stopped and restarted that worked perfectly
  // when if the nodes were _not_ restarted in the test. So we have to test that too.
  private void concentratePropByRestartingJettys() throws Exception {

    List<JettySolrRunner> jettys = new ArrayList<>(cluster.getJettySolrRunners());
    Collections.shuffle(jettys, random());
    jettys.remove(random().nextInt(jettys.size()));
    // Now we have a list of jettys, and there is one missing. Stop all of the remaining jettys, then start them again
    // to concentrate the leaders. It's not necessary that all shards have a leader.

    for (JettySolrRunner jetty : jettys) {
      cluster.stopJettySolrRunner(jetty);
      cluster.waitForJettyToStop(jetty);
    }
    checkReplicasInactive(jettys);

    for (int idx = 0; idx < jettys.size(); ++idx) {
      cluster.startJettySolrRunner(jettys.get(idx));
    }
    cluster.waitForAllNodes(60);
    // the nodes are present, but are all replica active?
    checkAllReplicasActive();
  }

  // while banging my nead against a wall, I put a lot of force refresh statements in. Want to leave them in
  // but have this be a no-op so if we start to get failures, we can re-enable with minimal effort.
  private void forceUpdateCollectionStatus() throws KeeperException, InterruptedException {
    // cluster.getSolrClient().getZkStateReader().forceUpdateCollection(COLLECTION_NAME);
  }

  // Since we have to restart jettys, we don't want to try rebalancing etc. until we're sure all jettys that should
  // be up are up and all replicas are active.
  private void checkReplicasInactive(List<JettySolrRunner> downJettys) throws KeeperException, InterruptedException {
    TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    DocCollection docCollection = null;
    Set<String> liveNodes = null;

    Set<String> downJettyNodes = new TreeSet<>();
    for (JettySolrRunner jetty : downJettys) {
      downJettyNodes.add(jetty.getBaseUrl().getHost() + ":" + jetty.getBaseUrl().getPort() + "_solr");
    }
    while (timeout.hasTimedOut() == false) {
      forceUpdateCollectionStatus();
      docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
      liveNodes = cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes();
      boolean expectedInactive = true;

      for (Slice slice : docCollection.getSlices()) {
        for (Replica rep : slice.getReplicas()) {
          if (downJettyNodes.contains(rep.getNodeName()) == false) {
            continue; // We are on a live node
          }
          // A replica on an allegedly down node is reported as active.
          if (rep.isActive(liveNodes)) {
            expectedInactive = false;
          }
        }
      }
      if (expectedInactive) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    fail("timed out waiting for all replicas to become inactive: livenodes: " + liveNodes +
        " Collection state: " + docCollection.toString());
  }

  // We need to wait around until all replicas are active before expecting rebalancing or distributing shard-unique
  // properties to work.
  private void checkAllReplicasActive() throws KeeperException, InterruptedException {
    TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (timeout.hasTimedOut() == false) {
      forceUpdateCollectionStatus();
      DocCollection docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
      Set<String> liveNodes = cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes();
      boolean allActive = true;
      for (Slice slice : docCollection.getSlices()) {
        for (Replica rep : slice.getReplicas()) {
          if (rep.isActive(liveNodes) == false) {
            allActive = false;
          }
        }
      }
      if (allActive) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    fail("timed out waiting for all replicas to become active");
  }

  // use a simple heuristic to put as many replicas with the property on as few nodes as possible. The point is that
  // then we can execute BALANCESHARDUNIQUE and be sure it worked correctly
  private void concentrateProp(String prop) throws KeeperException, InterruptedException, IOException, SolrServerException {
    // find all the live nodes
    // for each slice, assign the leader to the first replica that is in the lowest position on live_nodes
    List<String> liveNodes = new ArrayList<>(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes());
    Collections.shuffle(liveNodes, random());

    Map<String, String> uniquePropMap = new TreeMap<>();
    forceUpdateCollectionStatus();
    DocCollection docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
    for (Slice slice : docCollection.getSlices()) {
      Replica changedRep = null;
      int livePos = Integer.MAX_VALUE;
      for (Replica rep : slice.getReplicas()) {
        int pos = liveNodes.indexOf(rep.getNodeName());
        if (pos >= 0 && pos < livePos) {
          livePos = pos;
          changedRep = rep;
        }
      }
      if (livePos == Integer.MAX_VALUE) {
        fail("Invalid state! We should have a replica to add the property to! " + docCollection.toString());
      }

      uniquePropMap.put(slice.getName(), changedRep.getName());
      // Now set the property on the "lowest" node in live_nodes.
      setProp(slice, changedRep, prop);
    }
    verifyPropDistributedAsExpected(uniquePropMap, prop);
  }

  // make sure that the property in question is unique per shard.
  private Map<String, String> verifyPropUniquePerShard(String prop) throws InterruptedException, KeeperException {
    Map<String, String> uniquePropMaps = new TreeMap<>();

    TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (timeout.hasTimedOut() == false) {
      uniquePropMaps.clear();
      if (checkdUniquePropPerShard(uniquePropMaps, prop)) {
        return uniquePropMaps;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    fail("There should be exactly one replica with value " + prop + " set to true per shard: "
        + cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).toString());
    return null; // keeps IDE happy.
  }

  // return true if every shard has exactly one replica with the unique property set to "true"
  private boolean checkdUniquePropPerShard(Map<String, String> uniques, String prop) throws KeeperException, InterruptedException {
    forceUpdateCollectionStatus();
    DocCollection docCollection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);

    for (Slice slice : docCollection.getSlices()) {
      int propfCount = 0;
      for (Replica rep : slice.getReplicas()) {
        if (rep.getBool("property." + prop.toLowerCase(Locale.ROOT), false)) {
          propfCount++;
          uniques.put(slice.getName(), rep.getName());
        }
      }
      if (1 != propfCount) {
        return false;
      }
    }
    return true;
  }
}