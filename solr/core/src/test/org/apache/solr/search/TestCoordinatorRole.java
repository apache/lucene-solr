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

package org.apache.solr.search;

import static org.apache.solr.common.params.CommonParams.OMIT_HEADER;
import static org.apache.solr.common.params.CommonParams.TRUE;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZkStateReaderAccessor;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.servlet.CoordinatorHttpSolrCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class TestCoordinatorRole extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void testSimple() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(4).addConfig("conf", configset("cloud-minimal")).configure();
    try {
      CloudSolrClient client = cluster.getSolrClient();
      String COLLECTION_NAME = "test_coll";
      String SYNTHETIC_COLLECTION = CoordinatorHttpSolrCall.getSyntheticCollectionName("conf");
      CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 2, 2)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 4);
      UpdateRequest ur = new UpdateRequest();
      for (int i = 0; i < 10; i++) {
        SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField("id", "" + i);
        ur.add(doc2);
      }

      ur.commit(client, COLLECTION_NAME);
      QueryResponse rsp = client.query(COLLECTION_NAME, new SolrQuery("*:*"));
      assertEquals(10, rsp.getResults().getNumFound());

      System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
      final JettySolrRunner coordinatorJetty;
      try {
        coordinatorJetty = cluster.startJettySolrRunner();
      } finally {
        System.clearProperty(NodeRoles.NODE_ROLES_PROP);
      }
      QueryResponse rslt =
          (QueryResponse) new QueryRequest(new SolrQuery("*:*"))
              .setPreferredNodes(ImmutableList.of(coordinatorJetty.getNodeName()))
              .process(client, COLLECTION_NAME);

      assertEquals(10, rslt.getResults().size());

      DocCollection collection =
          cluster.getSolrClient().getClusterStateProvider().getCollection(SYNTHETIC_COLLECTION);
      assertNotNull(collection);

      Set<String> expectedNodes = new HashSet<>();
      expectedNodes.add(coordinatorJetty.getNodeName());
      collection.forEachReplica((s, replica) -> expectedNodes.remove(replica.getNodeName()));
      assertTrue(expectedNodes.isEmpty());
    } finally {
      cluster.shutdown();
    }
  }

  public void testMultiCollectionMultiNode() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(4).addConfig("conf", configset("cloud-minimal")).configure();
    try {
      CloudSolrClient client = cluster.getSolrClient();
      String COLLECTION_NAME = "test_coll";
      String SYNTHETIC_COLLECTION = CoordinatorHttpSolrCall.getSyntheticCollectionName("conf");
      for (int j = 1; j <= 10; j++) {
        String collname = COLLECTION_NAME + "_" + j;
        CollectionAdminRequest.createCollection(collname, "conf", 2, 2)
            .process(cluster.getSolrClient());
        cluster.waitForActiveCollection(collname, 2, 4);
        UpdateRequest ur = new UpdateRequest();
        for (int i = 0; i < 10; i++) {
          SolrInputDocument doc2 = new SolrInputDocument();
          doc2.addField("id", "" + i);
          ur.add(doc2);
        }

        ur.commit(client, collname);
        QueryResponse rsp = client.query(collname, new SolrQuery("*:*"));
        assertEquals(10, rsp.getResults().getNumFound());
      }

      System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
      final JettySolrRunner coordinatorJetty1;
      final JettySolrRunner coordinatorJetty2;
      try {
        coordinatorJetty1 = cluster.startJettySolrRunner();
        coordinatorJetty2 = cluster.startJettySolrRunner();
      } finally {
        System.clearProperty(NodeRoles.NODE_ROLES_PROP);
      }
      for (int j = 1; j <= 10; j++) {
        String collname = COLLECTION_NAME + "_" + j;
        QueryResponse rslt =
            (QueryResponse) new QueryRequest(new SolrQuery("*:*"))
                .setPreferredNodes(ImmutableList.of(coordinatorJetty1.getNodeName()))
                .process(client, collname);

        assertEquals(10, rslt.getResults().size());
      }

      for (int j = 1; j <= 10; j++) {
        String collname = COLLECTION_NAME + "_" + j;
        QueryResponse rslt =
            (QueryResponse) new QueryRequest(new SolrQuery("*:*"))
                .setPreferredNodes(ImmutableList.of(coordinatorJetty2.getNodeName()))
                .process(client, collname);

        assertEquals(10, rslt.getResults().size());
      }

      DocCollection collection =
          cluster.getSolrClient().getClusterStateProvider().getCollection(SYNTHETIC_COLLECTION);
      assertNotNull(collection);

      int coordNode1NumCores = coordinatorJetty1.getCoreContainer().getNumAllCores();
      assertEquals("Unexpected number of cores found for coordinator node", 1, coordNode1NumCores);
      int coordNode2NumCores = coordinatorJetty2.getCoreContainer().getNumAllCores();
      assertEquals("Unexpected number of cores found for coordinator node", 1, coordNode2NumCores);
    } finally {
      cluster.shutdown();
    }
  }

  public void testNRTRestart() throws Exception {
    // we restart jetty and expect to find on disk data - need a local fs directory
    useFactory(null);
    String COLL = "coordinator_test_coll";
    MiniSolrCloudCluster cluster =
        configureCluster(3)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("conf3"))
            .configure();
    System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
    JettySolrRunner qaJetty = cluster.startJettySolrRunner();
    String qaJettyBase = qaJetty.getBaseUrl().toString();
    System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("manipulateJetty"));
    try {
      CollectionAdminRequest.createCollection(COLL, "conf", 1, 1, 0, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLL, 1, 2);
      DocCollection docColl =
          cluster.getSolrClient().getClusterStateProvider().getClusterState().getCollection(COLL);
      Replica nrtReplica = docColl.getReplicas(EnumSet.of(Replica.Type.NRT)).get(0);
      assertNotNull(nrtReplica);
      String nrtCore = nrtReplica.getCoreName();
      Replica pullReplica = docColl.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0);
      assertNotNull(pullReplica);
      String pullCore = pullReplica.getCoreName();

      SolrInputDocument sid = new SolrInputDocument();
      sid.addField("id", "123");
      sid.addField("desc_s", "A Document");
      JettySolrRunner nrtJetty = null;
      JettySolrRunner pullJetty = null;
      for (JettySolrRunner j : cluster.getJettySolrRunners()) {
        String nodeName = j.getNodeName();
        if (nodeName.equals(nrtReplica.getNodeName())) {
          nrtJetty = j;
        } else if (nodeName.equals(pullReplica.getNodeName())) {
          pullJetty = j;
        }
      }
      assertNotNull(nrtJetty);
      assertNotNull(pullJetty);
      try (SolrClient client = pullJetty.newClient()) {
        client.add(COLL, sid);
        client.commit(COLL);
        assertEquals(
            nrtCore,
            getHostCoreName(
                COLL, qaJettyBase, p -> p.add(ShardParams.SHARDS_PREFERENCE, "replica.type:NRT")));
        assertEquals(
            pullCore,
            getHostCoreName(
                COLL, qaJettyBase, p -> p.add(ShardParams.SHARDS_PREFERENCE, "replica.type:PULL")));
        // Now , kill NRT jetty
        JettySolrRunner nrtJettyF = nrtJetty;
        JettySolrRunner pullJettyF = pullJetty;
        Random r = random();
        final long establishBaselineMs = r.nextInt(1000);
        final long nrtDowntimeMs = r.nextInt(10000);
        // NOTE: for `pullServiceTimeMs`, it can't be super-short. This is just to simplify our
        // indexing code,
        // based on the fact that our indexing is based on a PULL-node client.
        final long pullServiceTimeMs = 1000 + (long) r.nextInt(9000);
        Future<?> jettyManipulationFuture =
            executor.submit(
                () -> {
                  // we manipulate the jetty instances in a separate thread to more closely mimic
                  // the behavior we'd see irl.
                  try {
                    Thread.sleep(establishBaselineMs);
                    log.info("stopping NRT jetty ...");
                    nrtJettyF.stop();
                    log.info("NRT jetty stopped.");
                    Thread.sleep(nrtDowntimeMs); // let NRT be down for a while
                    log.info("restarting NRT jetty ...");
                    nrtJettyF.start(true);
                    log.info("NRT jetty restarted.");
                    // once NRT is back up, we expect PULL to continue serving until the TTL on ZK
                    // state used for query request routing has expired (60s). But here we force a
                    // return to NRT by stopping the PULL replica after a brief delay ...
                    Thread.sleep(pullServiceTimeMs);
                    log.info("stopping PULL jetty ...");
                    pullJettyF.stop();
                    log.info("PULL jetty stopped.");
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
        String hostCore;
        long start = new Date().getTime();
        long individualRequestStart = start;
        int count = 0;
        while (nrtCore.equals(
            hostCore =
                getHostCoreName(
                    COLL,
                    qaJettyBase,
                    p -> p.add(ShardParams.SHARDS_PREFERENCE, "replica.type:NRT")))) {
          count++;
          Thread.sleep(100);
          individualRequestStart = new Date().getTime();
        }
        long now = new Date().getTime();
        log.info(
            "phase1 NRT queries count={}, overall_duration={}, baseline_expected_overall_duration={}, switch-to-pull_duration={}",
            count,
            now - start,
            establishBaselineMs,
            now - individualRequestStart);
        // default tolerance of 500ms below should suffice. Failover to PULL for this case should be
        // very fast, because our QA-based client already knows both replicas are active, the index
        // is stable, so the moment the client finds NRT is down it should be able to failover
        // immediately and transparently to PULL.
        assertEquals(
            "when we break out of the NRT query loop, should be b/c routed to PULL",
            pullCore,
            hostCore);
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", "345");
        d.addField("desc_s", "Another Document");
        // attempts to add another doc while NRT is down should fail, then eventually succeed when
        // NRT comes back up
        count = 0;
        start = new Date().getTime();
        individualRequestStart = start;
        for (; ; ) {
          try {
            client.add(COLL, d);
            client.commit(COLL);
            break;
          } catch (SolrException ex) {
            // we expect these until nrtJetty is back up.
            count++;
            Thread.sleep(100);
          }
          individualRequestStart = new Date().getTime();
        }
        now = new Date().getTime();
        log.info(
            "successfully added another doc; duration: {}, overall_duration={}, baseline_expected_overall_duration={}, exception_count={}",
            now - individualRequestStart,
            now - start,
            nrtDowntimeMs,
            count);
        // NRT replica is back up, registered as available with Zk, and availability info has been
        // pulled down by our PULL-replica-based `client`, forwarded indexing command to NRT,
        // index/commit completed. All of this accounts for the 3000ms tolerance allowed for below.
        // This is not a strict value, and if it causes failures regularly we should feel free to
        // increase the tolerance; but it's meant to provide a stable baseline from which to detect
        // regressions.
        count = 0;
        start = new Date().getTime();
        individualRequestStart = start;
        while (pullCore.equals(
            hostCore =
                getHostCoreName(
                    COLL,
                    qaJettyBase,
                    p -> {
                      p.set(CommonParams.Q, "id:345");
                      p.add(ShardParams.SHARDS_PREFERENCE, "replica.type:NRT");
                    }))) {
          count++;
          Thread.sleep(100);
          individualRequestStart = new Date().getTime();
        }
        now = new Date().getTime();
        log.info(
            "query retries between NRT index-ready and query-ready: {}; overall_duration={}; baseline_expected_overall_duration={}; failover-request_duration={}",
            count,
            now - start,
            pullServiceTimeMs,
            now - individualRequestStart);
        assertEquals(nrtCore, hostCore);
        // allow any exceptions to propagate
        jettyManipulationFuture.get();

        // next phase: just toggle a bunch
        // TODO: could separate this out into a different test method, but this should suffice for
        // now
        pullJetty.start(true);
        AtomicBoolean done = new AtomicBoolean();
        long runMinutes = 1;
        long finishTimeMs =
            new Date().getTime() + TimeUnit.MILLISECONDS.convert(runMinutes, TimeUnit.MINUTES);
        JettySolrRunner[] jettys = new JettySolrRunner[] {nrtJettyF, pullJettyF};
        Random threadRandom = new Random(r.nextInt());
        Future<Integer> f =
            executor.submit(
                () -> {
                  int iteration = 0;
                  while (new Date().getTime() < finishTimeMs && !done.get()) {
                    int idx = iteration++ % jettys.length;
                    JettySolrRunner toManipulate = jettys[idx];
                    try {
                      int serveTogetherTime = threadRandom.nextInt(7000);
                      int downTime = threadRandom.nextInt(7000);
                      log.info("serving together for {}ms", serveTogetherTime);
                      Thread.sleep(serveTogetherTime);
                      log.info("stopping {} ...", idx);
                      toManipulate.stop();
                      log.info("stopped {}.", idx);
                      Thread.sleep(downTime);
                      log.info("restarting {} ...", idx);
                      toManipulate.start(true);
                      log.info("restarted {}.", idx);
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
                  done.set(true);
                  return iteration;
                });
        count = 0;
        start = new Date().getTime();
        try {
          do {
            if (pullCore.equals(
                getHostCoreName(
                    COLL,
                    qaJettyBase,
                    p -> {
                      p.set(CommonParams.Q, "id:345");
                      p.add(ShardParams.SHARDS_PREFERENCE, "replica.type:NRT");
                    }))) {
              done.set(true);
            }
            count++;
            Thread.sleep(100);
          } while (!done.get());
        } finally {
          final String result;
          if (done.getAndSet(true)) {
            result = "Success";
          } else {
            // not yet set to done, completed abnormally (exception will be thrown beyond `finally`
            // block)
            result = "Failure";
          }
          Integer toggleCount = f.get();
          long secondsDuration =
              TimeUnit.SECONDS.convert(new Date().getTime() - start, TimeUnit.MILLISECONDS);
          log.info(
              "{}! {} seconds, {} toggles, {} requests served",
              result,
              secondsDuration,
              toggleCount,
              count);
        }
      }
    } finally {
      try {
        ExecutorUtil.shutdownAndAwaitTermination(executor);
      } finally {
        cluster.shutdown();
      }
    }
  }

  private String getHostCoreName(String COLL, String qaNode, Consumer<SolrQuery> p)
      throws Exception {
    boolean found = false;
    SolrQuery q =
        new SolrQuery(
            CommonParams.Q,
            "*:*",
            CommonParams.FL,
            "id,desc_s,_core_:[core]",
            OMIT_HEADER,
            TRUE,
            CommonParams.WT,
            CommonParams.JAVABIN);
    p.accept(q);
    SolrDocumentList docs = null;
    try (SolrClient solrClient = new Http2SolrClient.Builder(qaNode).build()) {
      for (int i = 0; i < 100; i++) {
        try {
          QueryResponse queryResponse = solrClient.query(COLL, q);
          docs = queryResponse.getResults();
          assertNotNull("Docs should not be null. Query response was: " + queryResponse, docs);
          if (docs.size() > 0) {
            found = true;
            break;
          }
        } catch (SolrException ex) {
          // we know we're doing tricky things that might cause transient errors
          // TODO: all these query requests go to the QA node -- should QA propagate internal
          // request errors to the external client (and the external client retry?) or should QA
          // attempt to failover transparently in the event of an error?
          if (i < 5) {
            log.info("swallowing transient error", ex);
          } else {
            log.error("only expect actual _errors_ within a small window (e.g. 500ms)", ex);
            fail("initial error time threshold exceeded");
          }
        }
        Thread.sleep(100);
      }
    }
    assertTrue(found);
    return (String) docs.get(0).getFieldValue("_core_");
  }

  public void testConcurrentAccess() throws Exception {
    final int DATA_NODE_COUNT = 2;
    final int COORDINATOR_NODE_COUNT = 4;
    MiniSolrCloudCluster cluster =
        configureCluster(DATA_NODE_COUNT).addConfig("conf", configset("cloud-minimal")).configure();

    List<String> dataNodes =
        cluster.getJettySolrRunners().stream()
            .map(JettySolrRunner::getNodeName)
            .collect(Collectors.toList());

    try {
      CloudSolrClient client = cluster.getSolrClient();
      String COLLECTION_PREFIX = "test_coll_";

      final int COLLECTION_COUNT = 10;
      final int DOC_PER_COLLECTION_COUNT = 1000;

      List<String> collectionNames = new ArrayList<>();
      for (int i = 0; i < COLLECTION_COUNT; i++) {
        String collectionName = COLLECTION_PREFIX + i;
        CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
            .setCreateNodeSet(String.join(",", dataNodes)) // only put data onto the 2 data nodes
            .process(cluster.getSolrClient());
        cluster.waitForActiveCollection(collectionName, 2, 2);
        collectionNames.add(collectionName);
      }

      for (String collectionName : collectionNames) {
        UpdateRequest ur = new UpdateRequest();
        for (int i = 0; i < DOC_PER_COLLECTION_COUNT; i++) {
          SolrInputDocument doc2 = new SolrInputDocument();
          doc2.addField("id", collectionName + "-" + i);
          ur.add(doc2);
        }
        ur.commit(client, collectionName);
        QueryResponse rsp = client.query(collectionName, new SolrQuery("*:*"));
        assertEquals(DOC_PER_COLLECTION_COUNT, rsp.getResults().getNumFound());
      }

      System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
      List<String> coordinatorNodes = new ArrayList<>();
      try {
        for (int i = 0; i < COORDINATOR_NODE_COUNT; i++) {
          JettySolrRunner coordinatorJetty = cluster.startJettySolrRunner();
          coordinatorNodes.add(coordinatorJetty.getNodeName());
        }
      } finally {
        System.clearProperty(NodeRoles.NODE_ROLES_PROP);
      }

      int THREAD_COUNT = 10;
      int RUN_COUNT = 20;
      // final AtomicInteger runCounter = new AtomicInteger();
      // 10 threads to concurrently access the collections and ensure data are not mixed up
      ExecutorService executorService =
          ExecutorUtil.newMDCAwareFixedThreadPool(
              THREAD_COUNT, new SolrNamedThreadFactory(this.getClass().getSimpleName()));
      List<Future<?>> testFutures = new ArrayList<>();

      for (int i = 0; i < RUN_COUNT; i++) {
        final int currentRun = i;
        testFutures.add(
            executorService.submit(
                () -> {
                  final String collectionName =
                      collectionNames.get(currentRun % collectionNames.size());
                  final String coordinatorNode =
                      coordinatorNodes.get(currentRun % coordinatorNodes.size());
                  QueryResponse response =
                      (QueryResponse) new QueryRequest(new SolrQuery("*:*"))
                          .setPreferredNodes(ImmutableList.of(coordinatorNode))
                          .process(client, collectionName);
                  assertEquals(DOC_PER_COLLECTION_COUNT, response.getResults().getNumFound());
                  // ensure docs have the correct id (ie not mixing up with other collections)
                  for (SolrDocument doc : response.getResults()) {
                    assertTrue(((String) doc.getFieldValue("id")).startsWith(collectionName));
                  }
                  return null;
                }));
      }
      for (Future<?> testFuture : testFutures) {
        testFuture.get(); // check for any exceptions/failures
      }

      // number of replicas created in the synthetic collection should be one per coordinator node
      assertEquals(
          COORDINATOR_NODE_COUNT,
          client
              .getZkStateReader()
              .getClusterState()
              .getCollection(CoordinatorHttpSolrCall.getSyntheticCollectionName("conf"))
              .getReplicas()
              .size());

      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } finally {
      cluster.shutdown();
    }
  }

  public void testConfigset() throws Exception {
    final int DATA_NODE_COUNT = 1;
    MiniSolrCloudCluster cluster =
        configureCluster(DATA_NODE_COUNT)
            .addConfig("conf1", configset("cloud-minimal"))
            .addConfig("conf2", configset("cache-control"))
            .configure();
    List<String> dataNodes =
        cluster.getJettySolrRunners().stream()
            .map(JettySolrRunner::getNodeName)
            .collect(Collectors.toList());

    try {
      CollectionAdminRequest.createCollection("c1", "conf1", 2, 1)
          .setMaxShardsPerNode(10)
          . process(cluster.getSolrClient());
      cluster.waitForActiveCollection("c1", 2, 2);
      CollectionAdminRequest.createCollection("c2", "conf2", 2, 1)
          .setMaxShardsPerNode(10)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection("c2", 2, 2);

      System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
      JettySolrRunner coordinatorJetty;
      try {
        coordinatorJetty = cluster.startJettySolrRunner();
      } finally {
        System.clearProperty(NodeRoles.NODE_ROLES_PROP);
      }

      // Tricky to test configset, since operation such as collection status would direct it to the
      // OS node.
      // So we use query and check the cache response header, which is determined by the
      // solr-config.xml in the configset
      // However using solr client would drop cache response header, hence we need to use the
      // underlying httpClient which has SSL correctly configured

      try (HttpSolrClient solrClient =
               new HttpSolrClient.Builder(coordinatorJetty.getBaseUrl().toString()).build()) {
        HttpResponse response =
            solrClient
                .getHttpClient()
                .execute(new HttpGet(coordinatorJetty.getBaseUrl() + "/c1/select?q=*:*"));
        // conf1 has no cache-control
        assertNull(response.getFirstHeader("cache-control"));

        response =
            solrClient
                .getHttpClient()
                .execute(new HttpGet(coordinatorJetty.getBaseUrl() + "/c2/select?q=*:*"));
        // conf2 has cache-control defined
        assertTrue(response.getFirstHeader("cache-control").getValue().contains("max-age=30"));
      }
    } finally {
      cluster.shutdown();
    }
  }

  public void testWatch() throws Exception {
    final int DATA_NODE_COUNT = 1;
    MiniSolrCloudCluster cluster =
        configureCluster(DATA_NODE_COUNT)
            .addConfig("conf1", configset("cloud-minimal"))
            .configure();
    final String TEST_COLLECTION_1 = "c1";
    final String TEST_COLLECTION_2 = "c2";

    try {
      CloudSolrClient client = cluster.getSolrClient();
      CollectionAdminRequest.createCollection(TEST_COLLECTION_1, "conf1", 1, 2)
          .setMaxShardsPerNode(10)
          .process(client);
      cluster.waitForActiveCollection(TEST_COLLECTION_1, 1, 2);
      System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
      JettySolrRunner coordinatorJetty;
      try {
        coordinatorJetty = cluster.startJettySolrRunner();
      } finally {
        System.clearProperty(NodeRoles.NODE_ROLES_PROP);
      }

      ZkStateReader zkStateReader =
          coordinatorJetty.getCoreContainer().getZkController().getZkStateReader();
      ZkStateReaderAccessor zkWatchAccessor = new ZkStateReaderAccessor(zkStateReader);

      // no watch at first
      assertTrue(!zkWatchAccessor.getWatchedCollections().contains(TEST_COLLECTION_1));
      new QueryRequest(new SolrQuery("*:*"))
          .setPreferredNodes(ImmutableList.of(coordinatorJetty.getNodeName()))
          .process(client, TEST_COLLECTION_1); // ok no exception thrown

      // now it should be watching it after the query
      assertTrue(zkWatchAccessor.getWatchedCollections().contains(TEST_COLLECTION_1));

      // add another collection
      CollectionAdminRequest.createCollection(TEST_COLLECTION_2, "conf1", 1, 2).process(client);
      cluster.waitForActiveCollection(TEST_COLLECTION_2, 1, 2);
      new QueryRequest(new SolrQuery("*:*"))
          .setPreferredNodes(ImmutableList.of(coordinatorJetty.getNodeName()))
          .process(client, TEST_COLLECTION_2);
      // watch both collections
      assertTrue(zkWatchAccessor.getWatchedCollections().contains(TEST_COLLECTION_1));
      assertTrue(zkWatchAccessor.getWatchedCollections().contains(TEST_COLLECTION_2));

      CollectionAdminRequest.deleteReplica(TEST_COLLECTION_1, "shard1", 1).process(client);
      cluster.waitForActiveCollection(TEST_COLLECTION_1, 1, 1);
      new QueryRequest(new SolrQuery("*:*"))
          .setPreferredNodes(ImmutableList.of(coordinatorJetty.getNodeName()))
          .process(client, TEST_COLLECTION_1); // ok no exception thrown

      // still one replica left, should not remove the watch
      assertTrue(zkWatchAccessor.getWatchedCollections().contains(TEST_COLLECTION_1));

      // now delete c1 and ensure it's cleared from various logic
      CollectionAdminRequest.deleteCollection(TEST_COLLECTION_1).process(client);
      zkStateReader.waitForState(TEST_COLLECTION_1, 30, TimeUnit.SECONDS, Objects::isNull);
      assertNull(zkStateReader.getCollection(TEST_COLLECTION_1)); // check the cluster state

      // ensure querying throws exception
      assertExceptionThrownWithMessageContaining(
          SolrException.class,
          ImmutableList.of("Collection not found"),
          () ->
              new QueryRequest(new SolrQuery("*:*"))
                  .setPreferredNodes(ImmutableList.of(coordinatorJetty.getNodeName()))
                  .process(client, TEST_COLLECTION_1));

      // watch should be removed after c1 deletion
      assertTrue(!zkWatchAccessor.getWatchedCollections().contains(TEST_COLLECTION_1));
      // still watching c2
      assertTrue(zkWatchAccessor.getWatchedCollections().contains(TEST_COLLECTION_2));
    } finally {
      cluster.shutdown();
    }
  }

  public void testSplitShard() throws Exception {
    final int DATA_NODE_COUNT = 1;
    MiniSolrCloudCluster cluster =
        configureCluster(DATA_NODE_COUNT)
            .addConfig("conf1", configset("cloud-minimal"))
            .configure();

    try {

      final String COLLECTION_NAME = "c1";
      CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf1", 1, 1)
          .setMaxShardsPerNode(10)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 1, 1);

      int DOC_PER_COLLECTION_COUNT = 1000;
      UpdateRequest ur = new UpdateRequest();
      for (int i = 0; i < DOC_PER_COLLECTION_COUNT; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", COLLECTION_NAME + "-" + i);
        ur.add(doc);
      }
      CloudSolrClient client = cluster.getSolrClient();
      ur.commit(client, COLLECTION_NAME);

      System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
      JettySolrRunner coordinatorJetty;
      try {
        coordinatorJetty = cluster.startJettySolrRunner();
      } finally {
        System.clearProperty(NodeRoles.NODE_ROLES_PROP);
      }

      QueryResponse response = (QueryResponse) new QueryRequest(new SolrQuery("*:*"))
          .setPreferredNodes(ImmutableList.of(coordinatorJetty.getNodeName()))
          .process(client, COLLECTION_NAME);

      assertEquals(DOC_PER_COLLECTION_COUNT, response.getResults().getNumFound());

      // now split the shard
      CollectionAdminRequest.splitShard(COLLECTION_NAME).setShardName("shard1").process(client);
      waitForState(
          "Failed to wait for child shards after split",
          COLLECTION_NAME,
          (liveNodes, collectionState) ->
              collectionState.getSlice("shard1_0") != null
                  && collectionState.getSlice("shard1_0").getState() == Slice.State.ACTIVE
                  && collectionState.getSlice("shard1_1") != null
                  && collectionState.getSlice("shard1_1").getState() == Slice.State.ACTIVE);

      // delete the parent shard
      CollectionAdminRequest.deleteShard(COLLECTION_NAME, "shard1").process(client);
      waitForState(
          "Parent shard is not yet deleted after split",
          COLLECTION_NAME,
          (liveNodes, collectionState) -> collectionState.getSlice("shard1") == null);

      response =
          (QueryResponse) new QueryRequest(new SolrQuery("*:*"))
              .setPreferredNodes(ImmutableList.of(coordinatorJetty.getNodeName()))
              .process(client, COLLECTION_NAME);

      assertEquals(DOC_PER_COLLECTION_COUNT, response.getResults().getNumFound());
    } finally {
      cluster.shutdown();
    }
  }

  public void testMoveReplica() throws Exception {
    final int DATA_NODE_COUNT = 2;
    MiniSolrCloudCluster cluster =
        configureCluster(DATA_NODE_COUNT)
            .addConfig("conf1", configset("cloud-minimal"))
            .configure();

    List<String> dataNodes =
        cluster.getJettySolrRunners().stream()
            .map(JettySolrRunner::getNodeName)
            .collect(Collectors.toList());
    try {

      final String COLLECTION_NAME = "c1";
      String fromNode = dataNodes.get(0); // put the shard on first data node
      CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf1", 1, 1)
          .setCreateNodeSet(fromNode)
          .process(cluster.getSolrClient());
      // ensure replica is placed on the expected node
      waitForState(
          "Cannot find replica on first node yet",
          COLLECTION_NAME,
          (liveNodes, collectionState) -> {
            if (collectionState.getReplicas().size() == 1) {
              Replica replica = collectionState.getReplicas().get(0);
              return fromNode.equals(replica.getNodeName())
                  && replica.getState() == Replica.State.ACTIVE;
            }
            return false;
          });

      int DOC_PER_COLLECTION_COUNT = 1000;
      UpdateRequest ur = new UpdateRequest();
      for (int i = 0; i < DOC_PER_COLLECTION_COUNT; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", COLLECTION_NAME + "-" + i);
        ur.add(doc);
      }
      CloudSolrClient client = cluster.getSolrClient();
      ur.commit(client, COLLECTION_NAME);

      System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
      JettySolrRunner coordinatorJetty;
      try {
        coordinatorJetty = cluster.startJettySolrRunner();
      } finally {
        System.clearProperty(NodeRoles.NODE_ROLES_PROP);
      }

      QueryResponse response =
          (QueryResponse) new QueryRequest(new SolrQuery("*:*"))
              .setPreferredNodes(ImmutableList.of(coordinatorJetty.getNodeName()))
              .process(client, COLLECTION_NAME);

      assertEquals(DOC_PER_COLLECTION_COUNT, response.getResults().getNumFound());

      // now move the shard/replica
      String replicaName = getCollectionState(COLLECTION_NAME).getReplicas().get(0).getName();
      String toNodeName = dataNodes.get(1);
      CollectionAdminRequest.moveReplica(COLLECTION_NAME, replicaName, toNodeName).process(client);
      waitForState(
          "Cannot find replica on second node yet after repliac move",
          COLLECTION_NAME,
          (liveNodes, collectionState) -> {
            if (collectionState.getReplicas().size() == 1) {
              Replica replica = collectionState.getReplicas().get(0);
              return toNodeName.equals(replica.getNodeName())
                  && replica.getState() == Replica.State.ACTIVE;
            }
            return false;
          });

      // We must stop the first node to ensure that query directs to the correct node from
      // coordinator.
      // In case if coordinator node has the wrong info (replica on first node), it might still
      // return valid result if
      // we do not stop the first node as first node might forward the query to second node.
      cluster.getJettySolrRunners().get(0).stop();

      response =
          (QueryResponse) new QueryRequest(new SolrQuery("*:*"))
              .setPreferredNodes(ImmutableList.of(coordinatorJetty.getNodeName()))
              .process(client, COLLECTION_NAME);

      assertEquals(DOC_PER_COLLECTION_COUNT, response.getResults().getNumFound());
    } finally {
      cluster.shutdown();
    }
  }
}
