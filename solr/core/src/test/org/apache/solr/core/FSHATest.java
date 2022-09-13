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

package org.apache.solr.core;


import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.OMIT_HEADER;
import static org.apache.solr.common.params.CommonParams.TRUE;

public class FSHATest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long DEFAULT_TOLERANCE = 500;
  private static void assertWithinTolerance(long expected, long actual) {
    assertWithinTolerance(expected, actual, DEFAULT_TOLERANCE);
  }
  private static void assertWithinTolerance(long expected, long actual, long tolerance) {
    assertTrue("expected=" + expected + ", actual=" + actual + ", tolerance=" + tolerance, Math.abs(expected - actual) <= tolerance);
  }

  public void testNRTRestart() throws Exception {
    // we restart jetty and expect to find on disk data - need a local fs directory
    useFactory(null);
    String COLL = "ha_test_coll";
    MiniSolrCloudCluster cluster =
        configureCluster(3)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("conf2"))
            .configure();
    System.setProperty(CoreContainer.SOLR_QUERY_AGGREGATOR, "true");
    JettySolrRunner qaJetty = cluster.startJettySolrRunner();
    String qaJettyBase = qaJetty.getBaseUrl().toString();
    System.clearProperty(CoreContainer.SOLR_QUERY_AGGREGATOR);
    ExecutorService executor = ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("manipulateJetty"));
    try {
      CollectionAdminRequest.createCollection(COLL,
              "conf",
              1,
              1,
              0,
              1)
          .setMaxShardsPerNode(1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLL, 1, 2);
      DocCollection docColl = cluster.getSolrClient()
          .getClusterStateProvider()
          .getClusterState()
          .getCollection(COLL);
      Replica nrtReplica = docColl.getReplicas(EnumSet.of(Replica.Type.NRT)).get(0);
      String nrtCore = nrtReplica.getCoreName();
      assertNotNull(nrtCore);
      Replica pullReplica = docColl.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0);
      String pullCore = pullReplica.getCoreName();
      assertNotNull(pullCore);

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
        assertEquals(nrtCore, getHostCoreName(COLL, qaJettyBase, cluster.getSolrClient(), p -> p.add("shards.preference", "replica.type:NRT")));
        assertEquals(pullCore, getHostCoreName(COLL, qaJettyBase, cluster.getSolrClient(), p -> p.add("shards.preference", "replica.type:PULL")));
        // Now , kill NRT jetty
        JettySolrRunner nrtJettyF = nrtJetty;
        JettySolrRunner pullJettyF = pullJetty;
        Random r = random();
        final long establishBaselineMs = r.nextInt(1000);
        final long nrtDowntimeMs = r.nextInt(10000);
        // NOTE: for `pullServiceTimeMs`, it can't be super-short. This is just to simplify our indexing code,
        // based on the fact that our indexing is based on a PULL-node client.
        final long pullServiceTimeMs = 1000 + r.nextInt(9000);
        Future<?> jettyManipulationFuture = executor.submit(() -> {
          // we manipulate the jetty instances in a separate thread to more closely mimic the behavior we'd
          // see irl.
          try {
            Thread.sleep(establishBaselineMs);
            log.info("stopping NRT jetty ...");
            nrtJettyF.stop();
            log.info("NRT jetty stopped.");
            Thread.sleep(nrtDowntimeMs); // let NRT be down for a while
            log.info("restarting NRT jetty ...");
            nrtJettyF.start(true);
            log.info("NRT jetty restarted.");
            // once NRT is back up, we expect PULL to continue serving until the TTL on ZK state
            // used for query request routing has expired (60s). But here we force a return to NRT
            // by stopping the PULL replica after a brief delay ...
            Thread.sleep(pullServiceTimeMs);
            log.info("stopping PULL jetty ...");
            pullJettyF.stop();
            log.info("PULL jetty stopped.");
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
        String hostCore;
        long start = System.currentTimeMillis();
        long individualRequestStart = start;
        int count = 0;
        while (nrtCore.equals(hostCore = getHostCoreName(COLL, qaJettyBase, cluster.getSolrClient(), p -> p.add("shards.preference", "replica.type:NRT")))) {
          count++;
          individualRequestStart = System.currentTimeMillis();
        }
        long now = System.currentTimeMillis();
        log.info("phase1 NRT queries count={}, overall_duration={}, baseline_expected_overall_duration={}, switch-to-pull_duration={}", count, now - start, establishBaselineMs, now - individualRequestStart);
        // default tolerance of 500ms below should suffice. Failover to PULL for this case should be very fast,
        // because our QA-based client already knows both replicas are active, the index is stable, so the moment
        // the client finds NRT is down it should be able to failover immediately and transparently to PULL.
        assertWithinTolerance(establishBaselineMs, now - start);
        assertEquals("when we break out of the NRT query loop, should be b/c routed to PULL", pullCore, hostCore);
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", "345");
        d.addField("desc_s", "Another Document");
        // attempts to add another doc while NRT is down should fail, then eventually succeed when NRT comes back up
        count = 0;
        start = System.currentTimeMillis();
        individualRequestStart = start;
        for ( ; ; ) {
          try {
            client.add(COLL, d);
            client.commit(COLL);
            break;
          } catch (SolrException ex) {
            // we expect these until nrtJetty is back up.
            count++;
            Thread.sleep(100);
          }
          individualRequestStart = System.currentTimeMillis();
        }
        now = System.currentTimeMillis();
        log.info("successfully added another doc; duration: {}, overall_duration={}, baseline_expected_overall_duration={}, exception_count={}", now - individualRequestStart, now - start, nrtDowntimeMs, count);
        // NRT replica is back up, registered as available with Zk, and availability info has been pulled down by
        // our PULL-replica-based `client`, forwarded indexing command to NRT, index/commit completed. All of this
        // accounts for the 3000ms tolerance allowed for below. This is not a strict value, and if it causes failures
        // regularly we should feel free to increase the tolerance; but it's meant to provide a stable baseline from
        // which to detect regressions.
        assertWithinTolerance(nrtDowntimeMs, now - start, 3000);
        count = 0;
        start = System.currentTimeMillis();
        individualRequestStart = start;
        while (pullCore.equals(hostCore = getHostCoreName(COLL, qaJettyBase, cluster.getSolrClient(), p -> {
          p.set(CommonParams.Q, "id:345");
          p.add("shards.preference", "replica.type:NRT");
        }))) {
          count++;
          Thread.sleep(100);
          individualRequestStart = System.currentTimeMillis();
        }
        now = System.currentTimeMillis();
        log.info("query retries between NRT index-ready and query-ready: {}; overall_duration={}; baseline_expected_overall_duration={}; failover-request_duration={}", count, now - start, pullServiceTimeMs, now - individualRequestStart);
        assertWithinTolerance(pullServiceTimeMs, now - start, 1000);
        assertEquals(nrtCore, hostCore);
        // allow any exceptions to propagate
        jettyManipulationFuture.get();
        if (true) return;

        // next phase: just toggle a bunch
        // TODO: could separate this out into a different test method, but this should suffice for now
        pullJetty.start(true);
        AtomicBoolean done = new AtomicBoolean();
        long runMinutes = 1;
        long finishTimeMs = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(runMinutes, TimeUnit.MINUTES);
        JettySolrRunner[] jettys = new JettySolrRunner[] { nrtJettyF, pullJettyF };
        Random threadRandom = new Random(r.nextInt());
        Future<Integer> f = executor.submit(() -> {
          int iteration = 0;
          while (System.currentTimeMillis() < finishTimeMs && !done.get()) {
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
        start = System.currentTimeMillis();
        try {
          do {
            pullCore.equals(hostCore = getHostCoreName(COLL, qaJettyBase, cluster.getSolrClient(), p -> {
              p.set(CommonParams.Q, "id:345");
              p.add("shards.preference", "replica.type:NRT");
            }));
            count++;
            Thread.sleep(100);
          } while (!done.get());
        } finally {
          final String result;
          if (done.getAndSet(true)) {
            result = "Success";
          } else {
            // not yet set to done, completed abnormally (exception will be thrown beyond `finally` block)
            result = "Failure";
          }
          Integer toggleCount = f.get();
          long secondsDuration = TimeUnit.SECONDS.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
          log.info("{}! {} seconds, {} toggles, {} requests served", result, secondsDuration, toggleCount, count);
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

  @SuppressWarnings("rawtypes")
  private String getHostCoreName(String COLL, String qaNode, CloudSolrClient solrClient, Consumer<SolrQuery> p)
      throws  Exception {

    boolean found = false;
    SolrQuery q = new SolrQuery("*:*");
    q.add("fl", "id,desc_s,_core_:[core]")
        .add(OMIT_HEADER, TRUE);
    p.accept(q);
    StringBuilder sb =  new StringBuilder(qaNode).append("/").append(COLL).append("/select?wt=javabin");
    q.forEach(e -> sb.append("&").append (e.getKey()).append("=").append(e.getValue()[0]));
    SolrDocumentList docs = null;
    for (int i = 0; i < 100; i++) {
      try {
        SimpleOrderedMap rsp = (SimpleOrderedMap) Utils.executeGET(solrClient.getHttpClient(),sb.toString(), Utils.JAVABINCONSUMER);
        docs = (SolrDocumentList) rsp.get("response");
        if (docs.size() > 0) {
          found = true;
          break;
        }
      } catch (SolrException ex) {
        // we know we're doing tricky things that might cause transient errors
        // TODO: all these query requests go to the QA node -- should QA propagate internal request errors
        //  to the external client (and the external client retry?) or should QA attempt to failover transparently
        //  in the event of an error?
        if (i < 5) {
          log.info("swallowing transient error", ex);
        } else {
          log.error("only expect actual _errors_ within a small window (e.g. 500ms)", ex);
          fail("initial error time threshold exceeded");
        }
      }
      Thread.sleep(100);
    }
    assertTrue(found);
    return (String) docs.get(0).getFieldValue("_core_");
  }
}
