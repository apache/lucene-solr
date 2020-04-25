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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.TestSQLHandler;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DaemonStreamApiTest extends SolrTestCaseJ4 {

  private MiniSolrCloudCluster cluster;

  private static final String SOURCE_COLL = "sourceColl";
  private static final String TARGET_COLL = "targetColl";
  private static final String CHECKPOINT_COLL = "checkpointColl";

  private static final String DAEMON_ROOT = "daemon";
  private static final String CONF_NAME = "conf";

  private static final String DAEMON_OP = "DaemonOp";

  // We want 2-5 daemons. Choose one of them to start/stop/kill to catch any off-by-one or other bookeeping errors.
  final int numDaemons = random().nextInt(3) + 2;
  String daemonOfInterest;

  List<String> daemonNames = new ArrayList<>();

  private String url;


  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    cluster = new MiniSolrCloudCluster(1, createTempDir(), buildJettyConfig("/solr"));

    url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + CHECKPOINT_COLL;

    cluster.uploadConfigSet(configset("cloud-minimal"), CONF_NAME);
    // create a single shard, single replica collection. This is necessary until SOLR-13245 since the commands
    // don't look in all replicas.
    CollectionAdminRequest.createCollection(SOURCE_COLL, CONF_NAME, 1, 1)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());

    CollectionAdminRequest.createCollection(TARGET_COLL, CONF_NAME, 1, 1)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());

    CollectionAdminRequest.createCollection(CHECKPOINT_COLL, CONF_NAME, 1, 1)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());

    for (int idx = 0; idx < numDaemons; ++idx) {
      String name = DAEMON_ROOT + idx;
      daemonNames.add(name);
    }
    daemonOfInterest = daemonNames.get(random().nextInt(numDaemons));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (null != cluster) {
      cluster.shutdown();
      cluster = null;
    }
    super.tearDown();
  }

  @Test
  public void testAPIs() throws IOException, SolrServerException, InterruptedException {

    checkCmdsNoDaemon(daemonOfInterest); // test no daemon defined

    // Now create all our daemons.
    for (String name : daemonNames) {
      createDaemon(DAEMON_DEF.replace("DAEMON_NAME", name), name);
    }

    List<Tuple> tuples = getTuples(TestSQLHandler.mapParams("qt", "/stream", "action", "list"));
    assertEquals("Should have all daemons listed", numDaemons, tuples.size());

    for (int idx = 0; idx < numDaemons; ++idx) {
      assertEquals("Daemon should be running ", tuples.get(idx).getString("id"), daemonNames.get(idx));
    }

    // Are all the daemons in a good state?
    for (String daemon : daemonNames) {
      checkAlive(daemon);
    }

    // We shouldn't be able to open a daemon twice without closing., leads to thread leeks.
    Tuple tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "start", "id", daemonOfInterest)
        , DAEMON_OP);
    assertTrue("Should not open twice without closing",
        tupleOfInterest.getString(DAEMON_OP).contains("There is already an open daemon named"));

    // Try stopping and check return.
    tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "stop", "id", daemonOfInterest),
        DAEMON_OP);
    assertTrue("Should have been able to stop the daemon",
        tupleOfInterest.getString(DAEMON_OP).contains(daemonOfInterest + " stopped"));
    checkStopped();

    // Are all the daemons alive? NOTE: a stopped daemon is still there, but in a TERMINATED state
    for (String daemon : daemonNames) {
      if (daemon.equals(daemonOfInterest) == false) {
        checkAlive(daemon);
      }
    }

    // Try starting and check return.
    tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "start", "id", daemonOfInterest),
        DAEMON_OP);
    assertTrue("Should have been able to start the daemon",
        tupleOfInterest.getString(DAEMON_OP).contains(daemonOfInterest + " started"));

    // Are all the daemons alive?
    for (String daemon : daemonNames) {
      checkAlive(daemon);
    }

    // Try killing a daemon, it should be removed from lists.
    tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "kill", "id", daemonOfInterest),
        DAEMON_OP);
    assertTrue("Daemon should have been killed",
        tupleOfInterest.getString(DAEMON_OP).contains(daemonOfInterest + " killed"));

    // Loop for a bit, waiting for the daemon to be removed from the list of possible entries.
    checkDaemonKilled(daemonOfInterest);

    // Should not be able to start a killed daemon
    tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "start", "id", daemonOfInterest),
        DAEMON_OP);
    assertTrue("Daemon should not be found",
        tupleOfInterest.getString(DAEMON_OP).contains(daemonOfInterest + " not found"));

    // Should not be able to sop a killed daemon
    tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "stop", "id", daemonOfInterest),
        DAEMON_OP);
    assertTrue("Daemon should not be found",
        tupleOfInterest.getString(DAEMON_OP).contains(daemonOfInterest + " not found"));

    // Should not be able to kill a killed daemon
    tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "kill", "id", daemonOfInterest),
        DAEMON_OP);
    assertTrue("Daemon should not be found",
        tupleOfInterest.getString(DAEMON_OP).contains(daemonOfInterest + " not found"));


    // Let's bring the killed daemon back and see if it returns in our lists. Use the method that loops a bit to check
    // in case there's a delay.
    createDaemon(DAEMON_DEF.replace("DAEMON_NAME", daemonOfInterest), daemonOfInterest);
    checkAlive(daemonOfInterest);

    // Now kill them all so the threads disappear.
    for (String daemon : daemonNames) {
        getTuples(TestSQLHandler.mapParams("qt", "/stream", "action", "kill", "id", daemon));
        checkDaemonKilled(daemon);
    }
  }

  // There can be some delay while threads stabilize, so we need to loop;
  private void checkAlive(String daemonName) throws InterruptedException, IOException {
    TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    while (timeout.hasTimedOut() == false) {
      Tuple tuple = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "list"),
          daemonName);
      String state = tuple.getString("state");
      if (state.equals("RUNNABLE") || state.equals("WAITING") || state.equals("TIMED_WAITING")) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    fail("State for daemon '" + daemonName + "' did not become RUNNABLE, WAITING or TIMED_WAITING in 10 seconds");
  }
  // There can be some delay while threads stabilize, so we need to loop. Evenutally, the statu of a stopped
  // thread should be "TERMINATED"
  private void checkStopped() throws InterruptedException, IOException {
    TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    while (timeout.hasTimedOut() == false) {
      Tuple tuple = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "list"),
          daemonOfInterest);
      if (tuple.getString("state").equals("TERMINATED")) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    fail("State for daemon '" + daemonOfInterest + "' did not become TERMINATED in 10 seconds");
  }

  private void checkDaemonKilled(String daemon) throws IOException, InterruptedException {
    TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    while (timeout.hasTimedOut() == false) {
      List<Tuple> tuples = getTuples(TestSQLHandler.mapParams("qt", "/stream", "action", "list"));
      Boolean foundIt = false;
      for (Tuple tuple : tuples) {
        if (tuple.get("id").equals(daemon)) {
          foundIt = true;
        }
      }
      if (foundIt == false) return;
      TimeUnit.MILLISECONDS.sleep(100);
    }
    fail("'" + daemonOfInterest + "' did not disappear in 10 seconds");
  }
  private void createDaemon(String daemonDef, String errMsg) throws IOException, SolrServerException {
    SolrClient client = cluster.getSolrClient();
    // create a daemon
    QueryResponse resp = client.query(CHECKPOINT_COLL, TestSQLHandler.mapParams("expr", daemonDef, "qt", "/stream"));
    assertEquals(errMsg, 0, resp.getStatus());

    // This should close and replace the current daemon and NOT leak threads.
    resp = client.query(CHECKPOINT_COLL, TestSQLHandler.mapParams("expr", daemonDef, "qt", "/stream"));
    assertEquals(errMsg, 0, resp.getStatus());
  }

  private void checkCmdsNoDaemon(String daemonName) throws IOException {

    List<Tuple> tuples = getTuples(TestSQLHandler.mapParams("qt", "/stream", "action", "list"));
    assertEquals("List should be empty", 0, tuples.size());

    Tuple tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "start", "id", daemonName),
        "DaemonOp");
    assertTrue("Start for daemon should not be found", tupleOfInterest.getString("DaemonOp").contains("not found on"));

    tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "stop", "id", daemonName),
        "DaemonOp");
    assertTrue("Stop for daemon should not be found", tupleOfInterest.getString("DaemonOp").contains("not found on"));

    tupleOfInterest = getTupleOfInterest(TestSQLHandler.mapParams("qt", "/stream", "action", "kill", "id", daemonName),
        "DaemonOp");

    assertTrue("Kill for daemon should not be found", tupleOfInterest.getString("DaemonOp").contains("not found on"));
  }

  // It's _really_ useful to have the tuples sorted....
  private List<Tuple> getTuples(final SolrParams params) throws IOException {
    return getTuples(params, null);
  }

  private List<Tuple> getTuples(final SolrParams params, String ofInterest) throws IOException {
    //log.info("Tuples from params: {}", params);
    TupleStream tupleStream = new SolrStream(url, params);

    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
    for (; ; ) {
      Tuple t = tupleStream.read();
      //log.info(" ... {}", t.fields);
      if (t.EOF) {
        break;
      } else if (ofInterest == null || t.getString("id").equals(ofInterest) || t.getString(ofInterest).equals("null") == false) {
        // a failed return is a bit different, the onlyh key is DaemonOp
        tuples.add(t);
      }
    }
    tupleStream.close();
    Collections.sort(tuples, (o1, o2) -> (o1.getString("id").compareTo(o2.getString("id"))));
    return tuples;
  }

  private Tuple getTupleOfInterest(final SolrParams params, String ofInterest) throws IOException {
    List<Tuple> tuples = getTuples(params, ofInterest);
    if (tuples.size() != 1) {
      fail("Should have found a tuple for tuple of interest: " + ofInterest);
    }
    return tuples.get(0);
  }

  private static String DAEMON_DEF =
      "  daemon(id=\"DAEMON_NAME\"," +
          "    runInterval=\"1000\"," +
          "    terminate=\"false\"," +
          "    update(targetColl," +
          "      batchSize=100," +
          "      topic(checkpointColl," +
          "        sourceColl," +
          "        q=\"*:*\"," +
          "        fl=\"id\"," +
          "        id=\"topic1\"," +
          "        initialCheckpoint=0)" +
          "))";
}
