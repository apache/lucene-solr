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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.apache.solr.util.TimeOut;

import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * tests various streaming expressions (via the SolrJ {@link SolrStream} API) against a SolrCloud cluster
 * using both Authenticationand Role based Authorization
 */
public class CloudAuthStreamTest extends SolrCloudTestCase {
    
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION_X = "collection_x";
  private static final String COLLECTION_Y = "collection_y";
  
  private static final String READ_ONLY_USER = "read_only_user";
  private static final String WRITE_X_USER = "write_x_user";
  private static final String WRITE_Y_USER = "write_y_user";
  private static final String ADMIN_USER = "admin_user";
  
  private static String solrUrl = null;
  
  /**
   * Helper that returns the original {@link SolrRequest} <em>with it's original type</em> 
   * so it can be chained.  This menthod knows that for the purpose of this test, every user name 
   * is it's own password
   *
   * @see SolrRequest#setBasicAuthCredentials
   */
  @SuppressWarnings({"rawtypes"})
  private static <T extends SolrRequest> T setBasicAuthCredentials(T req, String user) {
    assert null != user;
    req.setBasicAuthCredentials(user, user);
    return req;
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    final List<String> users = Arrays.asList(READ_ONLY_USER, WRITE_X_USER, WRITE_Y_USER, ADMIN_USER);
    // For simplicity: every user uses a password the same as their name...
    final Map<String,String> credentials = users.stream()
      .collect(Collectors.toMap(Function.identity(), s -> getSaltedHashedValue(s)));
    
    // For simplicity: Every user is their own role...
    final Map<String,String> roles = users.stream()
      .collect(Collectors.toMap(Function.identity(), Function.identity()));

    final String SECURITY_JSON = Utils.toJSONString
      (Utils.makeMap("authorization",
                     Utils.makeMap("class", RuleBasedAuthorizationPlugin.class.getName(),
                                   "user-role", roles,
                                   // NOTE: permissions order matters!
                                   "permissions", Arrays.asList(// any authn user can 'read' or hit /stream
                                                                Utils.makeMap("name","read",
                                                                              "role","*"),
                                                                Utils.makeMap("name","stream",
                                                                              "collection", "*",
                                                                              "path", "/stream",
                                                                              "role","*"),
                                                                // per collection write perms
                                                                Utils.makeMap("name","update",
                                                                              "collection", COLLECTION_X,
                                                                              "role", WRITE_X_USER),
                                                                Utils.makeMap("name","update",
                                                                              "collection", COLLECTION_Y,
                                                                              "role", WRITE_Y_USER),
                                                                Utils.makeMap("name","all",
                                                                              "role",ADMIN_USER))),
                     "authentication",
                     Utils.makeMap("class", BasicAuthPlugin.class.getName(),
                                   "blockUnknown",true,
                                   "credentials", credentials)));
    
    // we want at most one core per node to force lots of network traffic to try and tickle distributed bugs
    configureCluster(5)
      .withSecurityJson(SECURITY_JSON)
      .configure();

    for (String collection : Arrays.asList(COLLECTION_X, COLLECTION_Y)) {
      CollectionAdminRequest.createCollection(collection, "_default", 2, 2)
          .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
          .setBasicAuthCredentials(ADMIN_USER, ADMIN_USER)
        .process(cluster.getSolrClient());
    }
    
    for (String collection : Arrays.asList(COLLECTION_X, COLLECTION_Y)) {
      cluster.getSolrClient().waitForState(collection, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
                                           (n, c) -> DocCollection.isFullyActive(n, c, 2, 2));
    }

    solrUrl = cluster.getRandomJetty(random()).getProxyBaseUrl().toString();
    
    log.info("All stream requests will be sent to random solrUrl: {}", solrUrl);
  }
  
  @AfterClass
  public static void clearVariables() {
    solrUrl = null;
  }
  
  @After
  public void clearCollections() throws Exception {
    log.info("Clearing Collections @After test method...");
    assertEquals(0,
                 setBasicAuthCredentials(new UpdateRequest(), WRITE_X_USER)
                 .deleteByQuery("*:*")
                 .commit(cluster.getSolrClient(), COLLECTION_X).getStatus());
    assertEquals(0,
                 setBasicAuthCredentials(new UpdateRequest(), WRITE_Y_USER)
                 .deleteByQuery("*:*")
                 .commit(cluster.getSolrClient(), COLLECTION_Y).getStatus());
  }
  
  /**
   * Simple sanity checks that authentication is working the way the test expects 
   */
  public void testSanityCheckAuth() throws Exception {
    
    assertEquals("sanity check of non authenticated query request",
                 401,
                 expectThrows(SolrException.class, () -> {
                     final long ignored = 
                       (new QueryRequest(params("q", "*:*",
                                                "rows", "0",
                                                "_trace", "no_auth_sanity_check")))
                       .process(cluster.getSolrClient(), COLLECTION_X).getResults().getNumFound();
                   }).code());
    
    assertEquals("sanity check of update to X from write_X user",
                 0,
                 (setBasicAuthCredentials(new UpdateRequest(), WRITE_X_USER)
                  .add(sdoc("id", "1_from_write_X_user"))
                  .commit(cluster.getSolrClient(), COLLECTION_X)).getStatus());

    assertEquals("sanity check of update to X from read only user",
                 500, // should be 403, but CloudSolrClient lies on updates for now: SOLR-14222 
                 expectThrows(SolrException.class, () -> {
                     final int ignored = (setBasicAuthCredentials(new UpdateRequest(), READ_ONLY_USER)
                                          .add(sdoc("id", "2_from_read_only_user"))
                                          .commit(cluster.getSolrClient(), COLLECTION_X)).getStatus();
                   }).code());
    
    assertEquals("sanity check of update to X from write_Y user",
                 500, // should be 403, but CloudSolrClient lies on updates for now: SOLR-14222 
                 expectThrows(SolrException.class, () -> {
                     final int ignored = (setBasicAuthCredentials(new UpdateRequest(), WRITE_Y_USER)
                                          .add(sdoc("id", "3_from_write_Y_user"))
                                          .commit(cluster.getSolrClient(), COLLECTION_X)).getStatus();
                   }).code());
    
    assertEquals("sanity check of update to Y from write_Y user",
                 0,
                 (setBasicAuthCredentials(new UpdateRequest(), WRITE_Y_USER)
                  .add(sdoc("id", "1_from_write_Y_user"))
                  .commit(cluster.getSolrClient(), COLLECTION_Y)).getStatus());
    
    for (String user : Arrays.asList(READ_ONLY_USER, WRITE_Y_USER, WRITE_X_USER)) {
      for (String collection : Arrays.asList(COLLECTION_X, COLLECTION_Y)) {
        assertEquals("sanity check: query "+collection+" from user: "+user,
                     1, countDocsInCollection(collection, user));
      }
    }
  }

  public void testEchoStream() throws Exception {
    final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                 params("qt", "/stream", 
                                                        "expr", "echo(hello world)"));
    solrStream.setCredentials(READ_ONLY_USER, READ_ONLY_USER);
    final List<Tuple> tuples = getTuples(solrStream);
    assertEquals(1, tuples.size());
    assertEquals("hello world", tuples.get(0).get("echo"));
  }
  
  public void testEchoStreamNoCredentials() throws Exception {
    final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                 params("qt", "/stream", 
                                                        "expr", "echo(hello world)"));
    // NOTE: no credentials
    
    // NOTE: Can't make any assertions about Exception: SOLR-14226
    expectThrows(Exception.class, () -> {
        final List<Tuple> ignored = getTuples(solrStream);
      });
  }
  
  public void testEchoStreamInvalidCredentials() throws Exception {
    final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                 params("qt", "/stream", 
                                                        "expr", "echo(hello world)"));
    solrStream.setCredentials(READ_ONLY_USER, "BOGUS_PASSWORD");
    
    // NOTE: Can't make any assertions about Exception: SOLR-14226
    expectThrows(Exception.class, () -> {
        final List<Tuple> ignored = getTuples(solrStream);
      });
  }
  
  public void testSimpleUpdateStream() throws Exception {
    final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                 params("qt", "/stream", "expr",
                                                        "update("+COLLECTION_X+",batchSize=1," +
                                                        "tuple(id=42,a_i=1,b_i=5))"));
    solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
    final List<Tuple> tuples = getTuples(solrStream);
    assertEquals(1, tuples.size());
    assertEquals(1L, tuples.get(0).get("totalIndexed"));

    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }
  
  public void testSimpleUpdateStreamInvalidCredentials() throws Exception {
    final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                 params("qt", "/stream", "expr",
                                                        "update("+COLLECTION_X+",batchSize=1," +
                                                        "tuple(id=42,a_i=1,b_i=5))"));
    // "WRITE" credentials should be required for 'update(...)'
    solrStream.setCredentials(WRITE_X_USER, "BOGUS_PASSWORD");
    
    // NOTE: Can't make any assertions about Exception: SOLR-14226
    expectThrows(Exception.class, () -> {
        final List<Tuple> ignored = getTuples(solrStream);
      });
    
    assertEquals(0L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }
  
  public void testSimpleUpdateStreamInsufficientCredentials() throws Exception {
    // both of these users have valid credentials and authz read COLLECTION_X, but neither has
    // authz to write to X...
    for (String user : Arrays.asList(READ_ONLY_USER, WRITE_Y_USER)) {
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                   params("qt", "/stream", "expr",
                                                          "update("+COLLECTION_X+",batchSize=1," +
                                                          "tuple(id=42,a_i=1,b_i=5))"));
      
      solrStream.setCredentials(user, user);
    
      // NOTE: Can't make any assertions about Exception: SOLR-14226
      expectThrows(Exception.class, () -> {
          final List<Tuple> ignored = getTuples(solrStream);
        });
    }
    
    assertEquals(0L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }
  
  public void testIndirectUpdateStream() throws Exception {
    { // WRITE_X user should be able to update X via a (dummy) stream from Y...
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_Y,
                                                   params("qt", "/stream", "expr",
                                                          "update("+COLLECTION_X+",batchSize=1," +
                                                          "tuple(id=42,a_i=1,b_i=5))"));
      solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
      final List<Tuple> tuples = getTuples(solrStream);
      assertEquals(1, tuples.size());
      assertEquals(1L, tuples.get(0).get("totalIndexed"));
    }
    
    { // Now add some "real" docs directly to Y...
      final UpdateRequest update = setBasicAuthCredentials(new UpdateRequest(), WRITE_Y_USER);
      for (int i = 1; i <= 42; i++) {
        update.add(sdoc("id",i+"y","foo_i",""+i));
      }
      assertEquals("initial docs in Y",
                   0, update.commit(cluster.getSolrClient(), COLLECTION_Y).getStatus());
    }

    { // WRITE_X user should be able to update X via a (search) stream from Y (routed via Y)
      final String expr
        = "update("+COLLECTION_X+", batchSize=50,                   " // note batch size
        + "       search("+COLLECTION_Y+",                          "
        + "              q=\"foo_i:[* TO 10]\",                     " // 10 matches = 1 batch
        + "              rows=100,                                  "
        + "              fl=\"id,foo_i,_version_\",                 " // pruneVersionField default true
        + "              sort=\"foo_i desc\"))                      "
        ;
      
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_Y, // NOTE: Y route
                                                   params("qt", "/stream",
                                                          "expr", expr));
      solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
      final List<Tuple> tuples = getTuples(solrStream);
      assertEquals(1, tuples.size());
      assertEquals(10L, tuples.get(0).get("batchIndexed"));
      assertEquals(10L, tuples.get(0).get("totalIndexed"));
    }

    { // WRITE_X user should be able to update X via a (search) stream from Y (routed via X)...
      final String expr
        = "update("+COLLECTION_X+", batchSize=5,                    " // note batch size
        + "       search("+COLLECTION_Y+",                          "
        + "              q=\"foo_i:[30 TO *]\",                     " // 13 matches = 3 batches
        + "              rows=100,                                  "
        + "              fl=\"id,foo_i\",                           "
        + "              sort=\"foo_i desc\"))                      "
        ;
      
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X, // NOTE: X route
                                                   params("qt", "/stream",
                                                          "expr", expr));
      solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
      final List<Tuple> tuples = getTuples(solrStream);
      assertEquals(3, tuples.size());
      
      assertEquals( 5L, tuples.get(0).get("batchIndexed"));
      assertEquals( 5L, tuples.get(0).get("totalIndexed"));
      
      assertEquals( 5L, tuples.get(1).get("batchIndexed"));
      assertEquals(10L, tuples.get(1).get("totalIndexed"));
      
      assertEquals( 3L, tuples.get(2).get("batchIndexed"));
      assertEquals(13L, tuples.get(2).get("totalIndexed"));
    }

    assertEquals(1L + 10L + 13L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    
  }
  
  public void testIndirectUpdateStreamInsufficientCredentials() throws Exception {
    
    // regardless of how it's routed, WRITE_Y should NOT have authz to stream updates to X...
    for (String path : Arrays.asList(COLLECTION_X, COLLECTION_Y)) {
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + path,
                                                   params("qt", "/stream", "expr",
                                                          "update("+COLLECTION_X+",batchSize=1," +
                                                          "tuple(id=42,a_i=1,b_i=5))"));
      solrStream.setCredentials(WRITE_Y_USER, WRITE_Y_USER);
    
      // NOTE: Can't make any assertions about Exception: SOLR-14226
      expectThrows(Exception.class, () -> {
          final List<Tuple> ignored = getTuples(solrStream);
        });
    }

    assertEquals(0L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }

  public void testExecutorUpdateStream() throws Exception {
      final String expr
        = "executor(threads=1,                                              "
        + "         tuple(expr_s=\"update("+COLLECTION_X+", batchSize=5,    "
        + "                               tuple(id=42,a_i=1,b_i=5))       "
        + "                      \"))                                       "
        ;
    final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                 params("qt", "/stream",
                                                        "expr", expr));
    solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
    final List<Tuple> tuples = getTuples(solrStream);
    assertEquals(0, tuples.size());

    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }

  public void testExecutorUpdateStreamInsufficientCredentials() throws Exception {
    int id = 0;
    // both of these users have valid credentials and authz read COLLECTION_X, but neither has
    // authz to write to X...
    for (String user : Arrays.asList(READ_ONLY_USER, WRITE_Y_USER)) {
      // ... regardless of how the request is routed...
      for (String path : Arrays.asList(COLLECTION_X, COLLECTION_Y)) {
        final String trace = user + ":" + path;
        final String expr
          = "executor(threads=1,                                                         "
          + "         tuple(expr_s=\"update("+COLLECTION_X+", batchSize=5,               "
          + "                               tuple(id='"+(++id)+"',foo_s='"+trace+"'))    "
          + "                      \"))                                                  "
          ;
        final SolrStream solrStream = new SolrStream(solrUrl + "/" + path,
                                                     params("qt", "/stream",
                                                            "_trace", "executor_via_" + trace,
                                                            "expr", expr));
        solrStream.setCredentials(user, user);

        // NOTE: Becaue of the backgroun threads, no failures will to be returned to client...
        final List<Tuple> tuples = getTuples(solrStream);
        assertEquals(0, tuples.size());
        
        // we have to assert that the updates failed solely based on the side effects...
        assertEquals("doc count after execute update via " + trace,
                     0L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));

      }
    }

    // sanity check
    assertEquals("final doc count",
                 0L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }

  public void testDaemonUpdateStream() throws Exception {
    final String daemonUrl = getRandomCoreUrl(COLLECTION_X);
    log.info("Using Daemon @ {}", daemonUrl);
    
    {
      final String expr
        // NOTE: inspite of what is implied by 'terminate=true', this daemon will
        // NEVER terminate on it's own as long as the updates are successful
        // (aparently that requires usage of anest topic() stream to set a "sleepMillis"?!?!?!)
        = "daemon(id=daemonId,runInterval=1000,terminate=true,           "
        + "       update("+COLLECTION_X+",tuple(id=42,a_i=1,b_i=5)))     "
        ;
      final SolrStream solrStream = new SolrStream(daemonUrl,
                                                   params("qt", "/stream",
                                                          "expr", expr));
      solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
      final List<Tuple> tuples = getTuples(solrStream);
      assertEquals(1, tuples.size()); // daemon starting status
    }
    try {
      // We have to poll the daemon 'list' to know once it's run...
      long iterations = 0;
      final TimeOut timeout = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      while ( ! timeout.hasTimedOut() ) {
        final SolrStream daemonCheck = new SolrStream(daemonUrl,
                                                      params("qt", "/stream",
                                                             "action", "list"));
        daemonCheck.setCredentials(WRITE_X_USER, WRITE_X_USER);
        final List<Tuple> tuples = getTuples(daemonCheck);
        assertEquals(1, tuples.size()); // our daemon;
        iterations = tuples.get(0).getLong("iterations");
        if (1 < iterations) {
          // once the daemon has had a chance to run, break out of TimeOut
          break;
        }
        Thread.sleep(Math.max(1, Math.min(5000, timeout.timeLeft(TimeUnit.MILLISECONDS))));
      }
      assertTrue("Didn't see any iterations after waiting an excessive amount of time: " + iterations,
                 0 < iterations);
    } finally {
      // kill the damon...
      final SolrStream daemonKiller = new SolrStream(daemonUrl,
                                                     params("qt", "/stream",
                                                            "action", "kill",
                                                            "id", "daemonId"));
      daemonKiller.setCredentials(WRITE_X_USER, WRITE_X_USER);
      final List<Tuple> tuples = getTuples(daemonKiller);
      assertEquals(1, tuples.size()); // daemon death status
    }
    
    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }
  
  public void testDaemonUpdateStreamInsufficientCredentials() throws Exception {
    final String daemonUrl = getRandomCoreUrl(COLLECTION_X);
    log.info("Using Daemon @ {}", daemonUrl);
    
    // both of these users have valid credentials and authz read COLLECTION_X, but neither has
    // authz to write to X...
    for (String user : Arrays.asList(READ_ONLY_USER, WRITE_Y_USER)) {
      final String daemonId = "daemon_" + user;
      {
        final String expr
          = "daemon(id="+daemonId+",runInterval=1000,terminate=true,           "
          + "       update("+COLLECTION_X+",tuple(id=42,a_i=1,b_i=5)))     "
          ;
        final SolrStream solrStream = new SolrStream(daemonUrl,
                                                     params("qt", "/stream",
                                                            "_trace", "start_" + daemonId,
                                                            "expr", expr));
        solrStream.setCredentials(user, user);
        final List<Tuple> tuples = getTuples(solrStream);
        assertEquals(1, tuples.size()); // daemon starting status
      }
      try {
        // We have to poll the daemon 'list' to know once it's run / terminated...
        Object state = null;
        final TimeOut timeout = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        while ( ! timeout.hasTimedOut() ) {
          final SolrStream daemonCheck = new SolrStream(daemonUrl,
                                                        params("qt", "/stream",
                                                               "_trace", "check_" + daemonId,
                                                               "action", "list"));
          daemonCheck.setCredentials(user, user);
          final List<Tuple> tuples = getTuples(daemonCheck);
          assertEquals(1, tuples.size()); // our daemon;
          if (log.isInfoEnabled()) {
            log.info("Current daemon status: {}", tuples.get(0).getFields());
          }
          assertEquals(daemonId + " should have never had a successful iteration",
                       Long.valueOf(0L), tuples.get(0).getLong("iterations"));
          state = tuples.get(0).get("state");
          if ("TERMINATED".equals(state)) {
            // once the daemon has failed, break out of TimeOut
            break;
          }
          Thread.sleep(Math.max(1, Math.min(5000, timeout.timeLeft(TimeUnit.MILLISECONDS))));
        }
        assertEquals("Timed out w/o ever getting TERMINATED state from " + daemonId,
                     "TERMINATED", state);
      } finally {
        // kill the damon...
        final SolrStream daemonKiller = new SolrStream(daemonUrl,
                                                       params("qt", "/stream",
                                                              "_trace", "kill_" + daemonId,
                                                              "action", "kill",
                                                              "id", daemonId));
        daemonKiller.setCredentials(user, user);
        final List<Tuple> tuples = getTuples(daemonKiller);
        assertEquals(1, tuples.size()); // daemon death status
      }
      
      assertEquals("doc count after daemon update for " + user,
                   0L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    }
    
    // sanity check
    assertEquals("final doc count",
                 0L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    
  }

  public void testSimpleDeleteStream() throws Exception {
    assertEquals(0,
                 (setBasicAuthCredentials(new UpdateRequest(), WRITE_X_USER)
                  .add(sdoc("id", "42"))
                  .commit(cluster.getSolrClient(), COLLECTION_X)).getStatus());
    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    
    final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                 params("qt", "/stream", "expr",
                                                        "delete("+COLLECTION_X+",batchSize=1," +
                                                        "tuple(id=42))"));
    solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
    final List<Tuple> tuples = getTuples(solrStream);
    assertEquals(1, tuples.size());
    assertEquals(1L, tuples.get(0).get("totalIndexed"));
    
    assertEquals(0L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));

  }

  /** A simple "Delete by Query" example */
  public void testSimpleDeleteStreamByQuery() throws Exception {
    { // Put some "real" docs directly to both X...
      final UpdateRequest update = setBasicAuthCredentials(new UpdateRequest(), WRITE_X_USER);
      for (int i = 1; i <= 42; i++) {
        update.add(sdoc("id",i+"x","foo_i",""+i));
      }
      assertEquals("initial docs in X",
                   0, update.commit(cluster.getSolrClient(), COLLECTION_X).getStatus());
    }
    
    assertEquals(42L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    
    { // WRITE_X user should be able to delete X via a query from X
      final String expr
        = "delete("+COLLECTION_X+", batchSize=5,                    " // note batch size
        + "       search("+COLLECTION_X+",                          "
        + "              q=\"foo_i:[* TO 10]\",                     " // 10 matches = 2 batches
        + "              rows=100,                                  "
        + "              fl=\"id,foo_i,_version_\",                 " // foo_i should be ignored...
        + "              sort=\"foo_i desc\"))                      " // version constraint should be ok
        ;

      final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                   params("qt", "/stream",
                                                          "expr", expr));
      solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
      final List<Tuple> tuples = getTuples(solrStream);
      assertEquals(2, tuples.size());
      assertEquals(5L, tuples.get(0).get("totalIndexed"));
      assertEquals(10L, tuples.get(1).get("totalIndexed"));
    }
    
    assertEquals(42L - 10L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }

  
  public void testSimpleDeleteStreamInvalidCredentials() throws Exception {
    assertEquals(0,
                 (setBasicAuthCredentials(new UpdateRequest(), WRITE_X_USER)
                  .add(sdoc("id", "42"))
                  .commit(cluster.getSolrClient(), COLLECTION_X)).getStatus());
    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    
    final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                 params("qt", "/stream", "expr",
                                                        "update("+COLLECTION_X+",batchSize=1," +
                                                        "tuple(id=42))"));
    // "WRITE" credentials should be required for 'update(...)'
    solrStream.setCredentials(WRITE_X_USER, "BOGUS_PASSWORD");
    
    // NOTE: Can't make any assertions about Exception: SOLR-14226
    expectThrows(Exception.class, () -> {
        final List<Tuple> ignored = getTuples(solrStream);
      });
    
    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }


  public void testSimpleDeleteStreamInsufficientCredentials() throws Exception {
    assertEquals(0,
                 (setBasicAuthCredentials(new UpdateRequest(), WRITE_X_USER)
                  .add(sdoc("id", "42"))
                  .commit(cluster.getSolrClient(), COLLECTION_X)).getStatus());
    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    
    // both of these users have valid credentials and authz read COLLECTION_X, but neither has
    // authz to write to X...
    for (String user : Arrays.asList(READ_ONLY_USER, WRITE_Y_USER)) {
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X,
                                                   params("qt", "/stream", "expr",
                                                          "update("+COLLECTION_X+",batchSize=1," +
                                                          "tuple(id=42))"));
      
      solrStream.setCredentials(user, user);
    
      // NOTE: Can't make any assertions about Exception: SOLR-14226
      expectThrows(Exception.class, () -> {
          final List<Tuple> ignored = getTuples(solrStream);
        });
    }
    
    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }
  
  public void testIndirectDeleteStream() throws Exception {
    { // Put some "real" docs directly to both X & Y...
      final UpdateRequest xxx_Update = setBasicAuthCredentials(new UpdateRequest(), WRITE_X_USER);
      final UpdateRequest yyy_Update = setBasicAuthCredentials(new UpdateRequest(), WRITE_Y_USER);
      for (int i = 1; i <= 42; i++) {
        xxx_Update.add(sdoc("id",i+"z","foo_i",""+i));
        yyy_Update.add(sdoc("id",i+"z","foo_i",""+i));
      }
      assertEquals("initial docs in X",
                   0, xxx_Update.commit(cluster.getSolrClient(), COLLECTION_X).getStatus());
      assertEquals("initial docs in Y",
                   0, yyy_Update.commit(cluster.getSolrClient(), COLLECTION_Y).getStatus());
    }
    
    assertEquals(42L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    assertEquals(42L, commitAndCountDocsInCollection(COLLECTION_Y, WRITE_Y_USER));
    
    { // WRITE_X user should be able to delete X via a (dummy) stream from Y...
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_Y,
                                                   params("qt", "/stream", "expr",
                                                          "delete("+COLLECTION_X+",batchSize=1," +
                                                          "tuple(id=42z))"));
      solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
      final List<Tuple> tuples = getTuples(solrStream);
      assertEquals(1, tuples.size());
      assertEquals(1L, tuples.get(0).get("totalIndexed"));
    }

    assertEquals(42L - 1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    assertEquals(42L, commitAndCountDocsInCollection(COLLECTION_Y, WRITE_Y_USER));
    
    { // WRITE_X user should be able to delete ids from X via a (search) stream from Y (routed via Y)
      final String expr
        = "delete("+COLLECTION_X+", batchSize=50,                   " // note batch size
        + "       pruneVersionField=true,                           " // NOTE: ignoring Y version to del X
        + "       search("+COLLECTION_Y+",                          "
        + "              q=\"foo_i:[* TO 10]\",                     " // 10 matches = 1 batch
        + "              rows=100,                                  "
        + "              fl=\"id,foo_i,_version_\",                 " // foo_i & version should be ignored
        + "              sort=\"foo_i desc\"))                      "
        ;
      
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_Y, // NOTE: Y route
                                                   params("qt", "/stream",
                                                          "expr", expr));
      solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
      final List<Tuple> tuples = getTuples(solrStream);
      assertEquals(1, tuples.size());
      assertEquals(10L, tuples.get(0).get("batchIndexed"));
      assertEquals(10L, tuples.get(0).get("totalIndexed"));

    }

    assertEquals(42L - 1L - 10L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    assertEquals(42L, commitAndCountDocsInCollection(COLLECTION_Y, WRITE_Y_USER));
      
    { // WRITE_X user should be able to delete ids from X via a (search) stream from Y (routed via X)...
      final String expr
        = "delete("+COLLECTION_X+", batchSize=5,                    " // note batch size
        + "       search("+COLLECTION_Y+",                          "
        + "              q=\"foo_i:[30 TO *]\",                     " // 13 matches = 3 batches
        + "              rows=100,                                  "
        + "              fl=\"id,foo_i\",                           " // foo_i should be ignored
        + "              sort=\"foo_i desc\"))                      "
        ;
      
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + COLLECTION_X, // NOTE: X route
                                                   params("qt", "/stream",
                                                          "expr", expr));
      solrStream.setCredentials(WRITE_X_USER, WRITE_X_USER);
      final List<Tuple> tuples = getTuples(solrStream);
      assertEquals(3, tuples.size());
      
      assertEquals( 5L, tuples.get(0).get("batchIndexed"));
      assertEquals( 5L, tuples.get(0).get("totalIndexed"));
      
      assertEquals( 5L, tuples.get(1).get("batchIndexed"));
      assertEquals(10L, tuples.get(1).get("totalIndexed"));
      
      assertEquals( 3L, tuples.get(2).get("batchIndexed"));
      assertEquals(13L, tuples.get(2).get("totalIndexed"));
    }

    assertEquals(42L - 1L - 10L - (13L - 1L), // '42' in last 13 deletes was already deleted from X
                 commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    assertEquals(42L, commitAndCountDocsInCollection(COLLECTION_Y, WRITE_Y_USER));
    
  }

  public void testIndirectDeleteStreamInsufficientCredentials() throws Exception {
    assertEquals(0,
                 (setBasicAuthCredentials(new UpdateRequest(), WRITE_X_USER)
                  .add(sdoc("id", "42"))
                  .commit(cluster.getSolrClient(), COLLECTION_X)).getStatus());
    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
    
    // regardless of how it's routed, WRITE_Y should NOT have authz to delete from X...
    for (String path : Arrays.asList(COLLECTION_X, COLLECTION_Y)) {
      final SolrStream solrStream = new SolrStream(solrUrl + "/" + path,
                                                   params("qt", "/stream", "expr",
                                                          "delete("+COLLECTION_X+",batchSize=1," +
                                                          "tuple(id=42))"));
      solrStream.setCredentials(WRITE_Y_USER, WRITE_Y_USER);
    
      // NOTE: Can't make any assertions about Exception: SOLR-14226
      expectThrows(Exception.class, () -> {
          final List<Tuple> ignored = getTuples(solrStream);
        });
    }

    assertEquals(1L, commitAndCountDocsInCollection(COLLECTION_X, WRITE_X_USER));
  }

  /**
   * Helper method that uses the specified user to (first commit, and then) count the total 
   * number of documents in the collection
   */
  protected static long commitAndCountDocsInCollection(final String collection,
                                                   final String user) throws Exception {
    assertEquals(0, setBasicAuthCredentials(new UpdateRequest(), user).commit(cluster.getSolrClient(),
                                                                              collection).getStatus());
    return countDocsInCollection(collection, user);
  }
  
  /**
   * Helper method that uses the specified user to count the total number of documents in the collection
   */
  protected static long countDocsInCollection(final String collection,
                                           final String user) throws Exception {
    return setBasicAuthCredentials(new QueryRequest(params("q", "*:*",
                                                           "rows", "0",
                                                           "_trace", "count_via_" + user + ":" + collection)),
                                   user)
      .process(cluster.getSolrClient(), collection).getResults().getNumFound();
  }
  
  /**
   * Slurps a stream into a List
   */
  protected static List<Tuple> getTuples(final TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<Tuple>();
    try {
      log.trace("TupleStream: {}", tupleStream);
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        if (log.isTraceEnabled()) {
          log.trace("Tuple: {}", t.getFields());
        }
        tuples.add(t);
      }
    } finally {
      tupleStream.close();
    }
    return tuples;
  }

  /**
   * Sigh.  DaemonStream requires polling the same core where the stream was exectured.
   */
  protected static String getRandomCoreUrl(final String collection) throws Exception {
    final List<String> replicaUrls = 
      cluster.getSolrClient().getZkStateReader().getClusterState()
      .getCollectionOrNull(collection).getReplicas().stream()
      .map(Replica::getCoreUrl).collect(Collectors.toList());
    Collections.shuffle(replicaUrls, random());
    return replicaUrls.get(0);
  }

}
