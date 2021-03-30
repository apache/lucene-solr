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

package org.apache.solr.handler;

import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.ReplicationHandler.CMD_FETCH_INDEX;
import static org.apache.solr.handler.ReplicationHandler.CMD_GET_FILE_LIST;
import static org.apache.solr.handler.TestReplicationHandler.createAndStartJetty;
import static org.apache.solr.handler.TestReplicationHandler.createNewSolrClient;
import static org.apache.solr.handler.TestReplicationHandler.invokeReplicationCommand;

@LogLevel("org.apache.solr.handler.IndexFetcher=DEBUG")
@SolrTestCaseJ4.SuppressSSL
public class TestReplicationHandlerDiskOverFlow extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String expectedErr = "Search is temporarily disabled";
  Function<String, Long> originalDiskSpaceprovider = null;
  BooleanSupplier originalTestWait = null;
  
  JettySolrRunner leaderJetty, followerJetty;
  SolrClient leaderClient, followerClient;
  TestReplicationHandler.SolrInstance leader = null, follower = null;

  static String context = "/solr";

  @Before
  public void setUp() throws Exception {
    originalDiskSpaceprovider = IndexFetcher.usableDiskSpaceProvider;
    originalTestWait = IndexFetcher.testWait;
    
    super.setUp();
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    String factory = random().nextInt(100) < 75 ? "solr.NRTCachingDirectoryFactory" : "solr.StandardDirectoryFactory"; // test the default most of the time
    System.setProperty("solr.directoryFactory", factory);
    leader = new TestReplicationHandler.SolrInstance(createTempDir("solr-instance").toFile(), "leader", null);
    leader.setUp();
    leaderJetty = createAndStartJetty(leader);
    leaderClient = createNewSolrClient(leaderJetty.getLocalPort());
    System.setProperty("solr.tests." + HttpShardHandlerFactory.INIT_SHARDS_WHITELIST, leaderJetty.getBaseUrl().toString());

    follower = new TestReplicationHandler.SolrInstance(createTempDir("solr-instance").toFile(), "follower", leaderJetty.getLocalPort());
    follower.setUp();
    followerJetty = createAndStartJetty(follower);
    followerClient = createNewSolrClient(followerJetty.getLocalPort());

    System.setProperty("solr.indexfetcher.sotimeout2", "45000");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (null != leaderJetty) {
      leaderJetty.stop();
      leaderJetty = null;
    }
    if (null != followerJetty) {
      followerJetty.stop();
       followerJetty = null;
    }
    leader = follower = null;
    if (null != leaderClient) {
      leaderClient.close();
      leaderClient = null;
    }
    if (null != followerClient) {
      followerClient.close();
      followerClient = null;
    }
    System.clearProperty("solr.tests." + HttpShardHandlerFactory.INIT_SHARDS_WHITELIST);
    System.clearProperty("solr.indexfetcher.sotimeout");
    
    IndexFetcher.usableDiskSpaceProvider = originalDiskSpaceprovider;
    IndexFetcher.testWait = originalTestWait;
  }

  @Test
  public void testDiskOverFlow() throws Exception {
    invokeReplicationCommand(followerJetty.getLocalPort(), "disablepoll");
    //index docs
    log.info("Indexing to LEADER");
    int docsInLeader = 1000;
    long szLeader = indexDocs(leaderClient, docsInLeader, 0);
    log.info("Indexing to FOLLOWER");
    long szFollower = indexDocs(followerClient, 1200, 1000);

    IndexFetcher.usableDiskSpaceProvider = new Function<String, Long>() {
      @Override
      public Long apply(String s) {
        return szLeader;
      }
    };

    // we don't need/want the barrier to be cyclic, so we use a ref that our barrier action will null
    // out to prevent it from being triggered multiple times (which shouldn't happen anyway)
    final AtomicReference<CyclicBarrier> commonBarrier = new AtomicReference<>();
    commonBarrier.set(new CyclicBarrier(2, () -> { commonBarrier.set(null); }));
    
    final List<Throwable> threadFailures = new ArrayList<>(7);
    
    IndexFetcher.testWait = new BooleanSupplier() {
      @Override
      public boolean getAsBoolean() {
        try {
          final CyclicBarrier barrier = commonBarrier.get();
          if (null != barrier) {
            barrier.await(60, TimeUnit.SECONDS);
          }
        } catch (Exception e) {
          log.error("IndexFetcher Thread Failure", e);
          threadFailures.add(e);
        }
        return true;
      }
    };
    
    new Thread(() -> {
        try {
          for (int i = 0; i < 100; i++) {
            final CyclicBarrier barrier = commonBarrier.get();
            assertNotNull("why is query thread still looping if barrier has already been cleared?",
                          barrier);
            try {
              QueryResponse rsp = followerClient.query(new SolrQuery()
                                                    .setQuery("*:*")
                                                    .setRows(0));
              Thread.sleep(200);
            } catch (SolrException e) {
              if (e.code() == SolrException.ErrorCode.SERVICE_UNAVAILABLE.code
                  && e.getMessage().contains(expectedErr)
                  ) { 
                log.info("Got expected exception", e);
                // now let the barrier complete & clear itself, and we're done
                barrier.await(60, TimeUnit.SECONDS);
                return; // break out
              }
              // else...
              // not our expected exception, re-throw to fail fast...
              throw e;
            }
          }
          // if we made it this far, something is wrong...
          throw new RuntimeException("Query thread gave up waiting for expected error: " + expectedErr);
        } catch (Exception e) {
          log.error("Query Thread Failure", e);
          threadFailures.add(e);
        }
      }).start();

    QueryResponse response = followerClient.query(new SolrQuery()
                                               .add("qt", "/replication")
                                               .add("command", CMD_FETCH_INDEX)
                                               .add("wait", "true")
                                               );
    assertEquals("Replication command status",
                 "OK", response._getStr("status", null));
    
    assertEquals("threads encountered failures (see logs for when)",
                 Collections.emptyList(), threadFailures);

    response = followerClient.query(new SolrQuery().setQuery("*:*").setRows(0));
    assertEquals("docs in follower", docsInLeader, response.getResults().getNumFound());

    response = followerClient.query(new SolrQuery()
        .add("qt", "/replication")
        .add("command", ReplicationHandler.CMD_DETAILS)
    );
    if (log.isInfoEnabled()) {
      log.info("DETAILS {}", Utils.writeJson(response, new StringWriter(), true).toString());
    }
    assertEquals("follower's clearedLocalIndexFirst (from rep details)",
                 "true", response._getStr("details/slave/clearedLocalIndexFirst", null));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private long indexDocs(SolrClient client, int totalDocs, int start) throws Exception {
    for (int i = 0; i < totalDocs; i++)
      TestReplicationHandler.index(client, "id", i + start, "name", TestUtil.randomSimpleString(random(), 1000, 5000));
    client.commit(true, true);
    QueryResponse response = client.query(new SolrQuery()
        .add("qt", "/replication")
        .add("command", "filelist")
        .add("generation", "-1"));

    long totalSize = 0;
    for (Map map : (List<Map>) response.getResponse().get(CMD_GET_FILE_LIST)) {
      Long sz = (Long) map.get(ReplicationHandler.SIZE);
      totalSize += sz;
    }
    return totalSize;
  }

}
