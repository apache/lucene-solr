package org.apache.solr.cloud;

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

import org.apache.commons.collections.CollectionUtils;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RollingRestartTest extends AbstractFullDistribZkTestBase {
  public static Logger log = LoggerFactory.getLogger(ChaosMonkeyNothingIsSafeTest.class);

  private static final long MAX_WAIT_TIME = TimeUnit.NANOSECONDS.convert(300, TimeUnit.SECONDS);

  public RollingRestartTest() {
    fixShardCount = true;
    sliceCount = 2;
    shardCount = TEST_NIGHTLY ? 16 : 2;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    useFactory("solr.StandardDirectoryFactory");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    System.clearProperty("numShards");
    super.tearDown();
    resetExceptionIgnores();
  }

  @Override
  public void doTest() throws Exception {
    waitForRecoveriesToFinish(false);

    restartWithRolesTest();

    waitForRecoveriesToFinish(false);
  }


  public void restartWithRolesTest() throws Exception {
    String leader = OverseerCollectionProcessor.getLeaderNode(cloudClient.getZkStateReader().getZkClient());
    assertNotNull(leader);
    log.info("Current overseer leader = {}", leader);

    cloudClient.getZkStateReader().getZkClient().printLayoutToStdOut();

    int numDesignateOverseers = TEST_NIGHTLY ? 16 : 2;
    numDesignateOverseers = Math.max(shardCount, numDesignateOverseers);
    List<String> designates = new ArrayList<>();
    List<CloudJettyRunner> designateJettys = new ArrayList<>();
    for (int i = 0; i < numDesignateOverseers; i++) {
      int n = random().nextInt(shardCount);
      String nodeName = cloudJettys.get(n).nodeName;
      log.info("Chose {} as overseer designate", nodeName);
      invokeCollectionApi(CollectionParams.ACTION, CollectionParams.CollectionAction.ADDROLE.toLower(), "role", "overseer", "node", nodeName);
      designates.add(nodeName);
      designateJettys.add(cloudJettys.get(n));
    }

    waitUntilOverseerDesignateIsLeader(cloudClient.getZkStateReader().getZkClient(), designates, MAX_WAIT_TIME);

    cloudClient.getZkStateReader().getZkClient().printLayoutToStdOut();

    boolean sawLiveDesignate = false;
    int numRestarts = 1 + random().nextInt(TEST_NIGHTLY ? 12 : 2);
    for (int i = 0; i < numRestarts; i++) {
      log.info("Rolling restart #{}", i + 1);
      for (CloudJettyRunner cloudJetty : designateJettys) {
        log.info("Restarting {}", cloudJetty);
        chaosMonkey.stopJetty(cloudJetty);
        cloudClient.getZkStateReader().updateLiveNodes();
        boolean liveDesignates = CollectionUtils.intersection(cloudClient.getZkStateReader().getClusterState().getLiveNodes(), designates).size() > 0;
        if (liveDesignates) {
          sawLiveDesignate = true;
          boolean success = waitUntilOverseerDesignateIsLeader(cloudClient.getZkStateReader().getZkClient(), designates, MAX_WAIT_TIME);
          if (!success) {
            leader = OverseerCollectionProcessor.getLeaderNode(cloudClient.getZkStateReader().getZkClient());
            if (leader == null)
              log.error("NOOVERSEER election queue is :" +
                  OverseerCollectionProcessor.getSortedElectionNodes(cloudClient.getZkStateReader().getZkClient(),
                      OverseerElectionContext.PATH + LeaderElector.ELECTION_NODE));
            fail("No overseer designate as leader found after restart #" + (i + 1) + ": " + leader);
          }
        }
        assertTrue("Unable to restart (#" + i + "): " + cloudJetty, ChaosMonkey.start(cloudJetty.jetty));
        boolean success = waitUntilOverseerDesignateIsLeader(cloudClient.getZkStateReader().getZkClient(), designates, MAX_WAIT_TIME);
        if (!success) {
          leader = OverseerCollectionProcessor.getLeaderNode(cloudClient.getZkStateReader().getZkClient());
          if (leader == null)
            log.error("NOOVERSEER election queue is :" +
                OverseerCollectionProcessor.getSortedElectionNodes(cloudClient.getZkStateReader().getZkClient(),
                    OverseerElectionContext.PATH + LeaderElector.ELECTION_NODE));
          fail("No overseer leader found after restart #" + (i + 1) + ": " + leader);
        }
        
        cloudClient.getZkStateReader().updateLiveNodes();
        sawLiveDesignate = CollectionUtils.intersection(cloudClient.getZkStateReader().getClusterState().getLiveNodes(), designates).size() > 0;
        
      }
    }
    
    assertTrue("Test may not be working if we never saw a live designate", sawLiveDesignate);

    leader = OverseerCollectionProcessor.getLeaderNode(cloudClient.getZkStateReader().getZkClient());
    assertNotNull(leader);
    log.info("Current overseer leader (after restart) = {}", leader);

    cloudClient.getZkStateReader().getZkClient().printLayoutToStdOut();
  }

  static boolean waitUntilOverseerDesignateIsLeader(SolrZkClient testZkClient, List<String> overseerDesignates, long timeoutInNanos) throws KeeperException, InterruptedException {
    long now = System.nanoTime();
    long maxTimeout = now + timeoutInNanos; // the maximum amount of time we're willing to wait to see the designate as leader
    long timeout = now + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
    boolean firstTime = true;
    int stableCheckTimeout = 2000;
    String oldleader = null;
    while (System.nanoTime() < timeout && System.nanoTime() < maxTimeout) {
      String newLeader = OverseerCollectionProcessor.getLeaderNode(testZkClient);
      if (newLeader != null && !newLeader.equals(oldleader)) {
        // the leaders have changed, let's move the timeout further
        timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
        log.info("oldLeader={} newLeader={} - Advancing timeout to: {}", oldleader, newLeader, timeout);
        oldleader = newLeader;
      }
      if (!overseerDesignates.contains(newLeader)) {
        Thread.sleep(500);
      } else {
        if (firstTime) {
          firstTime = false;
          Thread.sleep(stableCheckTimeout);
        } else {
          return true;
        }
      }
    }
    if (System.nanoTime() < maxTimeout) {
      log.error("Max wait time exceeded");
    }
    return false;
  }
}
