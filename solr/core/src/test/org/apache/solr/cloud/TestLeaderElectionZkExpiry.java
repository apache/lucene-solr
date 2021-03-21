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

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.KeeperException;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore // MRM TODO: debug
public class TestLeaderElectionZkExpiry extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SOLRXML = "<solr></solr>";

  @Test
  public void testLeaderElectionWithZkExpiry() throws Exception {
    Path zkDir = SolrTestUtil.createTempDir("zkData");
    Path ccDir = SolrTestUtil.createTempDir("testLeaderElectionWithZkExpiry-solr");
    CoreContainer cc = createCoreContainer(ccDir, SOLRXML);
    final ZkTestServer server = new ZkTestServer(zkDir);
    server.setTheTickTime(1000);
    SolrZkClient zc = null;
    try {
      server.run();

      CloudConfig cloudConfig = new CloudConfig.CloudConfigBuilder("dummy.host.com", 8984, "solr")
          .setLeaderConflictResolveWait(5000)
          .setLeaderVoteWait(5000)
          .build();
      final ZkController zkController = new ZkController(cc, server.getZkClient(), cloudConfig);
      try {
        Thread killer = new Thread() {
          @Override
          public void run() {
            long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert( TEST_NIGHTLY ? 10 : 5, TimeUnit.SECONDS);
            while (System.nanoTime() < timeout) {
              long sessionId = zkController.getZkClient().getConnectionManager().getKeeper().getSessionId();
              server.expire(sessionId);
              try {
                Thread.sleep(250);
              } catch (InterruptedException e)  {}
            }
          }
        };
        killer.start();
        killer.join();
        long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(15, TimeUnit.SECONDS);
        zc = new SolrZkClient(server.getZkAddress(), 10000);
        zc.start();
        boolean found = false;
        while (System.nanoTime() < timeout) {
          try {
            String leaderNode = OverseerCollectionConfigSetProcessor.getLeaderNode(zc);
            if (leaderNode != null && !leaderNode.trim().isEmpty()) {
              if (log.isInfoEnabled()) {
                log.info("Time={} Overseer leader is = {}", System.nanoTime(), leaderNode);
              }
              found = true;
              break;
            }
          } catch (KeeperException.NoNodeException nne) {
            // ignore
          }
        }
        assertTrue(found);
      } finally {
        zkController.close();
      }
    } finally {
      if (zc != null) zc.close();
      cc.shutdown();
      server.shutdown();
    }
  }
}
