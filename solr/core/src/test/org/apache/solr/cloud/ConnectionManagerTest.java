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
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.DefaultConnectionStrategy;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.Ignore;
import org.junit.Test;

@Slow
public class ConnectionManagerTest extends SolrTestCaseJ4 {
  
  static final int TIMEOUT = 3000;
  
  @Ignore
  public void testConnectionManager() throws Exception {
    
    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();
      
      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      ConnectionManager cm = zkClient.getConnectionManager();
      try {
        assertFalse(cm.isLikelyExpired());

        zkClient.getSolrZooKeeper().closeCnxn();
        
        long sessionId = zkClient.getSolrZooKeeper().getSessionId();
        server.expire(sessionId);
        Thread.sleep(TIMEOUT);
        
        assertTrue(cm.isLikelyExpired());
      } finally {
        cm.close();
        zkClient.close();
      }
    } finally {
      server.shutdown();
    }
  }

  public void testLikelyExpired() throws Exception {

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      ConnectionManager cm = zkClient.getConnectionManager();
      try {
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
        cm.process(new WatchedEvent(EventType.None, KeeperState.Disconnected, ""));
        // disconnect shouldn't immediately set likelyExpired
        assertFalse(cm.isConnectedAndNotClosed());
        assertFalse(cm.isLikelyExpired());

        // but it should after the timeout
        Thread.sleep((long)(zkClient.getZkClientTimeout() * 1.5));
        assertFalse(cm.isConnectedAndNotClosed());
        assertTrue(cm.isLikelyExpired());

        // even if we disconnect immediately again
        cm.process(new WatchedEvent(EventType.None, KeeperState.Disconnected, ""));
        assertFalse(cm.isConnectedAndNotClosed());
        assertTrue(cm.isLikelyExpired());

        // reconnect -- should no longer be likely expired
        cm.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, ""));
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
      } finally {
        cm.close();
        zkClient.close();
      }
    } finally {
      server.shutdown();
    }
  }
  
  @Test
  @LuceneTestCase.BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-15848")
  public void testReconnectWhenZkDisappeared() throws Exception {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new SolrNamedThreadFactory("connectionManagerTest"));
    
    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();
      
      MockZkClientConnectionStrategy strat = new MockZkClientConnectionStrategy();
      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT, strat , null);
      ConnectionManager cm = zkClient.getConnectionManager();
      
      try {
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
               
        // reconnect -- should no longer be likely expired
        cm.process(new WatchedEvent(EventType.None, KeeperState.Expired, ""));
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
        assertTrue(strat.isExceptionThrow());
      } finally {
        cm.close();
        zkClient.close();
        executor.shutdown();
      }
    } finally {
      server.shutdown();
    }
  }
  
  private static class MockZkClientConnectionStrategy extends DefaultConnectionStrategy {
    int called = 0;
    boolean exceptionThrown = false;
    
    @Override
    public void reconnect(final String serverAddress, final int zkClientTimeout,
        final Watcher watcher, final ZkUpdate updater) throws IOException, InterruptedException, TimeoutException {
      
      if(called++ < 1) {
        exceptionThrown = true;
        throw new IOException("Testing");
      }
      
      super.reconnect(serverAddress, zkClientTimeout, watcher, updater);
    }
    
    public boolean isExceptionThrow() {
      return exceptionThrown;
    }
  }
}
