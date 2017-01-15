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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkOperation;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ZkSolrClientTest extends AbstractSolrTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  class ZkConnection implements AutoCloseable {

    private ZkTestServer server = null;
    private SolrZkClient zkClient = null;

    ZkConnection() throws Exception {
      this (true);
    }

    ZkConnection(boolean makeRoot) throws Exception {
      String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
      server = new ZkTestServer(zkDir);
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      if (makeRoot) AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    }

    public ZkTestServer getServer () {
      return server;
    }

    public SolrZkClient getClient () {
      return zkClient;
    }

    @Override
    public void close() throws Exception {
      if (zkClient != null) zkClient.close();
      if (server != null) server.shutdown();
    }
  }

  public void testConnect() throws Exception {
    try (ZkConnection conn = new ZkConnection (false)) {
      // do nothing
    }
  }

  public void testMakeRootNode() throws Exception {
    try (ZkConnection conn = new ZkConnection ()) {
      final SolrZkClient zkClient = new SolrZkClient(conn.getServer().getZkHost(), AbstractZkTestCase.TIMEOUT);
      try {
        assertTrue(zkClient.exists("/solr", true));
      } finally {
        zkClient.close();
      }
    }
  }

  public void testClean() throws Exception {
    try (ZkConnection conn = new ZkConnection ()) {
      final SolrZkClient zkClient = conn.getClient();

      zkClient.makePath("/test/path/here", true);

      zkClient.makePath("/zz/path/here", true);

      zkClient.clean("/");

      assertFalse(zkClient.exists("/test", true));
      assertFalse(zkClient.exists("/zz", true));
    }
  }

  public void testReconnect() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    ZkTestServer server = null;
    SolrZkClient zkClient = null;
    try {
      server = new ZkTestServer(zkDir);
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT);
      String shardsPath = "/collections/collection1/shards";
      zkClient.makePath(shardsPath, false, true);

      zkClient.makePath("collections/collection1", false, true);
      int zkServerPort = server.getPort();
      // this tests disconnect state
      server.shutdown();

      Thread.sleep(80);


      try {
        zkClient.makePath("collections/collection2", false);
        Assert.fail("Server should be down here");
      } catch (KeeperException.ConnectionLossException e) {

      }

      // bring server back up
      server = new ZkTestServer(zkDir, zkServerPort);
      server.run();

      // TODO: can we do better?
      // wait for reconnect
      Thread.sleep(600);

      try {
        zkClient.makePath("collections/collection3", true);
      } catch (KeeperException.ConnectionLossException e) {
        Thread.sleep(5000); // try again in a bit
        zkClient.makePath("collections/collection3", true);
      }

      assertNotNull(zkClient.exists("/collections/collection3", null, true));
      assertNotNull(zkClient.exists("/collections/collection1", null, true));
      
      // simulate session expiration
      
      // one option
      long sessionId = zkClient.getSolrZooKeeper().getSessionId();
      server.expire(sessionId);
      
      // another option
      //zkClient.getSolrZooKeeper().getConnection().disconnect();

      // this tests expired state

      Thread.sleep(1000); // pause for reconnect
      
      for (int i = 0; i < 8; i++) {
        try {
          zkClient.makePath("collections/collection4", true);
          break;
        } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {

        }
        Thread.sleep(1000 * i);
      }

      assertNotNull("Node does not exist, but it should", zkClient.exists("/collections/collection4", null, true));

    } finally {

      if (zkClient != null) {
        zkClient.close();
      }
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  public void testZkCmdExectutor() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    ZkTestServer server = null;

    try {
      server = new ZkTestServer(zkDir);
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      final int timeout = random().nextInt(10000) + 5000;
      
      ZkCmdExecutor zkCmdExecutor = new ZkCmdExecutor(timeout);
      final long start = System.nanoTime();
      try {
      zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public String execute() throws KeeperException, InterruptedException {
          if (System.nanoTime() - start > TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS)) {
            throw new KeeperException.SessionExpiredException();
          } 
          throw new KeeperException.ConnectionLossException();
        }
      });
      } catch(KeeperException.SessionExpiredException e) {
        
      } catch (Exception e) {
        fail("Expected " + KeeperException.SessionExpiredException.class.getSimpleName() + " but got " + e.getClass().getSimpleName());
      }
    } finally {
      if (server != null) {
        server.shutdown();
      }
    }
  }

  public void testMultipleWatchesAsync() throws Exception {
    try (ZkConnection conn = new ZkConnection ()) {
      final SolrZkClient zkClient = conn.getClient();
      zkClient.makePath("/collections", true);

      final int numColls = random().nextInt(100);
      final CountDownLatch latch = new CountDownLatch(numColls);

      for (int i = 1; i <= numColls; i ++) {
        String collPath = "/collections/collection" + i;
        zkClient.makePath(collPath, true);
        zkClient.getChildren(collPath, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            latch.countDown();
            try {
              Thread.sleep(1000);
            }
            catch (InterruptedException e) {}
          }
        }, true);
      }

      for (int i = 1; i <= numColls; i ++) {
        String shardsPath = "/collections/collection" + i + "/shards";
        zkClient.makePath(shardsPath, true);
      }

      assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
    }
  }

  public void testWatchChildren() throws Exception {
    try (ZkConnection conn = new ZkConnection ()) {
      final SolrZkClient zkClient = conn.getClient();
      final AtomicInteger cnt = new AtomicInteger();
      final CountDownLatch latch = new CountDownLatch(1);

      zkClient.makePath("/collections", true);

      zkClient.getChildren("/collections", new Watcher() {

        @Override
        public void process(WatchedEvent event) {
          cnt.incrementAndGet();
          // remake watch
          try {
            zkClient.getChildren("/collections", this, true);
            latch.countDown();
          } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }, true);

      zkClient.makePath("/collections/collection99/shards", true);
      latch.await(); //wait until watch has been re-created

      zkClient.makePath("collections/collection99/config=collection1", true);

      zkClient.makePath("collections/collection99/config=collection3", true);
      
      zkClient.makePath("/collections/collection97/shards", true);
      
      // pause for the watches to fire
      Thread.sleep(700);
      
      if (cnt.intValue() < 2) {
        Thread.sleep(4000); // wait a bit more
      }
      
      if (cnt.intValue() < 2) {
        Thread.sleep(4000); // wait a bit more
      }
      
      assertEquals(2, cnt.intValue());

    }
  }
  
  public void testSkipPathPartsOnMakePath() throws Exception {
    try (ZkConnection conn = new ZkConnection()) {
      final SolrZkClient zkClient = conn.getClient();

      zkClient.makePath("/test", true);

      // should work
      zkClient.makePath("/test/path/here", (byte[]) null, CreateMode.PERSISTENT, (Watcher) null, true, true, 1);

      zkClient.clean("/");

      // should not work
      try {
        zkClient.makePath("/test/path/here", (byte[]) null, CreateMode.PERSISTENT, (Watcher) null, true, true, 1);
        fail("We should not be able to create this path");
      } catch (Exception e) {

      }

      zkClient.clean("/");

      ZkCmdExecutor zkCmdExecutor = new ZkCmdExecutor(30000);
      try {
        zkCmdExecutor.ensureExists("/collection/collection/leader", (byte[]) null, CreateMode.PERSISTENT, zkClient, 2);
        fail("We should not be able to create this path");
      } catch (Exception e) {

      }

      zkClient.makePath("/collection", true);

      try {
        zkCmdExecutor.ensureExists("/collections/collection/leader", (byte[]) null, CreateMode.PERSISTENT, zkClient, 2);
        fail("We should not be able to create this path");
      } catch (Exception e) {

      }
      zkClient.makePath("/collection/collection", true);
 
      byte[] bytes = new byte[10];
      zkCmdExecutor.ensureExists("/collection/collection", bytes, CreateMode.PERSISTENT, zkClient, 2);
      
      byte[] returnedBytes = zkClient.getData("/collection/collection", null, null, true);
      
      assertNull("We skipped 2 path parts, so data won't be written", returnedBytes);

      zkClient.makePath("/collection/collection/leader", true);

      zkCmdExecutor.ensureExists("/collection/collection/leader", (byte[]) null, CreateMode.PERSISTENT, zkClient, 2);

    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
  }
}
