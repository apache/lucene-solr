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

package org.apache.solr.common.cloud;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDataWatches extends SolrTestCaseJ4 {

  private static final int TIMEOUT = 30;

  private static ZkTestServer zkServer;
  private static SolrZkClient client;

  @BeforeClass
  public static void setupZkServer() throws Exception {
    zkServer = new ZkTestServer(createTempDir().toString());
    zkServer.run();

    // We have multiple sessions, so limit violations will be inaccurate
    zkServer.setViolationReportAction(ZkTestServer.LimitViolationAction.IGNORE);

    client = new SolrZkClient(zkServer.getZkAddress(), TIMEOUT);
    client.create("/", null, CreateMode.PERSISTENT, true);

  }

  @AfterClass
  public static void tearDownZkServer() throws IOException, InterruptedException {
    client.close();
    zkServer.shutdown();
  }

  private class SyncDataWatch implements DataWatch {

    int version;
    byte[] data;
    CountDownLatch latch = new CountDownLatch(1);

    @Override
    public boolean onChanged(int version, byte[] data) {
      this.version = version;
      this.data = data;
      this.latch.countDown();
      return true;  // always retain watch
    }
  }

  private static byte[] bytes(String data) {
    return data.getBytes(Charset.defaultCharset());
  }

  @Test
  public void testSimpleDataWatch() throws Exception {

    SyncDataWatch watch = new SyncDataWatch();

    // watch for non-existent node
    client.addDataWatch("/test", watch);
    watch.latch.await(TIMEOUT, TimeUnit.SECONDS);
    assertEquals(-1, watch.version);
    assertEquals(null, watch.data);

    // watch should be called when data is added
    watch.latch = new CountDownLatch(1);
    client.create("/test", bytes("some data"), CreateMode.PERSISTENT, true);
    watch.latch.await(TIMEOUT, TimeUnit.SECONDS);
    assertEquals(0, watch.version);
    assertArrayEquals(bytes("some data"), watch.data);
    assertEquals(0, client.getWatchedVersions().get("/test").intValue());

    // and when data is updated
    watch.latch = new CountDownLatch(1);
    client.setData("/test", bytes("some more data"), true);
    watch.latch.await(TIMEOUT, TimeUnit.SECONDS);
    assertEquals(1, watch.version);
    assertArrayEquals(bytes("some more data"), watch.data);
    assertEquals(1, client.getWatchedVersions().get("/test").intValue());

    // and when data is deleted
    watch.latch = new CountDownLatch(1);
    client.delete("/test", 1, true);
    watch.latch.await(TIMEOUT, TimeUnit.SECONDS);
    assertEquals(-1, watch.version);
    assertEquals(null, watch.data);
    assertEquals(-1, client.getWatchedVersions().get("/test").intValue());

    // and when a forceUpdate is called
    watch.latch = new CountDownLatch(1);
    client.forceUpdateWatch("/test");
    watch.latch.await(TIMEOUT, TimeUnit.SECONDS);

  }

  private class SyncChildWatch implements ChildWatch {

    List<String> children = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);

    @Override
    public boolean onChanged(List<String> children) {
      this.children.clear();
      this.children.addAll(children);
      latch.countDown();
      return true;
    }
  }

  @Test
  public void testSimpleChildWatch() throws Exception {

    SyncChildWatch watcher = new SyncChildWatch();

    // watch for non-existent node
    client.addChildWatch("/parent", watcher);
    watcher.latch.await(TIMEOUT, TimeUnit.SECONDS);
    assertTrue(watcher.children.isEmpty());

    watcher.latch = new CountDownLatch(1);
    client.create("/parent", bytes("test"), CreateMode.PERSISTENT, true);
    watcher.latch.await(TIMEOUT, TimeUnit.SECONDS);

    // now add a child
    watcher.latch = new CountDownLatch(1);
    client.makePath("/parent/child1", bytes(""), true);
    watcher.latch.await(TIMEOUT, TimeUnit.SECONDS);
    assertEquals(watcher.children.size(), 1);

    // add another
    watcher.latch = new CountDownLatch(1);
    client.makePath("/parent/child2", bytes(""), true);
    watcher.latch.await(TIMEOUT, TimeUnit.SECONDS);
    assertEquals(watcher.children.size(), 2);

    // delete one
    watcher.latch = new CountDownLatch(1);
    client.delete("/parent/child1", 0, true);
    watcher.latch.await(TIMEOUT, TimeUnit.SECONDS);
    assertEquals(watcher.children.size(), 1);

    // force an update
    watcher.latch = new CountDownLatch(1);
    client.forceUpdateChildren("/parent");
    watcher.latch.await(TIMEOUT, TimeUnit.SECONDS);

  }

  // watches are re-set after session expiry
  public void testSessionExpiry() throws Exception {

    client.makePath("/expirytest", bytes("test"), true);
    client.makePath("/expirytest/child1", bytes("test"), true);
    client.makePath("/expirytest/child2", bytes("test"), true);

    SyncDataWatch watch = new SyncDataWatch();
    client.addDataWatch("/expirytest", watch);
    watch.latch.await(TIMEOUT, TimeUnit.SECONDS);
    watch.latch = new CountDownLatch(1);

    CountDownLatch childWatchLatch = new CountDownLatch(2);

    client.addChildWatch("/expirytest", children -> {
      childWatchLatch.countDown();
      return true;
    });

    // after expiry, both data watch and child watch should be called again

    zkServer.expire(client.getSolrZooKeeper().getSessionId());

    watch.latch.await(TIMEOUT, TimeUnit.SECONDS);
    childWatchLatch.await(TIMEOUT, TimeUnit.SECONDS);

  }

  // multiple watches share a Watcher
  public void testSingleWatcherCreated() throws Exception {

    zkServer.getLimiter().setAction(ZkTestServer.LimitViolationAction.FAIL);
    try {
      SyncDataWatch watch1 = new SyncDataWatch();
      CountDownLatch latch = new CountDownLatch(1);
      DataWatch watch2 = (version, data) -> {
        latch.countDown();
        return false;
      };

      client.addDataWatch("/multiplewatchers", watch1);
      client.addDataWatch("/multiplewatchers", watch2);

      client.create("/multiplewatchers", bytes("test"), CreateMode.PERSISTENT, true);

      // Both watchers should be notified, but the limiter should ensure
      // that only a single ZK watcher was created
      watch1.latch.await(TIMEOUT, TimeUnit.SECONDS);
      latch.await(TIMEOUT, TimeUnit.SECONDS);

    }
    finally {
      zkServer.getLimiter().setAction(ZkTestServer.LimitViolationAction.IGNORE);
    }

  }

  // watches get removed
  public void testWatchesAreRemoved() throws Exception {
    CountDownLatch latch = new CountDownLatch(6);
    AtomicInteger count1 = new AtomicInteger(0);
    AtomicInteger count2 = new AtomicInteger(0);
    DataWatch preservedWatch = (version, data) -> {
      count1.incrementAndGet();
      latch.countDown();
      return true;
    };
    DataWatch singleFireWatch = (version, data) -> {
      count2.incrementAndGet();
      latch.countDown();
      return version == -1;
    };

    // watches are fired immediately with the current values
    client.addDataWatch("/removals", preservedWatch);
    client.addDataWatch("/removals", singleFireWatch);

    // first data watch will fire both watches, but remove the second
    client.create("/removals", bytes("data"), CreateMode.PERSISTENT, true);
    // second should only fire one watch
    client.setData("/removals", bytes("update"), true);
    // third will again only fire one watch, and ensures that all notifications
    // from the second change have completed
    client.setData("/removals", bytes("and another update"), true);

    latch.await(TIMEOUT, TimeUnit.SECONDS);
    assertEquals(4, count1.intValue());
    assertEquals(2, count2.intValue());
  }

}
