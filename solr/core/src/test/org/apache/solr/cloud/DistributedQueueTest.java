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

import java.nio.charset.Charset;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DistributedQueueTest extends SolrTestCaseJ4 {

  private static final Charset UTF8 = Charset.forName("UTF-8");

  protected ZkTestServer zkServer;
  protected SolrZkClient zkClient;
  protected ExecutorService executor = ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrjNamedThreadFactory("dqtest-"));

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    setupZk();
  }

  @Test
  public void testDistributedQueue() throws Exception {
    String dqZNode = "/distqueue/test";
    byte[] data = "hello world".getBytes(UTF8);

    DistributedQueue dq = makeDistributedQueue(dqZNode);

    // basic ops
    assertNull(dq.poll());
    try {
      dq.remove();
      fail("NoSuchElementException expected");
    } catch (NoSuchElementException expected) {
      // expected
    }

    dq.offer(data);
    assertArrayEquals(dq.peek(500), data);
    assertArrayEquals(dq.remove(), data);
    assertNull(dq.poll());

    dq.offer(data);
    assertArrayEquals(dq.take(), data); // waits for data
    assertNull(dq.poll());

    dq.offer(data);
    dq.peek(true); // wait until data is definitely there before calling remove
    assertArrayEquals(dq.remove(), data);
    assertNull(dq.poll());

    // should block until the background thread makes the offer
    (new QueueChangerThread(dq, 1000)).start();
    assertNotNull(dq.peek(true));
    assertNotNull(dq.remove());
    assertNull(dq.poll());

    // timeout scenario ... background thread won't offer until long after the peek times out
    QueueChangerThread qct = new QueueChangerThread(dq, 1000);
    qct.start();
    assertNull(dq.peek(500));
    qct.join();
  }

  @Test
  public void testDistributedQueueCache() throws Exception {
    String dqZNode = "/distqueue/test";
    byte[] data = "hello world".getBytes(UTF8);

    ZkDistributedQueue consumer = makeDistributedQueue(dqZNode);
    DistributedQueue producer = makeDistributedQueue(dqZNode);
    DistributedQueue producer2 = makeDistributedQueue(dqZNode);

    producer2.offer(data);
    producer.offer(data);
    producer.offer(data);
    consumer.poll();

    assertEquals(2, consumer.getZkStats().getQueueLength());
    producer.offer(data);
    producer2.offer(data);
    consumer.poll();
    // Wait for watcher being kicked off
    while (!consumer.isDirty()) {
      Thread.sleep(20);
    }
    // DQ still have elements in their queue, so we should not fetch elements path from Zk
    assertEquals(1, consumer.getZkStats().getQueueLength());
    consumer.poll();
    consumer.peek();
    assertEquals(2, consumer.getZkStats().getQueueLength());
  }

  @Test
  public void testDistributedQueueBlocking() throws Exception {
    String dqZNode = "/distqueue/test";
    String testData = "hello world";

    ZkDistributedQueue dq = makeDistributedQueue(dqZNode);

    assertNull(dq.peek());
    Future<String> future = executor.submit(() -> new String(dq.peek(true), UTF8));
    try {
      future.get(1000, TimeUnit.MILLISECONDS);
      fail("TimeoutException expected");
    } catch (TimeoutException expected) {
      assertFalse(future.isDone());
    }

    // Ultimately trips the watcher, triggering child refresh
    dq.offer(testData.getBytes(UTF8));
    assertEquals(testData, future.get(1000, TimeUnit.MILLISECONDS));
    assertNotNull(dq.poll());

    // After draining the queue, a watcher should be set.
    assertNull(dq.peek(100));
    
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeout.waitFor("Timeout waiting to see dirty=false", () -> {
      try {
        return !dq.isDirty();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    
    assertFalse(dq.isDirty());
    assertEquals(1, dq.watcherCount());

    forceSessionExpire();

    // Session expiry should have fired the watcher.
    Thread.sleep(100);
    assertTrue(dq.isDirty());
    assertEquals(0, dq.watcherCount());

    // Rerun the earlier test make sure updates are still seen, post reconnection.
    future = executor.submit(() -> new String(dq.peek(true), UTF8));
    try {
      future.get(1000, TimeUnit.MILLISECONDS);
      fail("TimeoutException expected");
    } catch (TimeoutException expected) {
      assertFalse(future.isDone());
    }

    // Ultimately trips the watcher, triggering child refresh
    dq.offer(testData.getBytes(UTF8));
    assertEquals(testData, future.get(1000, TimeUnit.MILLISECONDS));
    assertNotNull(dq.poll());
    assertNull(dq.poll());
  }

  @Test
  public void testLeakChildWatcher() throws Exception {
    String dqZNode = "/distqueue/test";
    ZkDistributedQueue dq = makeDistributedQueue(dqZNode);
    assertTrue(dq.peekElements(1, 1, s1 -> true).isEmpty());
    assertEquals(1, dq.watcherCount());
    assertFalse(dq.isDirty());
    assertTrue(dq.peekElements(1, 1, s1 -> true).isEmpty());
    assertEquals(1, dq.watcherCount());
    assertFalse(dq.isDirty());
    assertNull(dq.peek());
    assertEquals(1, dq.watcherCount());
    assertFalse(dq.isDirty());
    assertNull(dq.peek(10));
    assertEquals(1, dq.watcherCount());
    assertFalse(dq.isDirty());

    dq.offer("hello world".getBytes(UTF8));
    assertNotNull(dq.peek()); // synchronously available
    // dirty and watcher state indeterminate here, race with watcher
    Thread.sleep(100); // watcher should have fired now
    assertNotNull(dq.peek());
    // in case of race condition, childWatcher is kicked off after peek()
    if (dq.watcherCount() == 0) {
      assertTrue(dq.isDirty());
      dq.poll();
      dq.offer("hello world".getBytes(UTF8));
      dq.peek();
    }
    assertEquals(1, dq.watcherCount());
    assertFalse(dq.isDirty());
    assertFalse(dq.peekElements(1, 1, s -> true).isEmpty());
    assertEquals(1, dq.watcherCount());
    assertFalse(dq.isDirty());
  }

  @Test
  public void testLocallyOffer() throws Exception {
    String dqZNode = "/distqueue/test";
    ZkDistributedQueue dq = makeDistributedQueue(dqZNode);
    dq.peekElements(1, 1, s -> true);
    for (int i = 0; i < 100; i++) {
      byte[] data = String.valueOf(i).getBytes(UTF8);
      dq.offer(data);
      assertNotNull(dq.peek());
      dq.poll();
      dq.peekElements(1, 1, s -> true);
    }
  }


  @Test
  public void testPeekElements() throws Exception {
    String dqZNode = "/distqueue/test";
    byte[] data = "hello world".getBytes(UTF8);

    ZkDistributedQueue dq = makeDistributedQueue(dqZNode);

    // Populate with data.
    dq.offer(data);
    dq.offer(data);
    dq.offer(data);

    Predicate<String> alwaysTrue = s -> true;
    Predicate<String> alwaysFalse = s -> false;

    // Should be able to get 0, 1, 2, or 3 instantly
    for (int i = 0; i <= 3; ++i) {
      assertEquals(i, dq.peekElements(i, 0, alwaysTrue).size());
    }

    // Asking for more should return only 3.
    assertEquals(3, dq.peekElements(4, 0, alwaysTrue).size());

    // If we filter everything out, we should block for the full time.
    long start = System.nanoTime();
    assertEquals(0, dq.peekElements(4, 1000, alwaysFalse).size());
    assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(500));

    // If someone adds a new matching element while we're waiting, we should return immediately.
    executor.submit(() -> {
      try {
        Thread.sleep(500);
        dq.offer(data);
      } catch (Exception e) {
        // ignore
      }
    });
    start = System.nanoTime();
    assertEquals(1, dq.peekElements(4, 2000, child -> {
      // The 4th element in the queue will end with a "3".
      return child.endsWith("3");
    }).size());
    long timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue("Time was " + timeTaken + "ms, expected 250-1500ms", timeTaken > 250 && timeTaken < 1500);
  }

  private void forceSessionExpire() throws InterruptedException, TimeoutException {
    long sessionId = zkClient.getSolrZooKeeper().getSessionId();
    zkServer.expire(sessionId);
    zkClient.getConnectionManager().waitForDisconnected(10000);
    zkClient.getConnectionManager().waitForConnected(10000);
    for (int i = 0; i < 100; ++i) {
      if (zkClient.isConnected()) {
        break;
      }
      Thread.sleep(50);
    }
    assertTrue(zkClient.isConnected());
    assertFalse(sessionId == zkClient.getSolrZooKeeper().getSessionId());
  }

  protected ZkDistributedQueue makeDistributedQueue(String dqZNode) throws Exception {
    return new ZkDistributedQueue(zkClient, setupNewDistributedQueueZNode(dqZNode));
  }

  private static class QueueChangerThread extends Thread {

    DistributedQueue dq;
    long waitBeforeOfferMs;

    QueueChangerThread(DistributedQueue dq, long waitBeforeOfferMs) {
      this.dq = dq;
      this.waitBeforeOfferMs = waitBeforeOfferMs;
    }

    public void run() {
      try {
        Thread.sleep(waitBeforeOfferMs);
        dq.offer(getName().getBytes(UTF8));
      } catch (InterruptedException ie) {
        // do nothing
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    }
  }

  protected String setupNewDistributedQueueZNode(String znodePath) throws Exception {
    if (!zkClient.exists("/", true))
      zkClient.makePath("/", false, true);
    if (zkClient.exists(znodePath, true))
      zkClient.clean(znodePath);
    zkClient.makePath(znodePath, false, true);
    return znodePath;
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      super.tearDown();
    } catch (Exception exc) {
    }
    closeZk();
    executor.shutdown();
  }

  protected void setupZk() throws Exception {
    System.setProperty("zkClientTimeout", "8000");
    zkServer = new ZkTestServer(createTempDir("zkData"));
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    assertTrue(zkClient.isConnected());
  }

  protected void closeZk() throws Exception {
    if (null != zkClient) {
      zkClient.close();
      zkClient = null;
    }
    if (null != zkServer) {
      zkServer.shutdown();
      zkServer = null;
    }
  }
}
