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
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
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
  public void testDistributedQueueBlocking() throws Exception {
    String dqZNode = "/distqueue/test";
    String testData = "hello world";

    DistributedQueue dq = makeDistributedQueue(dqZNode);

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
    assertTrue(dq.hasWatcher());

    forceSessionExpire();

    // Session expiry should have fired the watcher.
    Thread.sleep(100);
    assertFalse(dq.hasWatcher());

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
  public void testPeekElements() throws Exception {
    String dqZNode = "/distqueue/test";
    byte[] data = "hello world".getBytes(UTF8);

    DistributedQueue dq = makeDistributedQueue(dqZNode);

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
    assertTrue(System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(1000));
    assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(250));
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

  protected DistributedQueue makeDistributedQueue(String dqZNode) throws Exception {
    return new DistributedQueue(zkClient, setupNewDistributedQueueZNode(dqZNode));
  }

  private class QueueChangerThread extends Thread {

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
    zkServer = new ZkTestServer(createTempDir("zkData").toFile().getAbsolutePath());
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    assertTrue(zkClient.isConnected());
  }

  protected void closeZk() throws Exception {
    if (zkClient != null)
      zkClient.close();
    zkServer.shutdown();
  }
}
