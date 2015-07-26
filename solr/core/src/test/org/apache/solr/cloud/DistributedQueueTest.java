package org.apache.solr.cloud;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.File;
import java.nio.charset.Charset;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.cloud.DistributedQueue.QueueEvent;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DistributedQueueTest extends SolrTestCaseJ4 {

  private static final Charset UTF8 = Charset.forName("UTF-8");

  protected ZkTestServer zkServer;
  protected SolrZkClient zkClient;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    setupZk();
  }

  @Test
  public void testDistributedQueue() throws Exception {
    String dqZNode = "/distqueue/test";
    String testData = "hello world";
    long timeoutMs = 500L;

    DistributedQueue dq = new DistributedQueue(zkClient, setupDistributedQueueZNode(dqZNode));

    // basic ops
    assertTrue(dq.poll() == null);
    byte[] data = testData.getBytes(UTF8);
    dq.offer(data);
    assertEquals(new String(dq.peek(),UTF8), testData);
    assertEquals(new String(dq.take(),UTF8), testData);
    assertTrue(dq.poll() == null);
    QueueEvent qe = dq.offer(data, timeoutMs);
    assertNotNull(qe);
    assertEquals(new String(dq.remove(),UTF8), testData);

    // should block until the background thread makes the offer
    (new QueueChangerThread(dq, 1000)).start();
    qe = dq.peek(true);
    assertNotNull(qe);
    dq.remove();

    // timeout scenario ... background thread won't offer until long after the peek times out
    QueueChangerThread qct = new QueueChangerThread(dq, 1000);
    qct.start();
    qe = dq.peek(500);
    assertTrue(qe == null);

    try {
      qct.interrupt();
    } catch (Exception exc) {}
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

  protected String setupDistributedQueueZNode(String znodePath) throws Exception {
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
    } catch (Exception exc) {}
    closeZk();
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
