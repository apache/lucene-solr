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

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;

public class ZkSolrClientTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  static class ZkConnection implements AutoCloseable {

    private ZkTestServer server = null;
    private SolrZkClient zkClient = null;

    ZkConnection() throws Exception {
      this (true);
    }

    ZkConnection(boolean makeRoot) throws Exception {
      Path zkDir = createTempDir("zkData");
      server = new ZkTestServer(zkDir);
      server.run(true);

      zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT);
      zkClient.start();
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
      zkClient.start();
      try {
        assertTrue(zkClient.exists("/solr"));
      } finally {
        zkClient.close();
      }
    }
  }

  public void testClean() throws Exception {
    try (ZkConnection conn = new ZkConnection ()) {
      final SolrZkClient zkClient = conn.getClient();

      zkClient.mkdirs("/test", "/test/path", "/test/path/here");

      zkClient.mkdirs("/zz", "/zz/path", "/zz/path/here");

      zkClient.clean("/");

      assertFalse(zkClient.exists("/test"));
      assertFalse(zkClient.exists("/zz"));
    }
  }

  @Nightly
  public void testReconnect() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = null;
    server = new ZkTestServer(zkDir);
    server.run();
    try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT).start()) {

      String shardsPath = "/collections/collection1/shards";
      zkClient.makePath(shardsPath, false, true);

      int zkServerPort = server.getPort();
      // this tests disconnect state
      server.shutdown();

      Thread thread = new Thread() {
        public void run() {
          try {
            zkClient.mkdir("collections/collection2");
           // Assert.fail("Server should be down here");
          } catch (KeeperException e) {

          }
        }
      };

      thread.start();

      // bring server back up
      server = new ZkTestServer(zkDir, zkServerPort);
      server.run(false);

      Thread thread2 = new Thread() {
        public void run() {
          try {
            zkClient.mkdir("collections/collection3");
          } catch (KeeperException e) {
            throw new RuntimeException(e);
          }
        }
      };

      thread2.start();

      thread.join();
      
      thread2.join();

      assertNotNull(zkClient.exists("/collections/collection3", null));
      assertNotNull(zkClient.exists("/collections/collection1", null));
      
      // simulate session expiration
      
      // one option
      long sessionId = zkClient.getSolrZooKeeper().getSessionId();
      server.expire(sessionId);
      
      // another option
      //zkClient.getSolrZooKeeper().getConnection().disconnect();

      // this tests expired state

      Thread.sleep(TEST_NIGHTLY ? 1000 : 10); // pause for reconnect
      
      for (int i = 0; i < 4; i++) {
        try {
          zkClient.mkdir("collections/collection4");
          break;
        } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
          ParWork.propagateInterrupt(e);
        }
        Thread.sleep(50 * i);
      }

      assertNotNull("Node does not exist, but it should", zkClient.exists("/collections/collection4", null));

    } finally {

      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  public void testZkCmdExectutor() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = null;

    try {
      server = new ZkTestServer(zkDir);
      server.run();
      final int timeout;
      if (!TEST_NIGHTLY) {
        timeout = 50;
      } else {
        timeout = random().nextInt(1000) + 500;
      }
      
      ZkCmdExecutor zkCmdExecutor = new ZkCmdExecutor(server.getZkClient(), 3000);
      final long start = System.nanoTime();
      expectThrows(KeeperException.SessionExpiredException.class, () -> {
        zkCmdExecutor.retryOperation(() -> {
          if (System.nanoTime() - start > TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS)) {
            throw new KeeperException.SessionExpiredException();
          }
          throw new KeeperException.ConnectionLossException();
        });
      });
    } finally {
      if (server != null) {
        server.shutdown();
      }
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
}
