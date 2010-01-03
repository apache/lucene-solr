package org.apache.solr.cloud;

/**
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
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZkSolrClientTest extends TestCase {
  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());
  
  public void testBasic() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    ZkTestServer server = null;
    SolrZkClient zkClient = null;
    try {
      server = new ZkTestServer(zkDir);
      server.run();

      AbstractZkTestCase.makeSolrZkNode();

      zkClient = new SolrZkClient(AbstractZkTestCase.ZOO_KEEPER_ADDRESS,
          AbstractZkTestCase.TIMEOUT, new ZkClientConnectionStrategy() {
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            @Override
            public void reconnect(final String serverAddress, final int zkClientTimeout,
                final Watcher watcher, final ZkUpdate updater) throws IOException {
              System.out.println("reconnecting");
              executor.scheduleAtFixedRate(new Runnable() {
                public void run() {
                  // nocommit
                  System.out.println("Attempting the connect...");
                  try {
                    updater.update(new ZooKeeper(serverAddress, zkClientTimeout, watcher));
                    // nocommit
                    System.out.println("Connect done");
                  } catch (Exception e) {
                    // nocommit
                    e.printStackTrace();
                    System.out.println("failed reconnect");
                  }
                  executor.shutdownNow();
                  
                }
              }, 0, 400, TimeUnit.MILLISECONDS);
              
            }
            
            @Override
            public void connect(String zkServerAddress, int zkClientTimeout,
                Watcher watcher, ZkUpdate updater) throws IOException, InterruptedException, TimeoutException {
              System.out.println("connecting");
              updater.update(new ZooKeeper(zkServerAddress, zkClientTimeout, watcher));
              
            }
          });
      String shardsPath = "/collections/collection1/shards";
      zkClient.makePath(shardsPath);

      zkClient.makePath("collections/collection1/config=collection1");
      
      server.shutdown();
      
      Thread.sleep(80);
      
      boolean exceptionHappened = false;
      try {
        zkClient.makePath("collections/collection1/config=collection2");
      } catch (KeeperException.ConnectionLossException e) {
        // nocommit : the connection should be down
        exceptionHappened = true;
      }
      
      assertTrue("Server should be down here", exceptionHappened);
      
      server = new ZkTestServer(zkDir);
      server.run();
      
      Thread.sleep(80);
      zkClient.makePath("collections/collection1/config=collection3");
      
      zkClient.printLayoutToStdOut();
      
      assertNotNull(zkClient.exists("/collections/collection1/config=collection3", null));
      assertNotNull(zkClient.exists("/collections/collection1/config=collection1", null));

    } catch(Exception e) {
      // nocommit
      e.printStackTrace();
    } finally {
    
      if (zkClient != null) {
        zkClient.close();
      }
      if (server != null) {
        server.shutdown();
      }
    }
  }
}
