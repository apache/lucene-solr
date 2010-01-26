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

import junit.framework.TestCase;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ZkSolrClientTest extends TestCase {
  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());
  
  public void testConnect() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
    + "zookeeper/server1/data";
    ZkTestServer server = null;

    server = new ZkTestServer(zkDir);
    server.run();

    SolrZkClient zkClient = new SolrZkClient(AbstractZkTestCase.ZOO_KEEPER_ADDRESS,
        AbstractZkTestCase.TIMEOUT);
    
    zkClient.close();
    server.shutdown();
  }
  
  public void testMakeRootNode() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
    + "zookeeper/server1/data";
    ZkTestServer server = null;

    server = new ZkTestServer(zkDir);
    server.run();

    AbstractZkTestCase.makeSolrZkNode();
    
    SolrZkClient zkClient = new SolrZkClient(AbstractZkTestCase.ZOO_KEEPER_SERVER,
        AbstractZkTestCase.TIMEOUT);
    
    assertTrue(zkClient.exists("/solr"));
    
    zkClient.close();
    server.shutdown();
  }
  
  // nocommit : must be a clear way to do this
  public void testReconnect() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    ZkTestServer server = null;
    SolrZkClient zkClient = null;
    try {
      server = new ZkTestServer(zkDir);
      server.run();

      AbstractZkTestCase.makeSolrZkNode();

      zkClient = new SolrZkClient(AbstractZkTestCase.ZOO_KEEPER_ADDRESS, 5);
      String shardsPath = "/collections/collection1/shards";
      zkClient.makePath(shardsPath);

      zkClient.makePath("collections/collection1/config=collection1");
      
      // this tests disconnect state
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
      
      // wait for reconnect
      Thread.sleep(1000);
      
      zkClient.makePath("collections/collection1/config=collection3");
      
      zkClient.printLayoutToStdOut();
      
      assertNotNull(zkClient.exists("/collections/collection1/config=collection3", null));
      assertNotNull(zkClient.exists("/collections/collection1/config=collection1", null));
      
      //this tests expired state
      
      // cause expiration
      for(int i = 0; i < 1000; i++) {
        System.gc();
      }
      
      Thread.sleep(3000); // pause for reconnect
      
      zkClient.makePath("collections/collection1/config=collection4");
      
      zkClient.printLayoutToStdOut();
      
      assertNotNull(zkClient.exists("/collections/collection1/config=collection4", null));

    } catch(Exception e) {
      // nocommit
      e.printStackTrace();
      throw e;
    } finally {
    
      if (zkClient != null) {
        zkClient.close();
      }
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  public void testWatchChildren() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    ZkTestServer server = null;
    SolrZkClient zkClient = null;
    try {
      server = new ZkTestServer(zkDir);
      server.run();

      AbstractZkTestCase.makeSolrZkNode();

      zkClient = new SolrZkClient(AbstractZkTestCase.ZOO_KEEPER_ADDRESS, 5);

      zkClient.makePath("/collections");
      
      zkClient.getChildren("/collections", new Watcher(){

        public void process(WatchedEvent event) {
          System.out.println("children changed");
          
        }});
      
      zkClient.makePath("/collections/collection1/shards");

      zkClient.makePath("collections/collection1/config=collection1");


      zkClient.makePath("collections/collection1/config=collection3");


      zkClient.printLayoutToStdOut();


    } catch (Exception e) {
      // nocommit
      e.printStackTrace();
      throw e;
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
