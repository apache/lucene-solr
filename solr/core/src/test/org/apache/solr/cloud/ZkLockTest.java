package org.apache.solr.cloud;

/**
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


import java.io.File;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.lock.LockListener;
import org.apache.solr.cloud.lock.WriteLock;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.SolrConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkLockTest extends SolrTestCaseJ4 {
  
  static final int TIMEOUT = 10000;
  private ZkTestServer server;
  private SolrZkClient zkClient;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    createTempDir();
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    
    server = new ZkTestServer(zkDir);
    server.run();
    AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
    AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
    zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
  }
  
  @Test
  public void testLock() throws Exception {
    zkClient.makePath("/collections/collection1/shards_lock");
    zkClient.makePath("/collections/collection1/shards_lock2");
    zkClient.makePath("/collections/collection1/shards_lock3");
    zkClient.getData("/collections/collection1/shards_lock", null, null);
    
    WriteLock lock = new WriteLock(zkClient.getSolrZooKeeper(),
        "/collections/collection1/shards_lock", null, new LockListener() {
          
          @Override
          public void lockReleased() {
            
          }
          
          @Override
          public void lockAcquired() {
         // TODO this is only a dumb observation test now
            if (VERBOSE) System.out.println("I got the lock!");
            
          }
        });
    
    lock.lock();
   
    if (VERBOSE) printLayout(server.getZkHost());
  }
  
  @Override
  public void tearDown() throws Exception {
    zkClient.close();
    server.shutdown();
    SolrConfig.severeErrors.clear();
    super.tearDown();
  }
  
  private void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
  }
  
}
