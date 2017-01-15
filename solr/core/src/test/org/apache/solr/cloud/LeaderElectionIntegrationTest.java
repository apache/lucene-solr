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

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

@Slow
public class LeaderElectionIntegrationTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final static int NUM_SHARD_REPLICAS = 5;

  private static final Pattern HOST = Pattern
      .compile(".*?\\:(\\d\\d\\d\\d)_.*");
  
  protected ZkTestServer zkServer;
  
  protected String zkDir;
  
  private Map<Integer,CoreContainer> containerMap = new HashMap<>();
  
  private Map<String,Set<Integer>> shardPorts = new HashMap<>();
  
  private SolrZkClient zkClient;

  private ZkStateReader reader;
  
  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();

    ignoreException("No UpdateLog found - cannot sync");
    ignoreException("No UpdateLog found - cannot recover");
    
    System.setProperty("zkClientTimeout", "8000");
    
    zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    AbstractZkTestCase.buildZooKeeper(zkServer.getZkHost(),
        zkServer.getZkAddress(), "solrconfig.xml", "schema.xml");
    
    log.info("####SETUP_START " + getTestName());
    
    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    
    for (int i = 7000; i < 7000 + NUM_SHARD_REPLICAS; i++) {
      try {
        setupContainer(i, "shard1");
      } catch (Throwable t) {
        log.error("!!!Could not start container:" + i + " The exception thrown was: " + t.getClass() + " " + t.getMessage());
        fail("Could not start container:" + i + ". Reason:" + t.getClass() + " " + t.getMessage());
      }
    }
    try {
      setupContainer(3333, "shard2");
    } catch (Throwable t) {
      log.error("!!!Could not start container 3333. The exception thrown was: " + t.getClass() + " " + t.getMessage());
      fail("Could not start container: 3333");
    }
    
    zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT);
        
    reader = new ZkStateReader(zkClient); 
    reader.createClusterStateWatchersAndUpdate();
    boolean initSuccessful = false;
    for (int i = 0; i < 30; i++) {
      List<String> liveNodes = zkClient.getChildren("/live_nodes", null, true);
      if (liveNodes.size() == NUM_SHARD_REPLICAS + 1) {
        // all nodes up
        initSuccessful = true;
        break;
      }
      Thread.sleep(1000);
      log.info("Waiting for more nodes to come up, now: " + liveNodes.size()
          + "/" + (NUM_SHARD_REPLICAS + 1));
    }
    if (!initSuccessful) {
      fail("Init was not successful!");
    }
    log.info("####SETUP_END " + getTestName());
  }
     
  private void setupContainer(int port, String shard) throws IOException,
      ParserConfigurationException, SAXException {
    Path data = createTempDir();
    
    System.setProperty("hostPort", Integer.toString(port));
    System.setProperty("shard", shard);
    System.setProperty("solr.data.dir", data.toString());
    System.setProperty("solr.solr.home", TEST_HOME());
    Set<Integer> ports = shardPorts.get(shard);
    if (ports == null) {
      ports = new HashSet<>();
      shardPorts.put(shard, ports);
    }
    ports.add(port);

    SolrResourceLoader loader = new SolrResourceLoader(createTempDir());
    Files.copy(TEST_PATH().resolve("solr.xml"), loader.getInstancePath().resolve("solr.xml"));
    CoreContainer container = new CoreContainer(loader);
    container.load();
    container.create("collection1_" + shard, ImmutableMap.of("collection", "collection1"));
    containerMap.put(port, container);
    System.clearProperty("solr.solr.home");
    System.clearProperty("hostPort");
  }
  
  @Test
  public void testSimpleSliceLeaderElection() throws Exception {

    //printLayout(zkServer.getZkAddress());
    for (int i = 0; i < 4; i++) {
      // who is the leader?
      String leader = getLeader();
      
      Set<Integer> shard1Ports = shardPorts.get("shard1");
      
      int leaderPort = getLeaderPort(leader);
      assertTrue(shard1Ports.toString(), shard1Ports.contains(leaderPort));
      
      shard1Ports.remove(leaderPort);
      
      // kill the leader
      containerMap.get(leaderPort).shutdown();
      
      //printLayout(zkServer.getZkAddress());
      
      // poll until leader change is visible
      for (int j = 0; j < 90; j++) {
        String currentLeader = getLeader();
        if(!leader.equals(currentLeader)) {
          break;
        }
        Thread.sleep(500);
      }
      
      leader = getLeader();
      int newLeaderPort = getLeaderPort(leader);
      int retry = 0;
      while (leaderPort == newLeaderPort) {
        if (retry++ == 60) {
          break;
        }
        Thread.sleep(1000);
      }
      
      if (leaderPort == newLeaderPort) {
        zkClient.printLayoutToStdOut();
        fail("We didn't find a new leader! " + leaderPort + " was close, but it's still showing as the leader");
      }
      
      assertTrue("Could not find leader " + newLeaderPort + " in " + shard1Ports, shard1Ports.contains(newLeaderPort));
    }
    

  }
  
  @Test
  public void testLeaderElectionAfterClientTimeout() throws Exception {
    // TODO: work out the best timing here...
    System.setProperty("zkClientTimeout", Integer.toString(ZkTestServer.TICK_TIME * 2 + 100));
    // timeout the leader
    String leader = getLeader();
    int leaderPort = getLeaderPort(leader);
    ZkController zkController = containerMap.get(leaderPort).getZkController();

    zkController.getZkClient().getSolrZooKeeper().closeCnxn();
    long sessionId = zkClient.getSolrZooKeeper().getSessionId();
    zkServer.expire(sessionId);
    
    for (int i = 0; i < 60; i++) { // wait till leader is changed
      if (leaderPort != getLeaderPort(getLeader())) {
        break;
      }
      Thread.sleep(100);
    }
    
    // make sure we have waited long enough for the first leader to have come back
    Thread.sleep(ZkTestServer.TICK_TIME * 2 + 100);
    
    // kill everyone but the first leader that should have reconnected by now
    for (Map.Entry<Integer,CoreContainer> entry : containerMap.entrySet()) {
      if (entry.getKey() != leaderPort) {
        entry.getValue().shutdown();
      }
    }

    for (int i = 0; i < 320; i++) { // wait till leader is changed
      try {
        if (leaderPort == getLeaderPort(getLeader())) {
          break;
        }
        Thread.sleep(100);
      } catch (Exception e) {
        continue;
      }
    }

    // the original leader should be leader again now - everyone else is down
    // TODO: I saw this fail once...expected:<7000> but was:<7004>
    assertEquals(leaderPort, getLeaderPort(getLeader()));
    //printLayout(zkServer.getZkAddress());
    //Thread.sleep(100000);
  }
  
  private String getLeader() throws InterruptedException {
    
    ZkNodeProps props = reader.getLeaderRetry("collection1", "shard1", 30000);
    String leader = props.getStr(ZkStateReader.NODE_NAME_PROP);
    
    return leader;
  }
  
  private int getLeaderPort(String leader) {
    Matcher m = HOST.matcher(leader);
    int leaderPort = 0;
    if (m.matches()) {
      leaderPort = Integer.parseInt(m.group(1));
    } else {
      throw new IllegalStateException();
    }
    return leaderPort;
  }
  
  @Override
  public void tearDown() throws Exception {

    if (zkClient != null) {
      zkClient.close();
    }
    
    if (reader != null) {
      reader.close();
    }
    
    for (CoreContainer cc : containerMap.values()) {
      if (!cc.isShutDown()) {
        cc.shutdown();
      }
    }
    zkServer.shutdown();
    super.tearDown();
    System.clearProperty("zkClientTimeout");
    System.clearProperty("zkHost");
    System.clearProperty("hostPort");
    System.clearProperty("shard");
    System.clearProperty("solrcloud.update.delay");
  }
  
  private void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    System.clearProperty("solrcloud.skip.autorecovery");
    System.clearProperty("zkClientTimeout");
    System.clearProperty("zkHost");
    System.clearProperty("shard");
    System.clearProperty("solr.data.dir");
    System.clearProperty("solr.solr.home");
    resetExceptionIgnores();
    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
  }
}
