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
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

public class LeaderElectionIntegrationTest extends SolrTestCaseJ4 {
  protected static Logger log = LoggerFactory
      .getLogger(AbstractZkTestCase.class);
  
  private final static int NUM_SHARD_REPLICAS = 5;
  
  private static final boolean VERBOSE = false;
  
  private static final Pattern HOST = Pattern
      .compile(".*?\\:(\\d\\d\\d\\d)_.*");
  
  protected ZkTestServer zkServer;
  
  protected String zkDir;
  
  private Map<Integer,CoreContainer> containerMap = new HashMap<Integer,CoreContainer>();
  
  private Map<String,Set<Integer>> shardPorts = new HashMap<String,Set<Integer>>();
  
  private SolrZkClient zkClient;

  private ZkStateReader reader;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTempDir();
    System.setProperty("zkClientTimeout", "3000");
    
    zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    AbstractZkTestCase.buildZooKeeper(zkServer.getZkHost(),
        zkServer.getZkAddress(), "solrconfig.xml", "schema.xml");
    
    log.info("####SETUP_START " + getName());
    
    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    
    for (int i = 7000; i < 7000 + NUM_SHARD_REPLICAS; i++) {
      setupContainer(i, "shard1");
    }
    
    setupContainer(3333, "shard2");
    
    zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT);
    
    reader = new ZkStateReader(zkClient); 

    log.info("####SETUP_END " + getName());
    
  }
  
  private void setupContainer(int port, String shard) throws IOException,
      ParserConfigurationException, SAXException {
    File data = new File(dataDir + File.separator + "data_" + port);
    data.mkdirs();
    
    System.setProperty("hostPort", Integer.toString(port));
    System.setProperty("shard", shard);
    Initializer init = new CoreContainer.Initializer();
    System.setProperty("solr.data.dir", data.getAbsolutePath());
    System.setProperty("solr.solr.home", TEST_HOME());
    Set<Integer> ports = shardPorts.get(shard);
    if (ports == null) {
      ports = new HashSet<Integer>();
      shardPorts.put(shard, ports);
    }
    ports.add(port);
    CoreContainer container = init.initialize();
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
      if (VERBOSE) System.out.println("Killing " + leaderPort);
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
        if (retry++ == 20) {
          break;
        }
        Thread.sleep(1000);
      }
      
      if (leaderPort == newLeaderPort) {
        fail("We didn't find a new leader! " + leaderPort + " was shutdown, but it's still showing as the leader");
      }
      
      assertTrue("Could not find leader " + newLeaderPort + " in " + shard1Ports, shard1Ports.contains(newLeaderPort));
    }
    

  }
  
  @Test
  public void testLeaderElectionAfterClientTimeout() throws Exception {
    // TODO: work out the best timing here...
    System.setProperty("zkClientTimeout", "500");
    // timeout the leader
    String leader = getLeader();
    int leaderPort = getLeaderPort(leader);
    containerMap.get(leaderPort).getZkController().getZkClient().getSolrZooKeeper().pauseCnxn(2000);
    
    for (int i = 0; i < 60; i++) { // wait till leader is changed
      if (leaderPort != getLeaderPort(getLeader())) {
        break;
      }
      Thread.sleep(100);
    }
    
    if (VERBOSE) System.out.println("kill everyone");
    // kill everyone but the first leader that should have reconnected by now
    for (Map.Entry<Integer,CoreContainer> entry : containerMap.entrySet()) {
      if (entry.getKey() != leaderPort) {
        entry.getValue().shutdown();
      }
    }

    for (int i = 0; i < 60; i++) { // wait till leader is changed
      if (leaderPort == getLeaderPort(getLeader())) {
        break;
      }
      Thread.sleep(100);
    }

    // the original leader should be leader again now - everyone else is down
    // TODO: I saw this fail once...expected:<7000> but was:<7004>
    assertEquals(leaderPort, getLeaderPort(getLeader()));
    //printLayout(zkServer.getZkAddress());
    //Thread.sleep(100000);
  }
  
  private String getLeader() throws InterruptedException, KeeperException {
    
    reader.updateCloudState(true);
    ZkNodeProps props = reader.getLeaderProps("collection1", "shard1", 30000);
    String leader = props.get(ZkStateReader.NODE_NAME_PROP);
    
    return leader;
  }
  
  private int getLeaderPort(String leader) {
    Matcher m = HOST.matcher(leader);
    int leaderPort = 0;
    if (m.matches()) {
      leaderPort = Integer.parseInt(m.group(1));
      if (VERBOSE) System.out.println("The leader is:" + Integer.parseInt(m.group(1)));
    } else {
      throw new IllegalStateException();
    }
    return leaderPort;
  }
  
  @Override
  public void tearDown() throws Exception {
    if (VERBOSE) {
      printLayout(zkServer.getZkHost());
    }

    if (zkClient != null) {
      zkClient.close();
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
    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
  }
}
