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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.apache.solr.core.SolrConfig;
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
  
  @BeforeClass
  public static void beforeClass() throws Exception {}
  
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
    Set<Integer> ports = shardPorts.get(shard);
    if (ports == null) {
      ports = new HashSet<Integer>();
      shardPorts.put(shard, ports);
    }
    ports.add(port);
    CoreContainer container = init.initialize();
    containerMap.put(port, container);
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
      
      // wait a sec for new leader to register
      Thread.sleep(1000);
      
      leader = getLeader();
      int newLeaderPort = getLeaderPort(leader);
      
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
    
    Thread.sleep(4000);
    
    // first leader should not be leader anymore
    assertNotSame(leaderPort, getLeaderPort(getLeader()));
    
    if (VERBOSE) System.out.println("kill everyone");
    // kill everyone but the first leader that should have reconnected by now
    for (Map.Entry<Integer,CoreContainer> entry : containerMap.entrySet()) {
      if (entry.getKey() != leaderPort) {
        entry.getValue().shutdown();
      }
    }
    
    Thread.sleep(1000);
    
    // the original leader should be leader again now - everyone else is down
    assertEquals(leaderPort, getLeaderPort(getLeader()));
    //printLayout(zkServer.getZkAddress());
    //Thread.sleep(100000);
  }
  
  private String getLeader() throws Exception {
    String leader = null;
    int tries = 30;
    while (true) {
      List<String> leaderChildren = zkClient.getChildren(
          "/collections/collection1/leader_elect/shard1/leader", null);
      if (leaderChildren.size() > 0) {
        assertEquals("There should only be one leader", 1,
            leaderChildren.size());
        leader = leaderChildren.get(0);
        break;
      } else {
        if (tries-- == 0) {
          printLayout(zkServer.getZkAddress());
          fail("No registered leader was found");
        }
        Thread.sleep(1000);
      }
    }
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
    zkClient.close();
    
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
    System.clearProperty("CLOUD_UPDATE_DELAY");
    SolrConfig.severeErrors.clear();
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
