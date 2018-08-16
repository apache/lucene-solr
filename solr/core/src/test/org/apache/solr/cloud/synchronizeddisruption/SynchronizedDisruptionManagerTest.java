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

package org.apache.solr.cloud.synchronizeddisruption;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.synchronizeddisruption.SynchronizedDisruptionManager.CLASSNAME_PLUGIN_INFO_ATTRIBUTE;
import static org.apache.solr.cloud.synchronizeddisruption.SynchronizedDisruptionManager.CRON_PLUGIN_INFO_ATTRIBUTE;
import static org.apache.solr.cloud.synchronizeddisruption.SynchronizedDisruptionManager.DISRUPTION_NAME;
import static org.apache.solr.cloud.synchronizeddisruption.SynchronizedDisruptionManager.SYNCHRONIZED_DISRUPTION_ZNODE;

@LogLevel("org.apache.zookeeper=ERROR,org.apache.solr.common.cloud.ConnectionManager=ERROR")
public class SynchronizedDisruptionManagerTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NODE_COUNT = 1;
  private SolrResourceLoader loader;
  private SynchronizedDisruptionManager manager;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    // clear any persisted auto scaling configuration
    if (zkClient().exists(SYNCHRONIZED_DISRUPTION_ZNODE,true))
      zkClient().setData(SYNCHRONIZED_DISRUPTION_ZNODE, Utils.toJSON(new ZkNodeProps()), true);

    if (cluster.getJettySolrRunners().size() < NODE_COUNT) {
      // start some to get to original state
      int numJetties = cluster.getJettySolrRunners().size();
      for (int i = 0; i < NODE_COUNT - numJetties; i++) {
        cluster.startJettySolrRunner();
      }
    }
    cluster.waitForAllNodes(30);
    CoreContainer coreContainer = cluster.getJettySolrRunner(0).getCoreContainer();
    loader = cluster.getJettySolrRunner(0).getCoreContainer().getResourceLoader();
    manager = new SynchronizedDisruptionManager(coreContainer);
  }

  @After
  public void cleanUp() {
    this.manager.closeAll();
  }

  @Test
  public void addSynchronizedDisruption() throws KeeperException, InterruptedException {
    String testSD = "testSD";
    String cron = "0/30 * * * * ?";
    boolean addedDisruption = manager.addSynchronizedDisruption(loader, testSD, GarbageCollection.class.getName(),cron, true);
    assertTrue(addedDisruption);
    List<String> disruptionsList = manager.listDisruptions();
    log.info("Asserting that there are existing disruptions");
    assertEquals(1, disruptionsList.size());
    assertTrue(String.join(",", disruptionsList).contains(testSD));

    CloudSolrClient client = cluster.getSolrClient();

    List<String> children = client.getZkStateReader().getZkClient().getChildren(SYNCHRONIZED_DISRUPTION_ZNODE,null, true);
    log.info("Checking ZK children");
    assertEquals(1, children.size());
    log.info("Checking that ZK children contain the disruption name");
    assertEquals(testSD, children.get(0));

    List<Map<String, String>> existingDisruptions = manager.getExistingZKDisruptions();
    log.info("Asserting that there are existing disruptions");
    assertEquals(1, existingDisruptions.size());
    Map<String, String> disruption = existingDisruptions.get(0);
    log.info("Asserting that the disruption name matches");
    assertEquals(testSD, disruption.get(DISRUPTION_NAME));
    log.info("Asserting that the disruption cron matches");
    assertEquals(cron, disruption.get(CRON_PLUGIN_INFO_ATTRIBUTE));
    log.info("Asserting that the disruption class matches");
    assertEquals(GarbageCollection.class.getName(), disruption.get(CLASSNAME_PLUGIN_INFO_ATTRIBUTE));
  }

  @Test
  public void closeSynchronizedDisruption() {
    String testSD = "testSD";
    String cron = "0/30 * * * * ?";
    manager.addSynchronizedDisruption(loader, testSD, GarbageCollection.class.getName(),cron, false);
    List<String> disruptionsList = manager.listDisruptions();
    log.info("Asserting that there are existing disruptions");
    assertEquals(1, disruptionsList.size());
    log.info("Asserting that there are existing disruptions and they contain the disruption name");
    assertTrue(String.join(",", disruptionsList).contains(testSD));

    boolean closedTestSD = manager.closeSynchronizedDisruption(testSD);
    log.info("Asserting that the test disruption is closed");
    assertTrue(closedTestSD);
    disruptionsList = manager.listDisruptions();
    log.info("Asserting that there are no disruptions remaining");
    assertEquals(0, disruptionsList.size());

    log.info("Asserting that a fake disruption is closed");
    assertFalse(manager.closeSynchronizedDisruption("fakeName"));
  }

  @Test
  public void closeAll() {
    String testSD = "testSD";
    String cron = "0/30 * * * * ?";
    manager.addSynchronizedDisruption(loader, testSD, GarbageCollection.class.getName(),cron, false);
    List<String> disruptionsList = manager.listDisruptions();
    log.info("Asserting that there are existing disruptions");
    assertEquals(1, disruptionsList.size());
    log.info("Asserting that there are existing disruptions and they contain the disruption name");
    assertTrue(String.join(",", disruptionsList).contains(testSD));

    manager.closeAll();
    disruptionsList = manager.listDisruptions();
    log.info("Asserting that there are no existing disruptions");
    assertEquals(0, disruptionsList.size());
  }

  @Test
  public void getExistingZKDisruptions() throws KeeperException, InterruptedException {
    String testSD = "testSD";
    String cron = "0/30 * * * * ?";
    manager.addSynchronizedDisruption(loader, testSD, GarbageCollection.class.getName(),cron, true);
    List<Map<String, String>> existingDisruptions = manager.getExistingZKDisruptions();
    log.info("Asserting that there are existing disruptions");
    assertEquals(1, existingDisruptions.size());

    Map<String, String> disruption = existingDisruptions.get(0);
    log.info("Asserting that the disruption name matches");
    assertEquals(testSD, disruption.get(DISRUPTION_NAME));
    log.info("Asserting that the disruption cron matches");
    assertEquals(cron, disruption.get(CRON_PLUGIN_INFO_ATTRIBUTE));
    log.info("Asserting that the disruption class matches");
    assertEquals(GarbageCollection.class.getName(), disruption.get(CLASSNAME_PLUGIN_INFO_ATTRIBUTE));
  }
}