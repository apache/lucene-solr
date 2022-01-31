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


import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionConfigSetProcessor.getLeaderNode;
import static org.apache.solr.cloud.OverseerTaskProcessor.getSortedElectionNodes;

public class TestOverseerRoleSysProp extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Before
  public void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @After
  public void tearDownCluster() throws Exception {
    shutdownCluster();
  }

  private void waitForNewOverseer(int seconds, Predicate<String> state, boolean failOnIntermediateTransition) throws Exception {
    TimeOut timeout = new TimeOut(seconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    String current = null;
    while (timeout.hasTimedOut() == false) {
      String prev = current;
      current = OverseerCollectionConfigSetProcessor.getLeaderNode(zkClient());
      if (state.test(current))
        return;
      else if (failOnIntermediateTransition) {
        if (prev != null && current != null && !current.equals(prev)) {
          fail ("There was an intermediate transition, previous: "+prev+", intermediate transition: "+current);
        }
      }
      Thread.sleep(100);
    }
    fail("Timed out waiting for overseer state change. The current overseer is: "+current);
  }

  private void waitForNewOverseer(int seconds, String expected, boolean failOnIntermediateTransition) throws Exception {
    log.info("Expecting node: {}", expected);
    waitForNewOverseer(seconds, s -> Objects.equals(s, expected), failOnIntermediateTransition);
  }


  private void logOverseerState() throws KeeperException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info("Overseer: {}", getLeaderNode(zkClient()));
      log.info("Election queue: {}", getSortedElectionNodes(zkClient(), "/overseer_elect/election")); // nowarn
    }
  }

  @Test
  public void testOverseerRole() throws Exception {
    logOverseerState();
//    List<String> nodes = OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames(zkClient());
    System.setProperty("overseer.node", "true");
    JettySolrRunner jetty = cluster.startJettySolrRunner();
    waitForNewOverseer(5, jetty.getNodeName(), false);
    JettySolrRunner jetty2 = cluster.startJettySolrRunner();
    List<String> designates = getDesignates();
    assertTrue(designates.contains(jetty2.getNodeName()));
    assertTrue(designates.contains(jetty.getNodeName()));

    String COLL_NAME = "temp_coll";
    CollectionAdminRequest.createCollection(COLL_NAME, "conf", 4, 1)
        .setMaxShardsPerNode(4)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLL_NAME, 4, 4);
    DocCollection coll = cluster.getSolrClient().getClusterStateProvider().getCollection(COLL_NAME);
    coll.forEachReplica((s, replica) -> assertFalse(replica.getNodeName().equals(jetty.getNodeName()) ||
        replica.getNodeName().equals(jetty2.getNodeName())));
    System.clearProperty("overseer.node");
    jetty.stop();
    waitForNewOverseer(5, jetty2.getNodeName(), false);
    System.setProperty("overseer.node","false");
    jetty.start();
    assertFalse(getDesignates().contains(jetty.getNodeName()));
    System.clearProperty("overseer.node");




  }

  @SuppressWarnings("unchecked")
  private List<String> getDesignates() throws KeeperException, InterruptedException {
    byte[] rolesData =  cluster.getZkClient().getData(ZkStateReader.ROLES, null, null, true);
    Map<String,Object> roles = (Map<String, Object>) Utils.fromJSON(rolesData);
    return (List<String>) roles.getOrDefault("overseer", Collections.emptyList());
  }

}

