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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionConfigSetProcessor.getLeaderNode;
import static org.apache.solr.cloud.OverseerTaskProcessor.getSortedElectionNodes;

public class OverseerRolesTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  private void waitForNewOverseer(int seconds, Predicate<String> state) throws Exception {
    TimeOut timeout = new TimeOut(seconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    String current = null;
    while (timeout.hasTimedOut() == false) {
      current = OverseerCollectionConfigSetProcessor.getLeaderNode(zkClient());
      if (state.test(current))
        return;
      Thread.sleep(100);
    }
    fail("Timed out waiting for overseer state change");
  }

  private void waitForNewOverseer(int seconds, String expected) throws Exception {
    waitForNewOverseer(seconds, s -> Objects.equals(s, expected));
  }

  private JettySolrRunner getOverseerJetty() throws Exception {
    String overseer = getLeaderNode(zkClient());
    URL overseerUrl = new URL("http://" + overseer.substring(0, overseer.indexOf('_')));
    int hostPort = overseerUrl.getPort();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (jetty.getBaseUrl().getPort() == hostPort)
        return jetty;
    }
    fail("Couldn't find overseer node " + overseer);
    return null; // to keep the compiler happy
  }

  private void logOverseerState() throws KeeperException, InterruptedException {
    log.info("Overseer: {}", getLeaderNode(zkClient()));
    log.info("Election queue: ", getSortedElectionNodes(zkClient(), "/overseer_elect/election"));
  }

  @Test
  //commented 2-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 04-May-2018
  public void testOverseerRole() throws Exception {

    logOverseerState();
    List<String> nodes = OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames(zkClient());
    String overseer1 = OverseerCollectionConfigSetProcessor.getLeaderNode(zkClient());
    nodes.remove(overseer1);

    Collections.shuffle(nodes, random());
    String overseer2 = nodes.get(0);
    log.info("### Setting overseer designate {}", overseer2);

    CollectionAdminRequest.addRole(overseer2, "overseer").process(cluster.getSolrClient());

    waitForNewOverseer(15, overseer2);

    //add another node as overseer
    nodes.remove(overseer2);
    Collections.shuffle(nodes, random());

    String overseer3 = nodes.get(0);
    log.info("### Adding another overseer designate {}", overseer3);
    CollectionAdminRequest.addRole(overseer3, "overseer").process(cluster.getSolrClient());

    // kill the current overseer, and check that the new designate becomes the new overseer
    JettySolrRunner leaderJetty = getOverseerJetty();
    logOverseerState();

    ChaosMonkey.stop(leaderJetty);
    waitForNewOverseer(10, overseer3);

    // add another node as overseer
    nodes.remove(overseer3);
    Collections.shuffle(nodes, random());
    String overseer4 = nodes.get(0);
    log.info("### Adding last overseer designate {}", overseer4);
    CollectionAdminRequest.addRole(overseer4, "overseer").process(cluster.getSolrClient());
    logOverseerState();

    // remove the overseer role from the current overseer
    CollectionAdminRequest.removeRole(overseer3, "overseer").process(cluster.getSolrClient());
    waitForNewOverseer(15, overseer4);

    // Add it back again - we now have two delegates, 4 and 3
    CollectionAdminRequest.addRole(overseer3, "overseer").process(cluster.getSolrClient());

    // explicitly tell the overseer to quit
    String leaderId = OverseerCollectionConfigSetProcessor.getLeaderId(zkClient());
    String leader = OverseerCollectionConfigSetProcessor.getLeaderNode(zkClient());
    log.info("### Sending QUIT to overseer {}", leader);
    Overseer.getStateUpdateQueue(zkClient())
        .offer(Utils.toJSON(new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.QUIT.toLower(),
            "id", leaderId)));

    waitForNewOverseer(15, s -> Objects.equals(leader, s) == false);

    Thread.sleep(1000);
    
    logOverseerState();
    assertTrue("The old leader should have rejoined election",
        OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames(zkClient()).contains(leader));

  }

}
