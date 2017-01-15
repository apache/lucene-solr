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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionConfigSetProcessor.getLeaderNode;
import static org.apache.solr.cloud.OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames;
import static org.hamcrest.CoreMatchers.not;

public class OverseerRolesTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void clearAllOverseerRoles() throws Exception {
    for (String node : OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames(zkClient())) {
      CollectionAdminRequest.removeRole(node, "overseer").process(cluster.getSolrClient());
    }
  }

  @Test
  public void testQuitCommand() throws Exception {

    SolrZkClient zk = zkClient();
    byte[] data = zk.getData("/overseer_elect/leader", null, new Stat(), true);
    Map m = (Map) Utils.fromJSON(data);
    String s = (String) m.get("id");
    String leader = LeaderElector.getNodeName(s);
    log.info("Current overseer: {}", leader);
    Overseer.getStateUpdateQueue(zk)
        .offer(Utils.toJSON(new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.QUIT.toLower(),
                                            "id", s)));
    final TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS);
    String newLeader = null;
    for(;! timeout.hasTimedOut();){
      newLeader = OverseerCollectionConfigSetProcessor.getLeaderNode(zk);
      if (newLeader != null && !newLeader.equals(leader))
        break;
      Thread.sleep(100);
    }
    assertThat("Leader not changed yet", newLeader, not(leader));

    assertTrue("The old leader should have rejoined election",
        OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames(zk).contains(leader));
  }

  @Test
  public void testOverseerRole() throws Exception {

    List<String> l = OverseerCollectionConfigSetProcessor.getSortedOverseerNodeNames(zkClient()) ;

    log.info("All nodes {}", l);
    String currentLeader = OverseerCollectionConfigSetProcessor.getLeaderNode(zkClient());
    log.info("Current leader {} ", currentLeader);
    l.remove(currentLeader);

    Collections.shuffle(l, random());
    String overseerDesignate = l.get(0);
    log.info("overseerDesignate {}", overseerDesignate);

    CollectionAdminRequest.addRole(overseerDesignate, "overseer").process(cluster.getSolrClient());

    TimeOut timeout = new TimeOut(15, TimeUnit.SECONDS);

    boolean leaderchanged = false;
    for (;!timeout.hasTimedOut();) {
      if (overseerDesignate.equals(OverseerCollectionConfigSetProcessor.getLeaderNode(zkClient()))) {
        log.info("overseer designate is the new overseer");
        leaderchanged =true;
        break;
      }
      Thread.sleep(100);
    }
    assertTrue("could not set the new overseer . expected "+
        overseerDesignate + " current order : " +
        getSortedOverseerNodeNames(zkClient()) +
        " ldr :"+ OverseerCollectionConfigSetProcessor.getLeaderNode(zkClient()) ,leaderchanged);

    //add another node as overseer
    l.remove(overseerDesignate);
    Collections.shuffle(l, random());

    String anotherOverseer = l.get(0);
    log.info("Adding another overseer designate {}", anotherOverseer);
    CollectionAdminRequest.addRole(anotherOverseer, "overseer").process(cluster.getSolrClient());

    String currentOverseer = getLeaderNode(zkClient());

    log.info("Current Overseer {}", currentOverseer);

    String hostPort = currentOverseer.substring(0, currentOverseer.indexOf('_'));

    StringBuilder sb = new StringBuilder();
    log.info("hostPort : {}", hostPort);

    JettySolrRunner leaderJetty = null;

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      String s = jetty.getBaseUrl().toString();
      log.info("jetTy {}",s);
      sb.append(s).append(" , ");
      if (s.contains(hostPort)) {
        leaderJetty = jetty;
        break;
      }
    }

    assertNotNull("Could not find a jetty2 kill",  leaderJetty);

    log.info("leader node {}", leaderJetty.getBaseUrl());
    log.info("current election Queue",
        OverseerCollectionConfigSetProcessor.getSortedElectionNodes(zkClient(), "/overseer_elect/election"));
    ChaosMonkey.stop(leaderJetty);
    timeout = new TimeOut(10, TimeUnit.SECONDS);
    leaderchanged = false;
    for (; !timeout.hasTimedOut(); ) {
      currentOverseer = getLeaderNode(zkClient());
      if (anotherOverseer.equals(currentOverseer)) {
        leaderchanged = true;
        break;
      }
      Thread.sleep(100);
    }
    assertTrue("New overseer designate has not become the overseer, expected : " + anotherOverseer + "actual : " + getLeaderNode(zkClient()), leaderchanged);
  }

}
