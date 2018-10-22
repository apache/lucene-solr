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
package org.apache.solr.handler.component;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandlerFactory;

import org.junit.BeforeClass;
import org.junit.AfterClass;

/**
 * Tests specifying a custom ShardHandlerFactory
 */
public class TestHttpShardHandlerFactory extends SolrTestCaseJ4 {

  private static final String LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE = "solr.tests.loadBalancerRequestsMinimumAbsolute";
  private static final String LOAD_BALANCER_REQUESTS_MAX_FRACTION = "solr.tests.loadBalancerRequestsMaximumFraction";

  private static int   expectedLoadBalancerRequestsMinimumAbsolute = 0;
  private static float expectedLoadBalancerRequestsMaximumFraction = 1.0f;

  @BeforeClass
  public static void beforeTests() throws Exception {
    expectedLoadBalancerRequestsMinimumAbsolute = random().nextInt(3); // 0 .. 2
    expectedLoadBalancerRequestsMaximumFraction = (1+random().nextInt(10))/10f; // 0.1 .. 1.0
    System.setProperty(LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE, Integer.toString(expectedLoadBalancerRequestsMinimumAbsolute));
    System.setProperty(LOAD_BALANCER_REQUESTS_MAX_FRACTION, Float.toString(expectedLoadBalancerRequestsMaximumFraction));
  }

  @AfterClass
  public static void afterTests() {
    System.clearProperty(LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE);
    System.clearProperty(LOAD_BALANCER_REQUESTS_MAX_FRACTION);
  }

  public void testLoadBalancerRequestsMinMax() throws Exception {
    final Path home = Paths.get(TEST_HOME());
    CoreContainer cc = null;
    ShardHandlerFactory factory = null;
    try {
      cc = CoreContainer.createAndLoad(home, home.resolve("solr-shardhandler-loadBalancerRequests.xml"));
      factory = cc.getShardHandlerFactory();

      // test that factory is HttpShardHandlerFactory with expected url reserve fraction
      assertTrue(factory instanceof HttpShardHandlerFactory);
      final HttpShardHandlerFactory httpShardHandlerFactory = ((HttpShardHandlerFactory)factory);
      assertEquals(expectedLoadBalancerRequestsMinimumAbsolute, httpShardHandlerFactory.permittedLoadBalancerRequestsMinimumAbsolute, 0.0);
      assertEquals(expectedLoadBalancerRequestsMaximumFraction, httpShardHandlerFactory.permittedLoadBalancerRequestsMaximumFraction, 0.0);

      // create a dummy request and dummy url list
      final QueryRequest queryRequest = null;
      final List<String> urls = new ArrayList<>();
      for (int ii=0; ii<10; ++ii) {
        urls.add(null);
      }

      // create LBHttpSolrClient request
      final LBHttpSolrClient.Req req = httpShardHandlerFactory.newLBHttpSolrClientReq(queryRequest, urls);

      // actual vs. expected test
      final int actualNumServersToTry = req.getNumServersToTry().intValue();
      int expectedNumServersToTry = (int)Math.floor(urls.size() * expectedLoadBalancerRequestsMaximumFraction);
      if (expectedNumServersToTry < expectedLoadBalancerRequestsMinimumAbsolute) {
        expectedNumServersToTry = expectedLoadBalancerRequestsMinimumAbsolute;
      }
      assertEquals("wrong numServersToTry for"
          + " urls.size="+urls.size()
          + " expectedLoadBalancerRequestsMinimumAbsolute="+expectedLoadBalancerRequestsMinimumAbsolute
          + " expectedLoadBalancerRequestsMaximumFraction="+expectedLoadBalancerRequestsMaximumFraction,
          expectedNumServersToTry,
          actualNumServersToTry);

    } finally {
      if (factory != null) factory.close();
      if (cc != null) cc.shutdown();
    }
  }

  @SuppressWarnings("unchecked")
  public void testNodePreferenceRulesComparator() throws Exception {
    List<Replica> replicas = new ArrayList<Replica>();
    replicas.add(
      new Replica(
        "node1",
        map(
          ZkStateReader.BASE_URL_PROP, "http://host1:8983/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.CORE_NAME_PROP, "collection1",
          ZkStateReader.REPLICA_TYPE, "NRT"
        )
      )
    );
    replicas.add(
      new Replica(
        "node2",
        map(
          ZkStateReader.BASE_URL_PROP, "http://host2:8983/solr",
          ZkStateReader.NODE_NAME_PROP, "node2",
          ZkStateReader.CORE_NAME_PROP, "collection1",
          ZkStateReader.REPLICA_TYPE, "TLOG"
        )
      )
    );
    replicas.add(
      new Replica(
        "node3",
        map(
          ZkStateReader.BASE_URL_PROP, "http://host2_2:8983/solr",
          ZkStateReader.NODE_NAME_PROP, "node3",
          ZkStateReader.CORE_NAME_PROP, "collection1",
          ZkStateReader.REPLICA_TYPE, "PULL"
        )
      )
    );

    // Simple replica type rule
    List<String> rules = StrUtils.splitSmart(
      ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT," + 
      ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG", 
      ','
    );
    HttpShardHandlerFactory.NodePreferenceRulesComparator comparator = 
      new HttpShardHandlerFactory.NodePreferenceRulesComparator(rules, null);
    replicas.sort(comparator);
    assertEquals("node1", replicas.get(0).getNodeName());
    assertEquals("node2", replicas.get(1).getNodeName());

    // Another simple replica type rule
    rules = StrUtils.splitSmart(
      ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG," + 
      ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT", 
      ','
    );
    comparator = new HttpShardHandlerFactory.NodePreferenceRulesComparator(rules, null);
    replicas.sort(comparator);
    assertEquals("node2", replicas.get(0).getNodeName());
    assertEquals("node1", replicas.get(1).getNodeName());

    // replicaLocation rule
    rules = StrUtils.splitSmart(ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION + ":http://host2:8983", ',');
    comparator = new HttpShardHandlerFactory.NodePreferenceRulesComparator(rules, null);
    replicas.sort(comparator);
    assertEquals("node2", replicas.get(0).getNodeName());
    assertEquals("node1", replicas.get(1).getNodeName());

    // Add a replica so that sorting by replicaType:TLOG can cause a tie
    replicas.add(
      new Replica(
        "node4",
        map(
          ZkStateReader.BASE_URL_PROP, "http://host2_2:8983/solr",
          ZkStateReader.NODE_NAME_PROP, "node4",
          ZkStateReader.CORE_NAME_PROP, "collection1",
          ZkStateReader.REPLICA_TYPE, "TLOG"
        )
      )
    );

    // replicaType and replicaLocation combined rule
    rules = StrUtils.splitSmart(
      ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":NRT," + 
      ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG," + 
      ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION + ":http://host2_2", 
      ','
    );
    comparator = new HttpShardHandlerFactory.NodePreferenceRulesComparator(rules, null);
    replicas.sort(comparator);
    assertEquals("node1", replicas.get(0).getNodeName());
    assertEquals("node4", replicas.get(1).getNodeName());
    assertEquals("node2", replicas.get(2).getNodeName());
    assertEquals("node3", replicas.get(3).getNodeName());

    // Bad rule
    rules = StrUtils.splitSmart(ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE, ',');
    try {
      comparator = new HttpShardHandlerFactory.NodePreferenceRulesComparator(rules, null);
      replicas.sort(comparator);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid shards.preference rule: " + ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE, e.getMessage());
    }

    // Unknown rule
    rules = StrUtils.splitSmart("badRule:test", ',');
    try {
      comparator = new HttpShardHandlerFactory.NodePreferenceRulesComparator(rules, null);
      replicas.sort(comparator);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid shards.preference type: badRule", e.getMessage());
    }
  }

}
