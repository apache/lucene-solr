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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.HttpShardHandlerFactory.WhitelistHostChecker;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests specifying a custom ShardHandlerFactory
 */
public class TestHttpShardHandlerFactory extends SolrTestCaseJ4 {

  private static final String LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE = "solr.tests.loadBalancerRequestsMinimumAbsolute";
  private static final String LOAD_BALANCER_REQUESTS_MAX_FRACTION = "solr.tests.loadBalancerRequestsMaximumFraction";
  private static final String SHARDS_WHITELIST = "solr.tests.shardsWhitelist";

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
      final LBSolrClient.Req req = httpShardHandlerFactory.newLBHttpSolrClientReq(queryRequest, urls);

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

  @Test
  public void getShardsWhitelist() throws Exception {
    System.setProperty(SHARDS_WHITELIST, "http://abc:8983/,http://def:8984/,");
    final Path home = Paths.get(TEST_HOME());
    CoreContainer cc = null;
    ShardHandlerFactory factory = null;
    try {
      cc = CoreContainer.createAndLoad(home, home.resolve("solr.xml"));
      factory = cc.getShardHandlerFactory();
      assertTrue(factory instanceof HttpShardHandlerFactory);
      final HttpShardHandlerFactory httpShardHandlerFactory = ((HttpShardHandlerFactory)factory);
      assertThat(httpShardHandlerFactory.getWhitelistHostChecker().getWhitelistHosts().size(), is(2));
      assertThat(httpShardHandlerFactory.getWhitelistHostChecker().getWhitelistHosts(), hasItem("abc:8983"));
      assertThat(httpShardHandlerFactory.getWhitelistHostChecker().getWhitelistHosts(), hasItem("def:8984"));
    } finally {
      if (factory != null) factory.close();
      if (cc != null) cc.shutdown();
      System.clearProperty(SHARDS_WHITELIST);
    }
  }
  
  @Test
  public void testLiveNodesToHostUrl() throws Exception {
    Set<String> liveNodes = new HashSet<>(Arrays.asList(new String[]{
        "1.2.3.4:8983_solr",
        "1.2.3.4:9000_",
        "1.2.3.4:9001_solr-2",
    }));
    ClusterState cs = new ClusterState(0, liveNodes, new HashMap<>());
    WhitelistHostChecker checker = new WhitelistHostChecker(null, true);
    Set<String> hostSet = checker.generateWhitelistFromLiveNodes(cs);
    assertThat(hostSet.size(), is(3));
    assertThat(hostSet, hasItem("1.2.3.4:8983"));
    assertThat(hostSet, hasItem("1.2.3.4:9000"));
    assertThat(hostSet, hasItem("1.2.3.4:9001"));
  }
  
  @Test
  public void testWhitelistHostCheckerDisabled() throws Exception {
    WhitelistHostChecker checker = new WhitelistHostChecker("http://cde:8983", false);
    checker.checkWhitelist("http://abc-1.com:8983/solr", Arrays.asList(new String[]{"abc-1.com:8983/solr"}));
    
    try {
      checker = new WhitelistHostChecker("http://cde:8983", true);
      checker.checkWhitelist("http://abc-1.com:8983/solr", Arrays.asList(new String[]{"http://abc-1.com:8983/solr"}));
      fail("Expecting exception");
    } catch (SolrException se) {
      assertThat(se.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
    }
  }
  
  @Test
  public void testWhitelistHostCheckerNoInput() throws Exception {
    assertNull("Whitelist hosts should be null with null input",
        new WhitelistHostChecker(null, true).getWhitelistHosts());
    assertNull("Whitelist hosts should be null with empty input",
        new WhitelistHostChecker("", true).getWhitelistHosts());
  }
  
  @Test
  public void testWhitelistHostCheckerSingleHost() {
    WhitelistHostChecker checker = new WhitelistHostChecker("http://abc-1.com:8983/solr", true);
    checker.checkWhitelist("http://abc-1.com:8983/solr", Arrays.asList(new String[]{"http://abc-1.com:8983/solr"}));
  }
  
  @Test
  public void testWhitelistHostCheckerMultipleHost() {
    WhitelistHostChecker checker = new WhitelistHostChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983", true);
    checker.checkWhitelist("http://abc-1.com:8983/solr", Arrays.asList(new String[]{"http://abc-1.com:8983/solr"}));
  }
  
  @Test
  public void testWhitelistHostCheckerMultipleHost2() {
    WhitelistHostChecker checker = new WhitelistHostChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983", true);
    checker.checkWhitelist("http://abc-1.com:8983/solr", Arrays.asList(new String[]{"http://abc-1.com:8983/solr", "http://abc-2.com:8983/solr"}));
  }
  
  @Test
  public void testWhitelistHostCheckerNoProtocolInParameter() {
    WhitelistHostChecker checker = new WhitelistHostChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983", true);
    checker.checkWhitelist("abc-1.com:8983/solr", Arrays.asList(new String[]{"abc-1.com:8983/solr"}));
  }
  
  @Test
  public void testWhitelistHostCheckerNonWhitelistedHost1() {
    WhitelistHostChecker checker = new WhitelistHostChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983", true);
    try {
      checker.checkWhitelist("http://abc-1.com:8983/solr", Arrays.asList(new String[]{"http://abc-4.com:8983/solr"}));
      fail("Expected exception");
    } catch (SolrException e) {
      assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
      assertThat(e.getMessage(), containsString("not on the shards whitelist"));
    }
  }
  
  @Test
  public void testWhitelistHostCheckerNonWhitelistedHost2() {
    WhitelistHostChecker checker = new WhitelistHostChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983", true);
    try {
      checker.checkWhitelist("http://abc-1.com:8983/solr", Arrays.asList(new String[]{"http://abc-1.com:8983/solr", "http://abc-4.com:8983/solr"}));
      fail("Expected exception");
    } catch (SolrException e) {
      assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
      assertThat(e.getMessage(), containsString("not on the shards whitelist"));
    }
  }
  
  @Test
  public void testWhitelistHostCheckerNonWhitelistedHostHttps() {
    WhitelistHostChecker checker = new WhitelistHostChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983", true);
    checker.checkWhitelist("https://abc-1.com:8983/solr", Arrays.asList(new String[]{"https://abc-1.com:8983/solr"}));
  }
  
  @Test
  public void testWhitelistHostCheckerInvalidUrl() {
    WhitelistHostChecker checker = new WhitelistHostChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983", true);
    try {
      checker.checkWhitelist("abc_1", Arrays.asList(new String[]{"abc_1"}));
      fail("Expected exception");
    } catch (SolrException e) {
      assertThat(e.code(), is(SolrException.ErrorCode.BAD_REQUEST.code));
      assertThat(e.getMessage(), containsString("Invalid URL syntax"));
    }
  }
  
  @Test
  public void testWhitelistHostCheckerCoreSpecific() {
    // cores are removed completely so it doesn't really matter if they were set in config
    WhitelistHostChecker checker = new WhitelistHostChecker("http://abc-1.com:8983/solr/core1, http://abc-2.com:8983/solr2/core2", true);
    checker.checkWhitelist("http://abc-1.com:8983/solr/core2", Arrays.asList(new String[]{"http://abc-1.com:8983/solr/core2"}));
  }
  
  @Test
  public void testGetShardsOfWhitelistedHostsUnset() {
    assertThat(WhitelistHostChecker.implGetShardsWhitelist(null), nullValue());
  }
  
  @Test
  public void testGetShardsOfWhitelistedHostsEmpty() {
    assertThat(WhitelistHostChecker.implGetShardsWhitelist(""), nullValue());
  }
  
  @Test
  public void testGetShardsOfWhitelistedHostsSingle() {
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://abc-1.com:8983/solr/core1").size(), is(1));
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://abc-1.com:8983/solr/core1").iterator().next(), equalTo("abc-1.com:8983"));
  }
  
  @Test
  public void testGetShardsOfWhitelistedHostsMulti() {
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://abc-1.com:8983/solr/core1,http://abc-1.com:8984/solr").size(), is(2));
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://abc-1.com:8983/solr/core1,http://abc-1.com:8984/solr"), hasItem("abc-1.com:8983"));
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://abc-1.com:8983/solr/core1,http://abc-1.com:8984/solr"), hasItem("abc-1.com:8984"));
  }
  
  @Test
  public void testGetShardsOfWhitelistedHostsIpv4() {
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://10.0.0.1:8983/solr/core1,http://127.0.0.1:8984/solr").size(), is(2));
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://10.0.0.1:8983/solr/core1,http://127.0.0.1:8984/solr"), hasItem("10.0.0.1:8983"));
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://10.0.0.1:8983/solr/core1,http://127.0.0.1:8984/solr"), hasItem("127.0.0.1:8984"));
  }
  
  @Test
  public void testGetShardsOfWhitelistedHostsIpv6() {
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://[2001:abc:abc:0:0:123:456:1234]:8983/solr/core1,http://[::1]:8984/solr").size(), is(2));
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://[2001:abc:abc:0:0:123:456:1234]:8983/solr/core1,http://[::1]:8984/solr"), hasItem("[2001:abc:abc:0:0:123:456:1234]:8983"));
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("http://[2001:abc:abc:0:0:123:456:1234]:8983/solr/core1,http://[::1]:8984/solr"), hasItem("[::1]:8984"));
  }
  
  @Test
  public void testGetShardsOfWhitelistedHostsHttps() {
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("https://abc-1.com:8983/solr/core1").size(), is(1));
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("https://abc-1.com:8983/solr/core1"), hasItem("abc-1.com:8983"));
  }
  
  @Test
  public void testGetShardsOfWhitelistedHostsNoProtocol() {
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("abc-1.com:8983/solr"),
        equalTo(WhitelistHostChecker.implGetShardsWhitelist("http://abc-1.com:8983/solr")));
    assertThat(WhitelistHostChecker.implGetShardsWhitelist("abc-1.com:8983/solr"),
        equalTo(WhitelistHostChecker.implGetShardsWhitelist("https://abc-1.com:8983/solr")));
  }
}
