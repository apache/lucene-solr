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
package org.apache.solr.client.solrj.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.rule.ClientSnitchContext;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertThat;
import static org.hamcrest.core.Is.is;

public class SolrUrlListBuilderTest {

  private ImplicitSnitch snitch;
  private SnitchContext context;

  private static final String IP_1 = "ip_1";
  private static final String IP_2 = "ip_2";
  private static final String IP_3 = "ip_3";
  private static final String IP_4 = "ip_4";


  @Before
  public void beforeSolrUrlListBuilderTest() {
    snitch = new ImplicitSnitch();
    context = new ClientSnitchContext(null, null, new HashMap<>());
  }


  @Test
  public void testBuildUrlList()
  {

    SolrUrlListBuilder urlSelector = new SolrUrlListBuilder(snitch,context);

    Map<String,Slice> slices = new HashMap<>();
    Map<String,Replica> replicas = new HashMap<>();

    Map<String,Object> propMap = new HashMap<>();
    propMap.put("core", "COL_shard1_replica1");
    propMap.put("base_url", "http://server1:8983/solr");
    propMap.put("node_name", "server1:8983_solr");
    propMap.put("state", "active");
    Replica replica = new Replica("core_node1", propMap);
    replicas.put("core_node1", replica);

    propMap = new HashMap<>();
    propMap.put("core", "COL_shard1_replica2");
    propMap.put("base_url", "http://server2:8983/solr");
    propMap.put("node_name", "server2:8983_solr");
    propMap.put("state", "active");
    propMap.put("leader", "true");
    replica = new Replica("core_node2", propMap);
    replicas.put("core_node2", replica);

    Slice slice= new Slice("shard1", replicas, null);

    slices.put("COL_shard1", slice);
    Set<String> liveNodes = new HashSet<>();
    liveNodes.add("server1:8983_solr");
    liveNodes.add("server2:8983_solr");

    boolean sendToLeaders = false;
    String collection = "COL";
    SolrQuery query = new SolrQuery("*:*");

    List<String> urlList = urlSelector.buildUrlList(slices, liveNodes, sendToLeaders, query, collection);
    Assert.assertEquals(urlList.size(), 2);

    //Add one more slice

    propMap = new HashMap<>();
    propMap.put("core", "COL_shard2_replica1");
    propMap.put("base_url", "http://server3:8983/solr");
    propMap.put("node_name", "server3:8983_solr");
    propMap.put("state", "active");
    replica = new Replica("core_node3", propMap);
    replicas.put("core_node3", replica);

    propMap = new HashMap<>();
    propMap.put("core", "COL_shard2_replica2");
    propMap.put("base_url", "http://server4:8983/solr");
    propMap.put("node_name", "server4:8983_solr");
    propMap.put("state", "active");
    propMap.put("leader", "true");
    replica = new Replica("core_node4", propMap);
    replicas.put("core_node4", replica);

    slice= new Slice("shard2", replicas, null);
    slices.put("COL_shard2", slice);
   // liveNodes = new HashSet<>();
    liveNodes.add("server3:8983_solr");
    liveNodes.add("server4:8983_solr");

    urlList = urlSelector.buildUrlList(slices, liveNodes, sendToLeaders, query, collection);

    assertThat(urlList.size(), is(4));

  }

  @Test
  public void testBuildUrlListForRoutingRules()
  {
    ImplicitSnitch mockedSnitch = Mockito.spy(snitch);

    when(mockedSnitch.getHostIp("serv01.dc01.ny.us.apache.org")).thenReturn("10.1.12.101");
    when(mockedSnitch.getHostIp("serv01.dc02.ny.us.apache.org")).thenReturn("10.2.12.101");
    when(mockedSnitch.getHostIp("serv02.dc01.ny.us.apache.org")).thenReturn("10.1.12.102");
    when(mockedSnitch.getHostIp("serv02.dc02.ny.us.apache.org")).thenReturn("10.2.12.102");


    SolrUrlListBuilder urlSelector = new SolrUrlListBuilder(mockedSnitch,context);

    Map<String,Slice> slices = new HashMap<>();
    Map<String,Replica> replicas = new HashMap<>();
    Map<String,Object> propMap = new HashMap<>();

    //replica1
    propMap.put("core", "COL_shard1_replica1");
    propMap.put("base_url", "http://serv01.dc01.ny.us.apache.org:8983/solr");
    propMap.put("node_name", "serv01.dc01.ny.us.apache.org:8983_solr");
    propMap.put("state", "active");
    Replica replica = new Replica("core_node1", propMap);
    replicas.put("core_node1", replica);

    //replica2
    propMap = new HashMap<>();
    propMap.put("core", "COL_shard1_replica2");
    propMap.put("base_url", "http://serv01.dc02.ny.us.apache.org:8983/solr");
    propMap.put("node_name", "serv01.dc02.ny.us.apache.org:8983_solr");
    propMap.put("state", "active");
    propMap.put("leader", "true");
    replica = new Replica("core_node2", propMap);
    replicas.put("core_node2", replica);

    //slice1
    Slice slice= new Slice("shard1", replicas, null);
    slices.put("COL_shard1", slice);


    Set<String> liveNodes = new HashSet<>();

    //livenodes
    liveNodes.add("serv01.dc01.ny.us.apache.org:8983_solr");
    liveNodes.add("serv01.dc02.ny.us.apache.org:8983_solr");

    boolean sendToLeaders = false;
    String collection = "COL";
    SolrQuery query = new SolrQuery("*:*");

    //routing rules to query with ip 10.1.12.* selecting
    // only shard1 replica1
    query.add("routingRule","ip_4:10");
    query.add("routingRule","ip_3:1");
    query.add("routingRule","ip_2:12");

    //buildURL List
    List<String> urlList = urlSelector.buildUrlList(slices, liveNodes, sendToLeaders, query, collection);
    assertThat(urlList.size(), is(1));

    //replica3
    propMap = new HashMap<>();
    propMap.put("core", "COL_shard2_replica1");
    propMap.put("base_url", "http://serv02.dc01.ny.us.apache.org:8983/solr");
    propMap.put("node_name", "serv02.dc01.ny.us.apache.org:8983_solr");
    propMap.put("state", "active");
    replica = new Replica("core_node3", propMap);
    replicas.put("core_node3", replica);

  //replica4
    propMap = new HashMap<>();
    propMap.put("core", "COL_shard2_replica2");
    propMap.put("base_url", "http://serv02.dc02.ny.us.apache.org:8983/solr");
    propMap.put("node_name", "serv02.dc02.ny.us.apache.org:8983_solr");
    propMap.put("state", "active");
    propMap.put("leader", "true");
    replica = new Replica("core_node4", propMap);
    replicas.put("core_node4", replica);

    slice= new Slice("shard2", replicas, null);
    slices.put("COL_shard2", slice);

    //livenodes
    liveNodes.add("serv02.dc01.ny.us.apache.org:8983_solr");
    liveNodes.add("serv02.dc02.ny.us.apache.org:8983_solr");

    query = new SolrQuery("*:*");

    //routing rules to query with ip 10.1.12.* selecting
    // shards with dc01
    query.add("routingRule","ip_4:10");
    query.add("routingRule","ip_3:1");
    query.add("routingRule","ip_2:12");

    //buildURL List
    urlList = urlSelector.buildUrlList(slices, liveNodes, sendToLeaders, query, collection);
    assertThat(urlList.size(), is(2));

  }

}
