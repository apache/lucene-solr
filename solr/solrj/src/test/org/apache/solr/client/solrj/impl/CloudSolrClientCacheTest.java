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


import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;
import org.apache.http.NoHttpResponseException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.NamedList;
import org.easymock.EasyMock;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CloudSolrClientCacheTest extends SolrTestCaseJ4 {

  public void testCaching() throws Exception {
    String collName = "gettingstarted";
    Set<String> livenodes = new HashSet<>();
    Map<String, ClusterState.CollectionRef> refs = new HashMap<>();
    Map<String, DocCollection> colls = new HashMap<>();

    class Ref extends ClusterState.CollectionRef {
      private String c;

      public Ref(String c) {
        super(null);
        this.c = c;
      }

      @Override
      public boolean isLazilyLoaded() {
        return true;
      }

      @Override
      public DocCollection get() {
        gets.incrementAndGet();
        return colls.get(c);
      }
    }
    Map<String, Function> responses = new HashMap<>();
    NamedList okResponse = new NamedList();
    okResponse.add("responseHeader", new NamedList<>(Collections.singletonMap("status", 0)));

    LBHttpSolrClient mockLbclient = getMockLbHttpSolrClient(responses);
    AtomicInteger lbhttpRequestCount = new AtomicInteger();
    try (CloudSolrClient cloudClient = new CloudSolrClient.Builder()
        .withLBHttpSolrClient(mockLbclient)
        .withClusterStateProvider(getStateProvider(livenodes, refs))

        .build()) {
      livenodes.addAll(ImmutableSet.of("192.168.1.108:7574_solr", "192.168.1.108:8983_solr"));
      ClusterState cs = ClusterState.load(1, coll1State.getBytes(UTF_8),
          Collections.emptySet(), "/collections/gettingstarted/state.json");
      refs.put(collName, new Ref(collName));
      colls.put(collName, cs.getCollectionOrNull(collName));
      responses.put("request", o -> {
        int i = lbhttpRequestCount.incrementAndGet();
        if (i == 1) return new ConnectException("TEST");
        if (i == 2) return new SocketException("TEST");
        if (i == 3) return new NoHttpResponseException("TEST");
        return okResponse;
      });
      UpdateRequest update = new UpdateRequest()
          .add("id", "123", "desc", "Something 0");

      cloudClient.request(update, collName);
      assertEquals(2, refs.get(collName).getCount());
    }

  }


  private LBHttpSolrClient getMockLbHttpSolrClient(Map<String, Function> responses) throws Exception {
    LBHttpSolrClient mockLbclient = EasyMock.createMock(LBHttpSolrClient.class);
    EasyMock.reset(mockLbclient);

    mockLbclient.request(EasyMock.anyObject(LBHttpSolrClient.Req.class));
    EasyMock.expectLastCall().andAnswer(() -> {
      LBHttpSolrClient.Req req = (LBHttpSolrClient.Req) EasyMock.getCurrentArguments()[0];
      Function f = responses.get("request");
      if (f == null) return null;
      Object res = f.apply(null);
      if (res instanceof Exception) throw (Throwable) res;
      LBHttpSolrClient.Rsp rsp = new LBHttpSolrClient.Rsp();
      rsp.rsp = (NamedList<Object>) res;
      rsp.server = req.servers.get(0);
      return rsp;
    }).anyTimes();

    mockLbclient.getHttpClient();
    EasyMock.expectLastCall().andAnswer(() -> null).anyTimes();

    EasyMock.replay(mockLbclient);
    return mockLbclient;
  }

  private CloudSolrClient.ClusterStateProvider getStateProvider(Set<String> livenodes,
                                                                Map<String, ClusterState.CollectionRef> colls) {
    return new CloudSolrClient.ClusterStateProvider() {
      @Override
      public ClusterState.CollectionRef getState(String collection) {
        return colls.get(collection);
      }

      @Override
      public Set<String> liveNodes() {
        return livenodes;
      }

      @Override
      public Map<String, Object> getClusterProperties() {
        return Collections.EMPTY_MAP;
      }

      @Override
      public String getAlias(String collection) {
        return collection;
      }

      @Override
      public String getCollectionName(String name) {
        return name;
      }

      @Override
      public void connect() { }

      @Override
      public void close() throws IOException {

      }
    };

  }


  private String coll1State = "{'gettingstarted':{\n" +
      "    'replicationFactor':'2',\n" +
      "    'router':{'name':'compositeId'},\n" +
      "    'maxShardsPerNode':'2',\n" +
      "    'autoAddReplicas':'false',\n" +
      "    'shards':{\n" +
      "      'shard1':{\n" +
      "        'range':'80000000-ffffffff',\n" +
      "        'state':'active',\n" +
      "        'replicas':{\n" +
      "          'core_node2':{\n" +
      "            'core':'gettingstarted_shard1_replica1',\n" +
      "            'base_url':'http://192.168.1.108:8983/solr',\n" +
      "            'node_name':'192.168.1.108:8983_solr',\n" +
      "            'state':'active',\n" +
      "            'leader':'true'},\n" +
      "          'core_node4':{\n" +
      "            'core':'gettingstarted_shard1_replica2',\n" +
      "            'base_url':'http://192.168.1.108:7574/solr',\n" +
      "            'node_name':'192.168.1.108:7574_solr',\n" +
      "            'state':'active'}}},\n" +
      "      'shard2':{\n" +
      "        'range':'0-7fffffff',\n" +
      "        'state':'active',\n" +
      "        'replicas':{\n" +
      "          'core_node1':{\n" +
      "            'core':'gettingstarted_shard2_replica1',\n" +
      "            'base_url':'http://192.168.1.108:8983/solr',\n" +
      "            'node_name':'192.168.1.108:8983_solr',\n" +
      "            'state':'active',\n" +
      "            'leader':'true'},\n" +
      "          'core_node3':{\n" +
      "            'core':'gettingstarted_shard2_replica2',\n" +
      "            'base_url':'http://192.168.1.108:7574/solr',\n" +
      "            'node_name':'192.168.1.108:7574_solr',\n" +
      "            'state':'active'}}}}}}";


}
